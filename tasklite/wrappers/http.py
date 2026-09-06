"""组合式官方 HTTP 网络工具库（Composable HTTP Wrappers & Utilities）。

设计契约（ADR 0001）：
1. 坚决不重造统一重量级客户端抽象，赋予用户 100% 网络请求库选型自主权；
2. 快照去重（SnapshotStore）与 429 异常守卫（http_guard / HttpPolicy）正交独立；
3. 多进程限速完全复用 TaskLite 核心 RateLimitResource 与 ctx.suspend_resource；
4. 零外部强制依赖，开箱即用。
"""
from __future__ import annotations

import email.utils
import hashlib
import http.client
import json
import logging
import math
import os
from pathlib import Path
import sqlite3
import time
from typing import (
    Any,
    Callable,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Set,
    Tuple,
    Type,
    Union,
)
import urllib.error
import urllib.parse
import urllib.request

from ..exceptions import FatalError, RateLimitHit, RetryError
from ..utils.injective import sanitize_job_component
from ..utils.jsonutil import dumps as _json_dumps, loads as _json_loads

logger = logging.getLogger("tasklite.wrappers.http")


# ==============================================================================
# 1. 基础容器与 Cookie 转换工具
# ==============================================================================

def parse_netscape_cookies(file_or_content: Union[str, Path]) -> Dict[str, str]:
    """解析 Netscape / MozillaCookieJar 格式 cookies.txt 为字典。

    - 自动跳过空行与 '#' 注释行；
    - 兼容包含 '#HttpOnly_' 前缀的行；
    - 同名 Cookie 后出现的覆盖先出现的（符合浏览器覆盖规则）；
    - 若文件不存在或为空，返回空字典。
    """
    if isinstance(file_or_content, Path):
        if not file_or_content.exists():
            return {}
        try:
            content = file_or_content.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return {}
    else:
        text_str = str(file_or_content).strip()
        # 判断传入的是文件路径还是文本内容
        if "\n" not in text_str and ("\t" not in text_str) and (os.path.exists(text_str) or text_str.endswith(".txt")):
            path = Path(text_str)
            if not path.exists():
                return {}
            try:
                content = path.read_text(encoding="utf-8", errors="replace")
            except OSError:
                return {}
        else:
            content = text_str

    cookies: Dict[str, str] = {}
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) >= 7:
            name = parts[5].strip()
            value = parts[6].strip()
            if name:
                cookies[name] = value
        elif len(parts) == 6:
            name = parts[4].strip()
            value = parts[5].strip()
            if name:
                cookies[name] = value
    return cookies


def format_cookie_header(cookies: Mapping[str, str]) -> str:
    """将 Cookie 字典格式化为 'Cookie: k=v; k2=v2' 请求头字符串。"""
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


class HttpResponse:
    """轻量标准化不可变 HTTP 响应容器。"""

    __slots__ = ("_status_code", "_headers", "_body", "_url", "_text_cache")

    def __init__(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body: Union[bytes, str],
        url: str = "",
    ) -> None:
        self._status_code = int(status_code)
        # 归一化 headers 为全小写键名
        self._headers = {str(k).lower(): str(v) for k, v in headers.items()}
        if isinstance(body, str):
            self._body = body.encode("utf-8")
        else:
            self._body = bytes(body)
        self._url = str(url)
        self._text_cache: Optional[str] = None

    @property
    def status_code(self) -> int:
        return self._status_code

    @property
    def headers(self) -> Dict[str, str]:
        return dict(self._headers)

    @property
    def body(self) -> bytes:
        return self._body

    @property
    def url(self) -> str:
        return self._url

    @property
    def ok(self) -> bool:
        return 200 <= self._status_code < 300

    @property
    def text(self) -> str:
        if self._text_cache is None:
            self._text_cache = self._body.decode("utf-8", errors="replace")
        return self._text_cache

    def json(self) -> Any:
        return _json_loads(self.text)

    def header(self, name: str, default: Optional[str] = None) -> Optional[str]:
        return self._headers.get(name.lower(), default)

    def __repr__(self) -> str:
        return f"<HttpResponse [{self._status_code}] len={len(self._body)}>"


# ==============================================================================
# 2. 状态码与异常三分类守卫（HttpPolicy & http_guard）
# ==============================================================================

_DEFAULT_RATE_LIMIT_STATUSES = frozenset({429})
_DEFAULT_FATAL_STATUSES = frozenset({400, 401, 403, 404, 405, 410, 422})
_DEFAULT_RETRY_STATUSES = frozenset({500, 502, 503, 504, 520, 521, 522, 524})


class HttpPolicy:
    """HTTP 响应状态码与传输层异常三分类规则器。"""

    def __init__(
        self,
        rate_limit_statuses: Optional[Set[int]] = None,
        fatal_statuses: Optional[Set[int]] = None,
        retry_statuses: Optional[Set[int]] = None,
        status_classifier: Optional[Callable[[int, Any], Optional[Type[BaseException]]]] = None,
        exception_classifier: Optional[Callable[[BaseException], Optional[Type[BaseException]]]] = None,
    ) -> None:
        self.rate_limit_statuses = set(rate_limit_statuses if rate_limit_statuses is not None else _DEFAULT_RATE_LIMIT_STATUSES)
        self.fatal_statuses = set(fatal_statuses if fatal_statuses is not None else _DEFAULT_FATAL_STATUSES)
        self.retry_statuses = set(retry_statuses if retry_statuses is not None else _DEFAULT_RETRY_STATUSES)
        self.status_classifier = status_classifier
        self.exception_classifier = exception_classifier

    def extract_retry_after(self, headers: Any) -> Optional[float]:
        """从响应头中解析 Retry-After 字段（支持整型秒数与 HTTP-Date 格式）。"""
        if not headers:
            return None
        val: Optional[str] = None
        if hasattr(headers, "get"):
            val = headers.get("Retry-After") or headers.get("retry-after")
        if not val:
            return None
        val_str = str(val).strip()
        try:
            sec = float(val_str)
            return max(0.0, sec) if math.isfinite(sec) else None
        except ValueError:
            pass
        try:
            parsed_tuple = email.utils.parsedate_tz(val_str)
            if parsed_tuple is not None:
                timestamp = email.utils.mktime_tz(parsed_tuple)
                diff = timestamp - time.time()
                return max(0.0, diff)
        except Exception:
            pass
        return None

    def classify_status(self, status_code: int, response: Any = None) -> Optional[Type[BaseException]]:
        """分类 HTTP 状态码。返回 RateLimitHit / FatalError / RetryError 或 None。"""
        if self.status_classifier is not None:
            custom = self.status_classifier(status_code, response)
            if custom is not None:
                return custom

        if status_code in self.rate_limit_statuses:
            return RateLimitHit
        if status_code in self.fatal_statuses:
            return FatalError
        if status_code in self.retry_statuses:
            return RetryError
        if 200 <= status_code < 400:
            return None
        # 其它未显式声明的 4xx 默认按 FatalError，其它 5xx 按 RetryError
        if 400 <= status_code < 500:
            return FatalError
        if status_code >= 500:
            return RetryError
        return None

    def classify_exception(self, exc: BaseException) -> Optional[Type[BaseException]]:
        """分类底层网络传输异常。"""
        if isinstance(exc, (RateLimitHit, FatalError, RetryError)):
            return type(exc)

        if self.exception_classifier is not None:
            custom = self.exception_classifier(exc)
            if custom is not None:
                return custom

        # urllib.error.HTTPError
        if isinstance(exc, urllib.error.HTTPError):
            return self.classify_status(exc.code, exc)

        # requests.exceptions.HTTPError 软适配
        if hasattr(exc, "response") and getattr(exc.response, "status_code", None) is not None:
            return self.classify_status(exc.response.status_code, exc.response)

        # 瞬态传输层错误（超时、连接重置、DNS 解析失败等）
        if isinstance(
            exc,
            (
                urllib.error.URLError,
                TimeoutError,
                ConnectionError,
                http.client.RemoteDisconnected,
                http.client.IncompleteRead,
                OSError,
            ),
        ):
            return RetryError

        # requests / httpx 软适配检测
        exc_mod = getattr(exc.__class__, "__module__", "")
        if "requests" in exc_mod or "httpx" in exc_mod or "urllib3" in exc_mod or "curl_cffi" in exc_mod:
            return RetryError

        return None


class http_guard:
    """HTTP 请求执行守卫（上下文管理器）。

    作用：
    1. 捕获执行体中的底层异常或根据响应状态码分类；
    2. 遭遇 429 时，自动从 headers 提取 Retry-After 并调用 ctx.suspend_resource 挂起资源；
    3. 保证单一出口将异常收敛为 TaskLite 三分类异常（RateLimitHit / RetryError / FatalError）。
    """

    def __init__(
        self,
        ctx: Optional[Any] = None,
        resource: Optional[str] = None,
        policy: Optional[HttpPolicy] = None,
        default_suspend_ttl: float = 60.0,
    ) -> None:
        self.ctx = ctx
        self.resource = resource
        self.policy = policy or HttpPolicy()
        self.default_suspend_ttl = default_suspend_ttl

    def __enter__(self) -> "http_guard":
        return self

    def check_response(self, response: Any) -> None:
        """显式检查响应对象（支持 HttpResponse、requests.Response、urllib 响应）。"""
        status_code = getattr(response, "status_code", None)
        if status_code is None and hasattr(response, "getcode"):
            status_code = response.getcode()
        if status_code is None:
            return

        cls = self.policy.classify_status(status_code, response)
        if cls is RateLimitHit:
            headers = getattr(response, "headers", None)
            retry_after = self.policy.extract_retry_after(headers)
            ttl = retry_after if retry_after is not None else self.default_suspend_ttl
            hit_exc = RateLimitHit(f"HTTP 429 RateLimit hit (resource={self.resource}, ttl={ttl:.1f}s)")
            setattr(hit_exc, "_retry_after", ttl)
            raise hit_exc
        elif cls is FatalError:
            raise FatalError(f"HTTP {status_code} Fatal error")
        elif cls is RetryError:
            raise RetryError(f"HTTP {status_code} Transient server error")

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        if exc_val is None:
            return False

        cls = self.policy.classify_exception(exc_val)
        if cls is RateLimitHit or isinstance(exc_val, RateLimitHit):
            ttl = getattr(exc_val, "_retry_after", None)
            if ttl is None:
                headers = getattr(exc_val, "headers", None)
                retry_after = self.policy.extract_retry_after(headers)
                ttl = retry_after if retry_after is not None else self.default_suspend_ttl

            if not getattr(exc_val, "_suspended", False):
                if self.ctx is not None and self.resource is not None and hasattr(self.ctx, "suspend_resource"):
                    self.ctx.suspend_resource(self.resource, ttl)
                setattr(exc_val, "_suspended", True)

            if isinstance(exc_val, RateLimitHit):
                raise exc_val
            raise RateLimitHit(f"RateLimit hit: {exc_val}") from exc_val

        elif cls is FatalError:
            if isinstance(exc_val, FatalError):
                raise exc_val
            raise FatalError(f"Fatal request error: {exc_val}") from exc_val

        elif cls is RetryError:
            if isinstance(exc_val, RetryError):
                raise exc_val
            raise RetryError(f"Transient request error: {exc_val}") from exc_val

        # 未被策略识别的普通代码异常，原样向外抛出
        return False


def guard_request(
    func: Callable[..., Any],
    *args: Any,
    max_retries: int = 0,
    backoff: float = 1.0,
    ctx: Optional[Any] = None,
    resource: Optional[str] = None,
    policy: Optional[HttpPolicy] = None,
    default_suspend_ttl: float = 60.0,
    **kwargs: Any,
) -> Any:
    """包装执行任意 HTTP 请求函数，提供轻量就地重试与异常守卫收敛。"""
    pol = policy or HttpPolicy()
    for attempt in range(max(0, max_retries) + 1):
        if attempt > 0:
            time.sleep(backoff * attempt)
        try:
            with http_guard(ctx=ctx, resource=resource, policy=pol, default_suspend_ttl=default_suspend_ttl) as g:
                res = func(*args, **kwargs)
                g.check_response(res)
                return res
        except RetryError:
            if attempt >= max_retries:
                raise
        except (RateLimitHit, FatalError):
            raise
    raise RetryError(f"Request retries exhausted ({max_retries} retries)")


def guarded_fetch(
    ctx: Optional[Any] = None,
    resource: Optional[str] = None,
    policy: Optional[HttpPolicy] = None,
    max_retries: int = 0,
    backoff: float = 1.0,
    default_suspend_ttl: float = 60.0,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """用于修饰请求函数的装饰器。"""
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            return guard_request(
                fn,
                *args,
                max_retries=max_retries,
                backoff=backoff,
                ctx=ctx,
                resource=resource,
                policy=policy,
                default_suspend_ttl=default_suspend_ttl,
                **kwargs,
            )
        return wrapped
    return decorator


# ==============================================================================
# 3. 快照去重与离线重放（SnapshotStore）
# ==============================================================================

class SnapshotStore:
    """快照去重基类协议。"""

    @staticmethod
    def make_key(
        url: str,
        method: str = "GET",
        params: Optional[Mapping[str, Any]] = None,
        body: Optional[Union[bytes, str, Mapping[str, Any]]] = None,
    ) -> str:
        """单射生成请求唯一键（URL 规范化 + 参数排序 + Body 单射哈希）。"""
        clean_url = url.strip()
        parsed = urllib.parse.urlparse(clean_url)

        # 归一化 query 参数
        query_items: List[Tuple[str, str]] = []
        if parsed.query:
            query_items.extend(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        if params:
            for k, v in params.items():
                query_items.append((str(k), str(v)))
        query_items.sort()
        norm_query = urllib.parse.urlencode(query_items)

        norm_url = urllib.parse.urlunparse(
            (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.params, norm_query, "")
        )

        norm_method = method.strip().upper()

        body_hash = ""
        if body:
            if isinstance(body, (dict, list)):
                raw_bytes = _json_dumps(body).encode("utf-8")
            elif isinstance(body, str):
                raw_bytes = body.encode("utf-8")
            else:
                raw_bytes = bytes(body)
            body_hash = f"_{hashlib.sha256(raw_bytes).hexdigest()[:16]}"

        url_component = sanitize_job_component(norm_url)
        return f"{norm_method}::{url_component}{body_hash}"

    def has(self, key: str) -> bool:
        raise NotImplementedError

    def get(self, key: str) -> Optional[HttpResponse]:
        raise NotImplementedError

    def put(
        self,
        key: str,
        url: str,
        status_code: int,
        headers: Mapping[str, str],
        body: Union[bytes, str, Mapping[str, Any]],
        method: str = "GET",
    ) -> None:
        raise NotImplementedError

    def cached(
        self,
        fetch_fn: Callable[..., Any],
        key_func: Optional[Callable[..., str]] = None,
        ignore_statuses: Sequence[int] = (429, 500, 502, 503, 504, 520, 521, 522, 524),
    ) -> Callable[..., HttpResponse]:
        """将任意 fetch 函数包装为带快照缓存透明拦截的高阶函数。"""
        def wrapped(url: str, *args: Any, **kwargs: Any) -> HttpResponse:
            method = kwargs.get("method", "GET")
            params = kwargs.get("params", None)
            body = kwargs.get("data") or kwargs.get("json") or kwargs.get("body")

            if key_func is not None:
                key = key_func(url, *args, **kwargs)
            else:
                key = self.make_key(url, method=method, params=params, body=body)

            cached_resp = self.get(key)
            if cached_resp is not None:
                return cached_resp

            res = fetch_fn(url, *args, **kwargs)

            # 提取响应字段
            if isinstance(res, HttpResponse):
                status_code = res.status_code
                headers = res.headers
                raw_body = res.body
            else:
                status_code = getattr(res, "status_code", 200)
                if status_code is None and hasattr(res, "getcode"):
                    status_code = response.getcode()
                status_code = status_code or 200
                headers = dict(getattr(res, "headers", {}))
                if hasattr(res, "content"):
                    raw_body = res.content
                elif hasattr(res, "read"):
                    raw_body = res.read()
                elif isinstance(res, (dict, list)):
                    raw_body = _json_dumps(res).encode("utf-8")
                else:
                    raw_body = str(res).encode("utf-8")

            if status_code not in ignore_statuses:
                self.put(
                    key=key,
                    url=url,
                    status_code=status_code,
                    headers=headers,
                    body=raw_body,
                    method=method,
                )

            return HttpResponse(status_code=status_code, headers=headers, body=raw_body, url=url)

        return wrapped


class SQLiteSnapshotStore(SnapshotStore):
    """基于 SQLite WAL 模式的持久化快照存储。"""

    def __init__(self, db_path: Union[str, Path]) -> None:
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _get_conn(self) -> sqlite3.Connection:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS http_snapshots (
                    request_key TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    headers_json TEXT NOT NULL,
                    body BLOB NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.commit()

    def has(self, key: str) -> bool:
        with self._get_conn() as conn:
            cur = conn.execute("SELECT 1 FROM http_snapshots WHERE request_key = ?", (key,))
            return cur.fetchone() is not None

    def get(self, key: str) -> Optional[HttpResponse]:
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT url, status_code, headers_json, body FROM http_snapshots WHERE request_key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            url, status_code, headers_json, body = row
            try:
                headers = _json_loads(headers_json)
            except Exception:
                headers = {}
            return HttpResponse(status_code=status_code, headers=headers, body=body, url=url)

    def put(
        self,
        key: str,
        url: str,
        status_code: int,
        headers: Mapping[str, str],
        body: Union[bytes, str, Mapping[str, Any]],
        method: str = "GET",
    ) -> None:
        if isinstance(body, (dict, list)):
            body_bytes = _json_dumps(body).encode("utf-8")
        elif isinstance(body, str):
            body_bytes = body.encode("utf-8")
        else:
            body_bytes = bytes(body)

        headers_dict = {str(k): str(v) for k, v in headers.items()}
        headers_json = _json_dumps(headers_dict)
        now = time.time()

        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO http_snapshots (request_key, url, method, status_code, headers_json, body, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_key) DO UPDATE SET
                    url=excluded.url,
                    method=excluded.method,
                    status_code=excluded.status_code,
                    headers_json=excluded.headers_json,
                    body=excluded.body,
                    created_at=excluded.created_at
                """,
                (key, url, method.upper(), int(status_code), headers_json, body_bytes, now),
            )
            conn.commit()


class MemorySnapshotStore(SnapshotStore):
    """纯内存快照存储（适合单元测试或临时任务）。"""

    def __init__(self) -> None:
        self._data: Dict[str, HttpResponse] = {}

    def has(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str) -> Optional[HttpResponse]:
        return self._data.get(key)

    def put(
        self,
        key: str,
        url: str,
        status_code: int,
        headers: Mapping[str, str],
        body: Union[bytes, str, Mapping[str, Any]],
        method: str = "GET",
    ) -> None:
        if isinstance(body, (dict, list)):
            body_bytes = _json_dumps(body).encode("utf-8")
        elif isinstance(body, str):
            body_bytes = body.encode("utf-8")
        else:
            body_bytes = bytes(body)
        self._data[key] = HttpResponse(
            status_code=status_code,
            headers={str(k): str(v) for k, v in headers.items()},
            body=body_bytes,
            url=url,
        )


# ==============================================================================
# 4. 内置标准请求辅助函数（fetch_urllib & fetch_requests）
# ==============================================================================

def fetch_urllib(
    url: str,
    *,
    method: str = "GET",
    headers: Optional[Mapping[str, str]] = None,
    params: Optional[Mapping[str, Any]] = None,
    data: Optional[Union[bytes, str, Mapping[str, Any]]] = None,
    timeout: float = 30.0,
    policy: Optional[HttpPolicy] = None,
    ctx: Optional[Any] = None,
    resource: Optional[str] = None,
    default_suspend_ttl: float = 60.0,
    proxies: Optional[Mapping[str, str]] = None,
) -> HttpResponse:
    """零依赖标准库 HTTP 请求开箱即用实现。"""
    final_url = url
    if params:
        parsed = urllib.parse.urlparse(url)
        q = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
        for k, v in params.items():
            q.append((str(k), str(v)))
        new_query = urllib.parse.urlencode(q)
        final_url = urllib.parse.urlunparse(
            (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
        )

    req_body: Optional[bytes] = None
    req_headers = dict(headers) if headers else {}

    if data is not None:
        if isinstance(data, (dict, list)):
            req_body = _json_dumps(data).encode("utf-8")
            if "content-type" not in {k.lower(): v for k, v in req_headers.items()}:
                req_headers["Content-Type"] = "application/json"
        elif isinstance(data, str):
            req_body = data.encode("utf-8")
        else:
            req_body = bytes(data)

    req = urllib.request.Request(
        url=final_url,
        data=req_body,
        headers=req_headers,
        method=method.upper(),
    )

    if proxies is not None:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler(dict(proxies)))
    elif "127.0.0.1" in final_url or "localhost" in final_url:
        opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    else:
        opener = urllib.request.build_opener()

    with http_guard(ctx=ctx, resource=resource, policy=policy, default_suspend_ttl=default_suspend_ttl) as g:
        try:
            with opener.open(req, timeout=timeout) as resp:
                status_code = resp.getcode()
                resp_headers = dict(resp.headers)
                body = resp.read()
                http_resp = HttpResponse(status_code=status_code, headers=resp_headers, body=body, url=final_url)
                g.check_response(http_resp)
                return http_resp
        except urllib.error.HTTPError as e:
            err_headers = dict(e.headers) if hasattr(e, "headers") else {}
            err_body = e.read() if hasattr(e, "read") else b""
            err_resp = HttpResponse(status_code=e.code, headers=err_headers, body=err_body, url=final_url)
            g.check_response(err_resp)
            return err_resp


def fetch_requests(
    url: str,
    *,
    session: Optional[Any] = None,
    method: str = "GET",
    headers: Optional[Mapping[str, str]] = None,
    params: Optional[Mapping[str, Any]] = None,
    data: Optional[Any] = None,
    json_data: Optional[Any] = None,
    timeout: float = 30.0,
    policy: Optional[HttpPolicy] = None,
    ctx: Optional[Any] = None,
    resource: Optional[str] = None,
    default_suspend_ttl: float = 60.0,
    proxies: Optional[Mapping[str, str]] = None,
    trust_env: Optional[bool] = None,
    **kwargs: Any,
) -> HttpResponse:
    """基于 requests 的 HTTP 请求包装（当环境中安装了 requests 时可用）。"""
    try:
        import requests
    except ImportError as e:
        raise RuntimeError("fetch_requests requires 'requests' package to be installed") from e

    req_kwargs: Dict[str, Any] = {
        "method": method.upper(),
        "url": url,
        "headers": headers,
        "params": params,
        "data": data,
        "timeout": timeout,
        **kwargs,
    }
    if json_data is not None:
        req_kwargs["json"] = json_data

    created_session = False
    if session is None:
        sess = requests.Session()
        created_session = True
        if trust_env is not None:
            sess.trust_env = trust_env
        elif "127.0.0.1" in url or "localhost" in url:
            sess.trust_env = False
        if proxies is not None:
            sess.proxies = dict(proxies)
    else:
        sess = session

    with http_guard(ctx=ctx, resource=resource, policy=policy, default_suspend_ttl=default_suspend_ttl) as g:
        try:
            resp = sess.request(**req_kwargs)
            http_resp = HttpResponse(
                status_code=resp.status_code,
                headers=dict(resp.headers),
                body=resp.content,
                url=resp.url,
            )
            g.check_response(http_resp)
            return http_resp
        finally:
            if created_session:
                sess.close()




__all__ = [
    "parse_netscape_cookies",
    "format_cookie_header",
    "HttpResponse",
    "HttpPolicy",
    "http_guard",
    "guard_request",
    "guarded_fetch",
    "SnapshotStore",
    "SQLiteSnapshotStore",
    "MemorySnapshotStore",
    "fetch_urllib",
    "fetch_requests",
]
