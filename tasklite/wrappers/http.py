"""TaskLite 官方轻量 HTTP 网络工具库。

本模块提供面向批处理、数据同步与爬虫任务的通用 HTTP 辅助工具，主要包含：
- http_guard: 上下文管理器，自动将 HTTP 状态码与网络异常映射为 TaskLite 三分类异常，并在遇到 429 时自动挂起对应资源；
- HttpPolicy: HTTP 状态码与异常分类策略器，支持自定义状态码与异常分类钩子；
- SnapshotStore / SQLiteSnapshotStore: 原始 HTTP 响应快照存储，支持基于请求单射哈希的幂等缓存与离线重放；
- parse_netscape_cookies / format_cookie_header: Netscape 格式 cookies.txt 解析与请求头格式化工具；
- fetch_urllib / fetch_requests: 开箱即用的轻量请求实现。
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

    Args:
        file_or_content: cookies.txt 文件路径（Path 或 str）或直接传入的文本内容。

    Returns:
        Dict[str, str]: 解析提取的 Cookie 字典 `{cookie_name: cookie_value}`。
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
    """将 Cookie 字典格式化为 'Cookie: k=v; k2=v2' 请求头字符串。

    Args:
        cookies: Cookie 键值对映射。

    Returns:
        str: 格式化后的 Cookie 请求头字符串。
    """
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


class HttpResponse:
    """不可变 HTTP 响应容器。

    为不同底层请求库（urllib、requests、httpx 等）提供统一的只读响应数据视图。
    """

    __slots__ = ("_status_code", "_headers", "_body", "_url", "_text_cache")

    def __init__(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body: Union[bytes, str],
        url: str = "",
    ) -> None:
        """初始化 HttpResponse。

        Args:
            status_code: HTTP 状态码。
            headers: 响应头字典（内部自动归一化为小写键名）。
            body: 响应体内容（字节或 UTF-8 字符串）。
            url: 产生该响应的请求 URL。
        """
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
        """HTTP 响应状态码。"""
        return self._status_code

    @property
    def headers(self) -> Dict[str, str]:
        """HTTP 响应头字典（小写键名）。"""
        return dict(self._headers)

    @property
    def body(self) -> bytes:
        """原始响应体字节。"""
        return self._body

    @property
    def url(self) -> str:
        """请求 URL。"""
        return self._url

    @property
    def ok(self) -> bool:
        """是否为成功响应（200 <= status_code < 300）。"""
        return 200 <= self._status_code < 300

    @property
    def text(self) -> str:
        """UTF-8 解码后的文本内容（带替换容错）。"""
        if self._text_cache is None:
            self._text_cache = self._body.decode("utf-8", errors="replace")
        return self._text_cache

    def json(self) -> Any:
        """解析响应文本为 JSON 对象。"""
        return _json_loads(self.text)

    def header(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """大小写不敏感获取指定响应头。"""
        return self._headers.get(name.lower(), default)

    def __repr__(self) -> str:
        return f"<HttpResponse [{self._status_code}] len={len(self._body)}>"


# ==============================================================================
# 2. 状态码与异常分类守卫（HttpPolicy & http_guard）
# ==============================================================================

_DEFAULT_RATE_LIMIT_STATUSES = frozenset({429})
_DEFAULT_FATAL_STATUSES = frozenset({400, 401, 403, 404, 405, 410, 422})
_DEFAULT_RETRY_STATUSES = frozenset({500, 502, 503, 504, 520, 521, 522, 524})


class HttpPolicy:
    """HTTP 状态码与传输异常分类规则器。

    将 HTTP 状态码与底层异常分类为 TaskLite 三分类异常：
    - RateLimitHit: 限流（触发资源挂起，不扣减重试预算）；
    - FatalError: 确定性错误（如 4xx，直接进入死信队列）；
    - RetryError: 瞬态错误（如 5xx、超时，触发重试）。
    """

    def __init__(
        self,
        rate_limit_statuses: Optional[Set[int]] = None,
        fatal_statuses: Optional[Set[int]] = None,
        retry_statuses: Optional[Set[int]] = None,
        status_classifier: Optional[Callable[[int, Any], Optional[Type[BaseException]]]] = None,
        exception_classifier: Optional[Callable[[BaseException], Optional[Type[BaseException]]]] = None,
    ) -> None:
        """初始化分类规则器。

        Args:
            rate_limit_statuses: 判定为限流的状态码集合（默认 {429}）。
            fatal_statuses: 判定为确定性失败的状态码集合（默认 400/401/403/404 等）。
            retry_statuses: 判定为瞬态服务端错误的状态码集合（默认 500/502/503 等）。
            status_classifier: 自定义状态码分类函数 `(code, resp) -> ExceptionType | None`。
            exception_classifier: 自定义异常分类函数 `(exc) -> ExceptionType | None`。
        """
        self.rate_limit_statuses = set(rate_limit_statuses if rate_limit_statuses is not None else _DEFAULT_RATE_LIMIT_STATUSES)
        self.fatal_statuses = set(fatal_statuses if fatal_statuses is not None else _DEFAULT_FATAL_STATUSES)
        self.retry_statuses = set(retry_statuses if retry_statuses is not None else _DEFAULT_RETRY_STATUSES)
        self.status_classifier = status_classifier
        self.exception_classifier = exception_classifier

    def extract_retry_after(self, headers: Any) -> Optional[float]:
        """从响应头中提取 Retry-After 字段值并解析为秒数。

        支持整型秒数与标准 HTTP-Date 格式。

        Args:
            headers: 响应头 Mapping 对象。

        Returns:
            Optional[float]: 解析出的秒数；未提供或无法解析时返回 None。
        """
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
        """分类 HTTP 状态码。

        Returns:
            Type[BaseException] | None: 对应的异常类，或 None（表示请求正常）。
        """
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
        """分类底层网络异常。

        Returns:
            Type[BaseException] | None: 对应的 TaskLite 异常类型或 None。
        """
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
    """HTTP 请求守卫（上下文管理器）。

    功能：
    1. 捕获代码块内抛出的异常，或通过 `check_response` 检查响应状态码；
    2. 遭遇 429 限流时：自动解析 Retry-After，自动调用 `ctx.suspend_resource` 挂起对应的限速资源，并抛出 `RateLimitHit`；
    3. 遭遇 5xx/超时等瞬态错误：转换为 `RetryError` 触发调度器退避重试；
    4. 遭遇 4xx 等客户端错误：转换为 `FatalError` 直送死信队列。

    Examples:
        >>> with http_guard(ctx=ctx, resource="api_custom"):
        ...     resp = my_request(...)
        ...     if resp.status_code != 200:
        ...         raise requests.HTTPError(response=resp)
    """

    def __init__(
        self,
        ctx: Optional[Any] = None,
        resource: Optional[str] = None,
        policy: Optional[HttpPolicy] = None,
        default_suspend_ttl: float = 60.0,
    ) -> None:
        """初始化 HTTP 守卫。

        Args:
            ctx: 当前任务的 TaskContext（用于调用 `ctx.suspend_resource`）。
            resource: 关联的限速资源名称（如 'api_pixiv'）。
            policy: 状态码与异常分类规则器（默认使用标准 HttpPolicy）。
            default_suspend_ttl: 遭遇 429 且未提供 Retry-After 时的默认挂起秒数。
        """
        self.ctx = ctx
        self.resource = resource
        self.policy = policy or HttpPolicy()
        self.default_suspend_ttl = default_suspend_ttl

    def __enter__(self) -> "http_guard":
        return self

    def check_response(self, response: Any) -> None:
        """显式检查响应对象的状态码。"""
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

        # 未被策略识别的代码异常原样抛出
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
    """包装执行 HTTP 请求函数，提供就地快速重试与异常守卫。

    Args:
        func: 待执行的请求函数。
        *args: 传递给请求函数的位置参数。
        max_retries: 就地重试次数（默认 0：立即转交 TaskLite 引擎调度器退避）。
        backoff: 就地重试等待间隔基数秒（默认 1.0s）。
        ctx: 任务上下文。
        resource: 限速资源名称。
        policy: 分类规则器。
        default_suspend_ttl: 429 挂起秒数。
        **kwargs: 传递给请求函数的关键字参数。

    Returns:
        Any: 请求函数成功时的返回值。
    """
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
    """用于修饰请求函数的守卫装饰器。"""
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
    """快照存储基类协议。

    内容寻址的原始 HTTP 响应缓存，支持断点重试与离线重放。
    """

    @staticmethod
    def make_key(
        url: str,
        method: str = "GET",
        params: Optional[Mapping[str, Any]] = None,
        body: Optional[Union[bytes, str, Mapping[str, Any]]] = None,
    ) -> str:
        """规范化生成请求唯一键（URL 参数排序 + Method 大写 + Body 哈希）。

        Args:
            url: 目标 URL。
            method: HTTP 方法（GET、POST 等）。
            params: 查询参数字典。
            body: 请求体内容（bytes、str 或 dict）。

        Returns:
            str: 规范化的请求键。
        """
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
        """检查是否存在有效快照。"""
        raise NotImplementedError

    def get(self, key: str) -> Optional[HttpResponse]:
        """获取快照响应，不存在返回 None。"""
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
        """保存快照。"""
        raise NotImplementedError

    def cached(
        self,
        fetch_fn: Callable[..., Any],
        key_func: Optional[Callable[..., str]] = None,
        ignore_statuses: Sequence[int] = (429, 500, 502, 503, 504, 520, 521, 522, 524),
    ) -> Callable[..., HttpResponse]:
        """包装请求函数，提供透明的快照缓存拦截。

        Args:
            fetch_fn: 底层网络请求函数。
            key_func: 自定义 Key 生成函数（默认使用 make_key）。
            ignore_statuses: 不写入快照的状态码（默认忽略 429 与 5xx 故障）。

        Returns:
            Callable: 包装后的缓存函数。
        """
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
                    status_code = res.getcode()
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
    """基于 SQLite WAL 模式的持久化快照存储。

    使用独立的 SQLite 数据库文件保存原始 HTTP 响应，支持断电保护与并发读取。
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        """初始化 SQLite 快照库。

        Args:
            db_path: 数据库文件路径（如 './snapshots.db'）。
        """
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
    """纯内存快照存储（适合测试与临时会话）。"""

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
    """基于 Python 标准库 urllib 的 HTTP 请求实现（零外部依赖）。

    Args:
        url: 目标 URL。
        method: HTTP 方法（GET、POST 等）。
        headers: 请求头。
        params: 查询参数。
        data: 请求体（支持 dict/list 自动序列化为 JSON、字符串或 bytes）。
        timeout: 超时秒数（默认 30.0s）。
        policy: 状态码与异常分类规则器。
        ctx: 任务上下文。
        resource: 限速资源名称。
        default_suspend_ttl: 429 挂起秒数。
        proxies: 显式代理配置（访问 localhost/127.0.0.1 时默认不走环境变量代理）。

    Returns:
        HttpResponse: 响应容器对象。
    """
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
    """基于 requests 库的 HTTP 请求实现（当环境中安装了 requests 时可用）。

    Args:
        url: 目标 URL。
        session: requests.Session 实例（必须在当前子进程内创建）。
        method: HTTP 方法。
        headers: 请求头。
        params: 查询参数。
        data: 表单或原始数据。
        json_data: JSON 数据（对应 requests 的 json 参数）。
        timeout: 超时秒数。
        policy: 分类规则器。
        ctx: 任务上下文。
        resource: 限速资源名称。
        default_suspend_ttl: 429 挂起秒数。
        proxies: 显式代理配置。
        trust_env: 是否使用环境变量中的代理设置。
        **kwargs: 透传给 requests 的其它参数。

    Returns:
        HttpResponse: 响应容器对象。
    """
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
