"""组合式官方 HTTP 网络工具库（Composable HTTP Wrappers & Utilities）。

================================================================================
需求契约与设计手册（REQUIREMENTS & DESIGN CONTRACT —— 任何实现/重构必须逐条满足）
================================================================================

------------------------------------------------------------------------------
一、架构定位与设计哲学（Architecture & Philosophy）
------------------------------------------------------------------------------
1. **用户自主权第一（User Autonomy First）**：
   - 坚决不重造重量级统一客户端抽象层；
   - 赋予用户 100% 网络请求选型自主权：用户可自由选用 Python 标准库 `urllib`、
     `requests`、`httpx`、`curl_cffi`、GraphQL 客户端或第三方平台专用 SDK。
2. **正交双引擎解耦（Orthogonal Dual-Engine Decoupling）**：
   - **快照去重（`SnapshotStore`）**：专职内容寻址的原始 HTTP 响应持久化与离线
     幂等重放，零引擎依赖，可在任何离线分析或独立脚本中单用；
   - **429 与异常守卫（`http_guard` / `HttpPolicy`）**：专职将底层传输故障与
     HTTP 状态码收敛为 TaskLite 三分类异常，自动解析 `Retry-After` 并触发
     `ctx.suspend_resource` 资源挂起；
   - 两者互不依赖，亦可自由正交嵌套。
3. **全局限速复用（Global Rate Limiting Reuse）**：
   - 绝不新建独立的 `Pacer` 概念，多 Worker / 多进程速率限制统一复用 TaskLite
     原生 `RateLimitResource` 与 `ResourceManager`；
   - 429 挂起时经由 `ctx.suspend_resource` 实现 IPC 单一出口下发。
4. **零外部强制依赖（Zero Mandatory Dependencies）**：
   - 默认基于 Python 标准库（`urllib` / `http.cookiejar` / `sqlite3`），软支持
     `requests`。

------------------------------------------------------------------------------
二、异常三分类收敛映射契约（Exception Tri-Classification Matrix）
------------------------------------------------------------------------------
| 网络信号 / 状态码 | 对应引擎异常 | 引擎调度行为 | 重试预算消耗 |
| :--- | :--- | :--- | :--- |
| **HTTP 429 / 限流** | `RateLimitHit` | 自动解析 Retry-After，挂起资源，本任务放回队列等待 | ❌ 不烧预算（零污染） |
| **HTTP 5xx / 传输抖动** | `RetryError` | 触发框架指数退避重试，耗尽 max_retries 进 DLQ | ⚠️ 消耗 retry 预算 |
| **网络超时 / 连接重置** | `RetryError` | 判定为瞬态网络故障，触发框架退避重试 | ⚠️ 消耗 retry 预算 |
| **HTTP 4xx（非 429）** | `FatalError` | 判定为确定性客户端错误/认证失效，直送 DLQ 归档 | 🛑 立即终止，不空转 |

------------------------------------------------------------------------------
三、进程隔离与会话军规（Process Isolation & Session Discipline）
------------------------------------------------------------------------------
- **严禁跨进程共享 Session**：`requests.Session` 或底层套接字句柄包含未导出的
  内部锁与套接字，跨进程 pickle 传递会导致死锁或连接竞争崩溃；
- **子进程自洽构建**：每个 Worker 子进程在执行 Handler 时自行按需创建与回收
  连接或通过上下文管理器安全清理。

------------------------------------------------------------------------------
四、快照单射性与存储规范（Injective Snapshot Specification）
------------------------------------------------------------------------------
- 请求键生成遵循单射规范化：`make_key(url, method, params, body)`：
  1. URL 规范化（去除 Fragment、Query 参数字典排序转义）；
  2. HTTP 方法强制大写；
  3. Body 单射摘要（JSON 排序键序列化或字节 SHA-256 哈希）；
- `SQLiteSnapshotStore` 必须启用 `PRAGMA journal_mode=WAL`，保证读写并发与断电保护；
- 429、5xx 瞬态故障默认禁止写入快照库，防止污染离线缓存。
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

    规约契约：
    1. 自动跳过空行与 '#' 注释行；
    2. 兼容包含 '#HttpOnly_' 前缀的特殊声明行；
    3. 同名 Cookie 后出现的覆盖先出现的（严格遵循浏览器覆盖规则）；
    4. 若文件不存在或为空，安全返回空字典 `{}`。

    Args:
        file_or_content: cookies.txt 文件路径（Path 或 str）或直接传入的文本内容。

    Returns:
        Dict[str, str]: 解析提取的 Cookie 字典 `{cookie_name: cookie_value}`。

    Examples:
        >>> cookies = parse_netscape_cookies("./cookies.txt")
        >>> auth = cookies.get("session_id")
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
        str: 适合作为 HTTP Headers 中 'Cookie' 值的格式化字符串。
    """
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


class HttpResponse:
    """轻量标准化不可变 HTTP 响应容器。

    设计目标：
    为不同底层后端（urllib、requests、httpx 等）提供完全统一的只读响应访问视图，
    避免业务代码因切换底层请求库而产生重构开销。
    """

    __slots__ = ("_status_code", "_headers", "_body", "_url", "_text_cache")

    def __init__(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body: Union[bytes, str],
        url: str = "",
    ) -> None:
        """初始化 HttpResponse 容器。

        Args:
            status_code: HTTP 状态码（如 200, 404, 429）。
            headers: 响应头字典（内部自动归一化为小写键）。
            body: 响应体内容（字节或 UTF-8 文本）。
            url: 产生该响应的最终请求 URL。
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
        """HTTP 响应头字典（小写键名副本）。"""
        return dict(self._headers)

    @property
    def body(self) -> bytes:
        """原始响应体字节数组。"""
        return self._body

    @property
    def url(self) -> str:
        """最终响应的请求 URL。"""
        return self._url

    @property
    def ok(self) -> bool:
        """是否为成功响应（200 <= status_code < 300）。"""
        return 200 <= self._status_code < 300

    @property
    def text(self) -> str:
        """UTF-8 解码后的响应文本（自动缓存，带替换容错）。"""
        if self._text_cache is None:
            self._text_cache = self._body.decode("utf-8", errors="replace")
        return self._text_cache

    def json(self) -> Any:
        """将响应文本解析为 JSON 对象（使用 tasklite.utils.jsonutil 保证安全）。"""
        return _json_loads(self.text)

    def header(self, name: str, default: Optional[str] = None) -> Optional[str]:
        """大小写不敏感获取指定响应头。"""
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
    """HTTP 响应状态码与传输层异常三分类规则器。

    负责将复杂的 HTTP 协议响应与网络异常收敛为 TaskLite 标准三分类异常：
    - `RateLimitHit`: 429 / 限流（挂起资源，不烧预算）；
    - `FatalError`: 4xx / 认证过期（确定性失败，直送 DLQ）；
    - `RetryError`: 5xx / 网络超时 / 连接重置（瞬态重试）。
    """

    def __init__(
        self,
        rate_limit_statuses: Optional[Set[int]] = None,
        fatal_statuses: Optional[Set[int]] = None,
        retry_statuses: Optional[Set[int]] = None,
        status_classifier: Optional[Callable[[int, Any], Optional[Type[BaseException]]]] = None,
        exception_classifier: Optional[Callable[[BaseException], Optional[Type[BaseException]]]] = None,
    ) -> None:
        """初始化分类策略。

        Args:
            rate_limit_statuses: 视为限流的状态码集合（默认 {429}）。
            fatal_statuses: 视为确定性失败的状态码集合（默认 400/401/403/404/405/410/422）。
            retry_statuses: 视为服务端瞬态错误的状态码集合（默认 500/502/503/504 等）。
            status_classifier: 自定义状态码分类钩子 `(code, resp) -> ExceptionType | None`。
            exception_classifier: 自定义异常分类钩子 `(exc) -> ExceptionType | None`。
        """
        self.rate_limit_statuses = set(rate_limit_statuses if rate_limit_statuses is not None else _DEFAULT_RATE_LIMIT_STATUSES)
        self.fatal_statuses = set(fatal_statuses if fatal_statuses is not None else _DEFAULT_FATAL_STATUSES)
        self.retry_statuses = set(retry_statuses if retry_statuses is not None else _DEFAULT_RETRY_STATUSES)
        self.status_classifier = status_classifier
        self.exception_classifier = exception_classifier

    def extract_retry_after(self, headers: Any) -> Optional[float]:
        """从响应头中解析 Retry-After 字段（支持秒数与 HTTP-Date 格式）。

        Args:
            headers: 响应头 Mapping 对象。

        Returns:
            Optional[float]: 解析出的休眠秒数；若未提供或格式无效返回 None。
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
            Type[BaseException] | None: RateLimitHit / FatalError / RetryError 或 None（成功）。
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
        """分类底层网络传输异常。

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
    """HTTP 请求执行守卫（上下文管理器）。

    单一出口契约：
    1. 捕获执行块内抛出的网络异常，或通过 `check_response` 校验返回的状态码；
    2. 遭遇 429 时：从 headers 自动解析 Retry-After，自动调用 `ctx.suspend_resource`
       挂起全管线指定的限速资源，并抛出 `RateLimitHit`；
    3. 遭遇 5xx/超时：收敛为 `RetryError` 触发调度器退避重试；
    4. 遭遇 4xx：收敛为 `FatalError` 直送 DLQ，防止空耗重试预算。

    Examples:
        >>> with http_guard(ctx=ctx, resource="api_pixiv"):
        ...     resp = my_custom_client.get("https://api.pixiv.net/v1/...")
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
            resource: 关联的限速资源名（如 'api_twitter'）。
            policy: 异常与状态码分类策略器（缺省使用默认策略）。
            default_suspend_ttl: 遭遇 429 且未提供 Retry-After 时的默认挂起秒数（默认 60s）。
        """
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

            # 单一出口保证挂起只调用一次
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

        # 未被策略识别的代码异常原样向外抛出
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
    """包装执行任意 HTTP 请求函数，提供单任务内轻量就地重试与异常守卫收敛。

    Args:
        func: 目标请求函数。
        *args: 传递给目标函数的位置参数。
        max_retries: 单任务内就地快速重试次数（默认 0：立即转交 TaskLite 引擎调度器退避）。
        backoff: 就地重试基数秒（默认 1.0s）。
        ctx: 任务上下文（用于 429 挂起）。
        resource: 限速资源标识。
        policy: 分类策略。
        default_suspend_ttl: 429 挂起秒数。
        **kwargs: 传递给目标函数的关键字参数。

    Returns:
        Any: 目标函数执行成功的返回值。
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
    """快照去重抽象基类协议。

    定位：独立的内容寻址缓存，不依赖 TaskLite 引擎上下文，支持跨 run 离线幂等重放。
    """

    @staticmethod
    def make_key(
        url: str,
        method: str = "GET",
        params: Optional[Mapping[str, Any]] = None,
        body: Optional[Union[bytes, str, Mapping[str, Any]]] = None,
    ) -> str:
        """单射生成请求唯一键（URL 规范化 + 参数排序 + Body 单射哈希）。

        数学证明不变式：
        - 消除 Query 顺序差异：`?b=2&a=1` 与 `?a=1&b=2` 产生完全相同的 Key；
        - 单射百分号转义：URL 路径中的特殊字符经 injective 单射转义，防止跨组碰撞；
        - Body 单射哈希：携带请求体时附加 SHA-256 16 位摘要，杜绝跨 Payload 碰撞。

        Args:
            url: 目标 URL。
            method: HTTP 方法（GET / POST 等，自动大写）。
            params: 查询参数字典。
            body: 请求体内容（bytes, str, 或 JSON 字典）。

        Returns:
            str: 格式为 `"{METHOD}::{normalized_url}_{body_hash}"` 的单射键。
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
        """检查指定 key 是否已存在有效快照。"""
        raise NotImplementedError

    def get(self, key: str) -> Optional[HttpResponse]:
        """获取指定 key 的缓存响应，不存在返回 None。"""
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
        """写入原始响应快照。"""
        raise NotImplementedError

    def cached(
        self,
        fetch_fn: Callable[..., Any],
        key_func: Optional[Callable[..., str]] = None,
        ignore_statuses: Sequence[int] = (429, 500, 502, 503, 504, 520, 521, 522, 524),
    ) -> Callable[..., HttpResponse]:
        """将任意 fetch 函数包装为带快照缓存透明拦截的高阶函数。

        Args:
            fetch_fn: 底层网络请求函数 `(url, **kwargs) -> HttpResponse | Any`。
            key_func: 自定义 Key 派生函数（缺省使用 `SnapshotStore.make_key`）。
            ignore_statuses: 不落盘缓存的异常状态码（默认忽略 429 与 5xx 故障）。

        Returns:
            Callable: 包装后的透明缓存抓取函数。
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

    特性：
    1. 独立 SQLite 数据库文件（与 TaskLite 主状态库严格隔离，不干扰调度 WAL）；
    2. 启用 WAL + NORMAL 事务模式，提供高并发读写与断电安全；
    3. 支持跨会话、跨生命周期完全离线重放。
    """

    def __init__(self, db_path: Union[str, Path]) -> None:
        """初始化 SQLite 快照库。

        Args:
            db_path: 快照数据库文件路径（例如 './snapshots.db'）。
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
    """纯内存快照存储（适合单次运行、微基准与无 IO 单元测试）。"""

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
    """零依赖标准库 HTTP 请求开箱即用实现。

    Args:
        url: 请求目标 URL。
        method: HTTP 方法（GET / POST / PUT 等）。
        headers: 请求头映射。
        params: URL 查询参数映射。
        data: 请求体数据（支持 dict/list 自动 JSON 序列化并补齐 Content-Type、文本或 bytes）。
        timeout: 超时时间秒数（默认 30.0s）。
        policy: 状态码与异常分类策略。
        ctx: 任务上下文（用于 429 挂起）。
        resource: 限速资源标识。
        default_suspend_ttl: 429 默认挂起秒数。
        proxies: 显式代理配置（未指定且请求为 localhost/127.0.0.1 时自动屏蔽环境变量代理干扰）。

    Returns:
        HttpResponse: 标准响应容器。

    Raises:
        RateLimitHit: 遭遇 429 且完成资源挂起。
        RetryError: 遭遇 5xx 或网络超时/连接重置。
        FatalError: 遭遇 4xx 客户端错误。
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
    """基于 requests 的 HTTP 请求包装（当环境中安装了 requests 时可用）。

    Args:
        url: 请求目标 URL。
        session: 可选的 requests.Session 实例（必须在当前子进程内创建）。
        method: HTTP 方法。
        headers: 请求头映射。
        params: 查询参数。
        data: 表单或原始数据。
        json_data: JSON 数据（对应 requests 的 `json` 参数）。
        timeout: 超时秒数。
        policy: 分类策略。
        ctx: 任务上下文。
        resource: 限速资源标识。
        default_suspend_ttl: 429 挂起秒数。
        proxies: 显式代理配置。
        trust_env: 是否继承环境变量中的代理配置。
        **kwargs: 透传给 requests 的其它参数。

    Returns:
        HttpResponse: 标准响应容器。
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
