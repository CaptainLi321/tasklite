from __future__ import annotations

import email.utils
import io
import json
import math
import socket
import threading
import time
import urllib.error
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, Optional
from unittest.mock import MagicMock

import pytest

from tasklite.exceptions import FatalError, RateLimitHit, RetryError
from tasklite.wrappers.http import (
    HttpExecutor,
    HttpPolicy,
    HttpResponse,
    MemorySnapshotStore,
    SQLiteSnapshotStore,
    SnapshotStore,
    fetch_requests,
    fetch_urllib,
    format_cookie_header,
    guard_request,
    guarded_fetch,
    http_guard,
    parse_netscape_cookies,
)


# ==============================================================================
# 1. Cookie 工具测试
# ==============================================================================

def test_parse_netscape_cookies_basic(tmp_path: Path) -> None:
    cookie_content = """# Netscape HTTP Cookie File
# http://curl.haxx.se/rfc/cookie_spec.html

.example.com\tTRUE\t/\tTRUE\t1799999999\tsession_id\tabc123
#HttpOnly_.example.com\tTRUE\t/\tTRUE\t1799999999\tauth_token\txyz789
.example.com\tTRUE\t/\tTRUE\t1799999999\tsession_id\toverwritten456
"""
    cookie_file = tmp_path / "cookies.txt"
    cookie_file.write_text(cookie_content, encoding="utf-8")

    cookies = parse_netscape_cookies(cookie_file)
    assert cookies["auth_token"] == "xyz789"
    assert cookies["session_id"] == "overwritten456"

    # 直接从字符串解析
    cookies_str = parse_netscape_cookies(cookie_content)
    assert cookies_str == cookies

    # 文件不存在时返回空字典
    assert parse_netscape_cookies(tmp_path / "non_existent.txt") == {}


def test_format_cookie_header() -> None:
    cookies = {"a": "1", "b": "2"}
    header = format_cookie_header(cookies)
    assert header in ("a=1; b=2", "b=2; a=1")


# ==============================================================================
# 2. HttpResponse 测试
# ==============================================================================

def test_http_response_properties() -> None:
    resp = HttpResponse(
        status_code=200,
        headers={"Content-Type": "application/json; charset=utf-8", "X-Custom": "TestVal"},
        body=b'{"key": "\xe4\xb8\xad\xe6\x96\x87"}',
        url="https://api.example.com/item",
    )
    assert resp.status_code == 200
    assert resp.ok is True
    assert resp.url == "https://api.example.com/item"
    assert resp.header("content-type") == "application/json; charset=utf-8"
    assert resp.header("X-CUSTOM") == "TestVal"
    assert resp.header("non-existent", default="def") == "def"
    assert resp.text == '{"key": "中文"}'
    assert resp.json() == {"key": "中文"}

    err_resp = HttpResponse(status_code=404, headers={}, body=b"Not Found")
    assert err_resp.ok is False


# ==============================================================================
# 3. HttpPolicy 规则器测试
# ==============================================================================

def test_http_policy_classification() -> None:
    policy = HttpPolicy()

    assert policy.classify_status(200) is None
    assert policy.classify_status(204) is None
    assert policy.classify_status(429) is RateLimitHit
    assert policy.classify_status(404) is FatalError
    assert policy.classify_status(401) is FatalError
    assert policy.classify_status(500) is RetryError
    assert policy.classify_status(502) is RetryError

    # 自定义状态码钩子
    custom_policy = HttpPolicy(
        status_classifier=lambda code, resp: FatalError if code == 2483 else None
    )
    assert custom_policy.classify_status(2483) is FatalError
    assert custom_policy.classify_status(429) is RateLimitHit


def test_http_policy_retry_after_parsing() -> None:
    policy = HttpPolicy()

    # 秒数格式
    assert policy.extract_retry_after({"Retry-After": "120"}) == 120.0
    assert policy.extract_retry_after({"retry-after": "45.5"}) == 45.5

    # HTTP-Date 格式（未来时间）
    future_http_date = email.utils.formatdate(time.time() + 300, usegmt=True)
    parsed = policy.extract_retry_after({"Retry-After": future_http_date})
    assert parsed is not None
    assert 280.0 <= parsed <= 320.0

    # 无效格式
    assert policy.extract_retry_after({}) is None
    assert policy.extract_retry_after({"Retry-After": "invalid-val"}) is None


def test_extract_retry_after_non_positive_returns_none() -> None:
    """0/负数/非有限数/过期 HTTP-Date 均为「无有效时长」，返回 None 而非 clamp 0.0。

    不变式：仅正值具备挂起语义。TaskContext.suspend_resource 对 seconds<=0
    fail-loud，clamp 放行的 0.0 会在守卫 __exit__ 内以 ValueError 替换限流信号。
    """
    policy = HttpPolicy()

    assert policy.extract_retry_after({"Retry-After": "0"}) is None
    assert policy.extract_retry_after({"Retry-After": "-5"}) is None
    assert policy.extract_retry_after({"Retry-After": "inf"}) is None
    assert policy.extract_retry_after({"Retry-After": "nan"}) is None

    past_date = email.utils.formatdate(time.time() - 600, usegmt=True)
    assert policy.extract_retry_after({"Retry-After": past_date}) is None

    # 有效正值语义不回归
    assert policy.extract_retry_after({"Retry-After": "2.5"}) == 2.5


class _StrictSuspendCtx:
    """与引擎 TaskContext.suspend_resource 入口校验同构的最小上下文。"""

    def __init__(self) -> None:
        self.calls: list = []

    def suspend_resource(self, name: str, seconds: float) -> None:
        if not math.isfinite(seconds) or seconds <= 0:
            raise ValueError(f"seconds must be finite and > 0, got {seconds!r}")
        self.calls.append((name, seconds))


@pytest.mark.parametrize("hdrs", [{"Retry-After": "0"}, {"Retry-After": "-3"}])
def test_http_guard_invalid_retry_after_falls_back_to_default_ttl(hdrs: dict) -> None:
    """429 携带非法 Retry-After 时必须回落 default_suspend_ttl 并保持限流信号原样抛出。

    挂起信号必须以正时长落盘：若 0.0 直达 suspend_resource，入口 ValueError
    会替换 RateLimitHit，worker 兜底改写 error 终态——限流信号零落盘且任务
    以 UNKNOWN 终态误入 DLQ 不再重试。
    """
    ctx = _StrictSuspendCtx()
    with pytest.raises(RateLimitHit):
        with http_guard(ctx=ctx, resource="api_x", default_suspend_ttl=60.0):
            raise urllib.error.HTTPError(
                url="https://x.com",
                code=429,
                msg="Too Many Requests",
                hdrs=hdrs,
                fp=None,
            )
    assert ctx.calls == [("api_x", 60.0)]


# ==============================================================================
# 4. http_guard 守卫与引擎联动测试
# ==============================================================================

def test_http_guard_rate_limit_and_suspend() -> None:
    mock_ctx = MagicMock()
    mock_ctx.suspend_resource = MagicMock()

    # 429 异常抛出并自动挂起
    with pytest.raises(RateLimitHit):
        with http_guard(ctx=mock_ctx, resource="api_twitter", default_suspend_ttl=30.0):
            # 模拟 urllib 429
            raise urllib.error.HTTPError(
                url="https://x.com",
                code=429,
                msg="Too Many Requests",
                hdrs={"Retry-After": "75"},
                fp=None,
            )

    mock_ctx.suspend_resource.assert_called_once_with("api_twitter", 75.0)


def test_http_guard_nested_inner_without_ctx_defers_suspension_to_outer() -> None:
    """内层守卫无 ctx/resource 时不得置位 _suspended，挂起信号须由外层守卫落盘。

    API_GUIDE 场景 4 官方组合（外层带 ctx 守卫 + store.cached(fetch_urllib)）：
    fetch_urllib 的内层守卫 ctx=None，若其无条件置位 _suspended，外层会误判
    「已挂起」而跳过 suspend_resource——限流闭环静默失效，管线全速猛打被限流 API。
    """
    ctx = _StrictSuspendCtx()
    with pytest.raises(RateLimitHit):
        with http_guard(ctx=ctx, resource="api_feed", default_suspend_ttl=60.0):
            with http_guard(default_suspend_ttl=60.0):
                raise RateLimitHit("HTTP 429 RateLimit hit")
    assert ctx.calls == [("api_feed", 60.0)]


def test_http_guard_nested_both_bound_suspends_exactly_once() -> None:
    """内外层守卫均绑定 ctx/resource 时挂起信号恰好落盘一次（去重语义）。

    _suspended 标志的唯一职责是向更外层守卫传播「挂起已完成」，防止同一
    限流信号逐层重复下发 suspend_resource。
    """
    ctx = _StrictSuspendCtx()
    with pytest.raises(RateLimitHit):
        with http_guard(ctx=ctx, resource="api_outer", default_suspend_ttl=60.0):
            with http_guard(ctx=ctx, resource="api_inner", default_suspend_ttl=30.0):
                raise RateLimitHit("HTTP 429 RateLimit hit")
    assert ctx.calls == [("api_inner", 30.0)]


def test_http_guard_requests_style_retry_after_from_response() -> None:
    """requests 风格异常无 headers 属性，Retry-After 须从 exc_val.response.headers 提取。

    遗漏该回退时，requests.HTTPError 携带的 Retry-After 被静默丢弃，
    挂起时长退化为 default_suspend_ttl。
    """

    class _RequestsStyleHTTPError(Exception):
        """模拟官方 requests.HTTPError：仅携带 response，无 headers 属性。"""

        def __init__(self, response: Any) -> None:
            super().__init__("429 Too Many Requests")
            self.response = response

    class _Response:
        status_code = 429
        headers = {"Retry-After": "20"}

    ctx = _StrictSuspendCtx()
    with pytest.raises(RateLimitHit):
        with http_guard(ctx=ctx, resource="api_x", default_suspend_ttl=60.0):
            raise _RequestsStyleHTTPError(_Response())
    assert ctx.calls == [("api_x", 20.0)]


@pytest.mark.parametrize(
    "status_code, expected_exc, retry_after",
    [
        (429, RateLimitHit, "20"),
        (503, RetryError, None),
        (404, FatalError, None),
    ],
)
def test_http_guard_httpx_status_error_style_takeover(
    status_code: int, expected_exc: type, retry_after: Any,
) -> None:
    """httpx.HTTPStatusError 形态（``raise_for_status`` 主动 raise）按状态码三分类接管。

    契约：守卫块内任何携带 ``response.status_code`` 的库异常一律走状态码
    分类，不落传输层模块兜底——429 的挂起退避不因「用户自己 raise」而
    丢失，且 Retry-After 从 ``exc.response.headers`` 提取（httpx 异常
    同样无顶层 headers 属性）。
    """

    class _HTTPXStatusError(Exception):
        """模拟 httpx.HTTPStatusError：携带 request/response，无 headers 属性。"""

        def __init__(self, response: Any) -> None:
            super().__init__(f"HTTP status {response.status_code}")
            self.request = object()
            self.response = response

    class _Response:
        def __init__(self, code: int, headers: Any) -> None:
            self.status_code = code
            self.headers = headers

    headers = {"Retry-After": retry_after} if retry_after is not None else {}
    ctx = _StrictSuspendCtx()
    with pytest.raises(expected_exc):
        with http_guard(ctx=ctx, resource="api_x", default_suspend_ttl=60.0):
            raise _HTTPXStatusError(_Response(status_code, headers))
    if expected_exc is RateLimitHit:
        assert ctx.calls == [("api_x", 20.0)]
    else:
        assert ctx.calls == []


def test_default_suspend_ttl_construction_validation() -> None:
    """default_suspend_ttl 构造期 fail-loud：非数值 TypeError，非有限/非正值 ValueError。

    坏配置必须在构造期暴露，而非延迟到 429 命中时才在守卫 __exit__ 内
    以 ValueError 替换限流信号。
    """
    with pytest.raises(TypeError):
        http_guard(default_suspend_ttl="60")
    with pytest.raises(TypeError):
        HttpExecutor(default_suspend_ttl=True)
    with pytest.raises(ValueError):
        http_guard(default_suspend_ttl=0)
    with pytest.raises(ValueError):
        HttpExecutor(default_suspend_ttl=-1.0)
    with pytest.raises(ValueError):
        http_guard(default_suspend_ttl=float("inf"))
    # 合法值构造通过
    assert http_guard(default_suspend_ttl=1.5).default_suspend_ttl == 1.5
    assert HttpExecutor(default_suspend_ttl=90).default_suspend_ttl == 90.0


def test_http_guard_transient_and_fatal() -> None:
    # 500 转换为 RetryError
    with pytest.raises(RetryError):
        with http_guard():
            raise urllib.error.HTTPError(
                url="https://api.test",
                code=500,
                msg="Internal Server Error",
                hdrs={},
                fp=None,
            )

    # 网络连接异常转换为 RetryError
    with pytest.raises(RetryError):
        with http_guard():
            raise urllib.error.URLError(reason="Connection refused")

    # 404 转换为 FatalError
    with pytest.raises(FatalError):
        with http_guard():
            raise urllib.error.HTTPError(
                url="https://api.test",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            )


def test_http_policy_http_exception_family_transient() -> None:
    """http.client.HTTPException 家族整体按瞬态传输故障分类。

    BadStatusLine/LineTooLong 等 getresponse/read 阶段故障（代理/源站
    提前断连的典型形态）不是 OSError，漏判会裸逃逸为零重试致命错误。
    """
    import http.client

    policy = HttpPolicy()
    for exc in (
        http.client.HTTPException("base"),
        http.client.BadStatusLine("''"),
        http.client.LineTooLong("header"),
        http.client.ResponseNotReady("x"),
        http.client.CannotSendRequest(),
        http.client.IncompleteRead(b"partial"),
        http.client.RemoteDisconnected("x"),
    ):
        assert policy.classify_exception(exc) is RetryError, type(exc).__name__

    # HTTPError 同属该家族，但仍按状态码精确分类，不落入家族兜底
    err = urllib.error.HTTPError("https://api.test", 404, "Not Found", None, io.BytesIO(b""))
    assert policy.classify_exception(err) is FatalError


class _CustomFatal(FatalError):
    """模拟用户分类器返回的自定义 FatalError 子类（签名的自然用法）。"""


class _CustomRateLimit(RateLimitHit):
    pass


class _CustomRetry(RetryError):
    pass


def test_check_response_accepts_classifier_subclasses() -> None:
    """分类器返回三分类子类时必须命中对应分支，identity 比较会静默当成功。

    status_classifier 返回自定义 FatalError 子类属签名的自然用法；若以
    `cls is FatalError` 分派则三分类全不命中，401 错误响应被静默作为
    成功返回，错误既不上抛也不进死信队列。
    """
    fatal_policy = HttpPolicy(
        status_classifier=lambda code, resp: _CustomFatal if code == 401 else None
    )
    with pytest.raises(FatalError):
        http_guard(policy=fatal_policy).check_response(
            HttpResponse(status_code=401, headers={}, body=b"unauthorized")
        )

    rl_policy = HttpPolicy(
        status_classifier=lambda code, resp: _CustomRateLimit if code == 429 else None
    )
    with pytest.raises(RateLimitHit) as exc_info:
        http_guard(policy=rl_policy).check_response(
            HttpResponse(status_code=429, headers={}, body=b"rate limited")
        )
    # RateLimitHit 分支的 Retry-After 挂起语义不得因子类返回而丢失
    assert getattr(exc_info.value, "_retry_after", None) == 60.0

    retry_policy = HttpPolicy(
        status_classifier=lambda code, resp: _CustomRetry if code == 503 else None
    )
    with pytest.raises(RetryError):
        http_guard(policy=retry_policy).check_response(
            HttpResponse(status_code=503, headers={}, body=b"down")
        )


def test_guard_exit_converts_custom_classifier_subclass() -> None:
    """exception_classifier 返回三分类子类时守卫必须完成对应转换与挂起联动。"""
    ctx = _StrictSuspendCtx()
    with pytest.raises(RateLimitHit):
        with http_guard(
            ctx=ctx,
            resource="api_x",
            default_suspend_ttl=45.0,
            policy=HttpPolicy(exception_classifier=lambda exc: _CustomRateLimit),
        ):
            raise ValueError("boom")
    assert ctx.calls == [("api_x", 45.0)]

    with pytest.raises(FatalError):
        with http_guard(policy=HttpPolicy(exception_classifier=lambda exc: _CustomFatal)):
            raise ValueError("boom")

    with pytest.raises(RetryError):
        with http_guard(policy=HttpPolicy(exception_classifier=lambda exc: _CustomRetry)):
            raise ValueError("boom")


def test_policy_classifier_invalid_return_fails_loud() -> None:
    """分类器返回与三分类无关的异常类或非异常类必须 fail-loud，禁止静默当成功。"""
    unrelated_class_policy = HttpPolicy(
        status_classifier=lambda code, resp: ValueError if code == 500 else None
    )
    with pytest.raises(TypeError):
        http_guard(policy=unrelated_class_policy).check_response(
            HttpResponse(status_code=500, headers={}, body=b"err")
        )

    non_class_policy = HttpPolicy(
        status_classifier=lambda code, resp: "FatalError" if code == 500 else None
    )
    with pytest.raises(TypeError):
        http_guard(policy=non_class_policy).check_response(
            HttpResponse(status_code=500, headers={}, body=b"err")
        )


def test_guard_request_retry_loop() -> None:
    attempts = 0

    def flaky_request() -> HttpResponse:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise ConnectionResetError("Connection reset by peer")
        return HttpResponse(status_code=200, headers={}, body=b"success")

    # 配置 max_retries=2, backoff=0.01 快速就地重试成功
    resp = guard_request(flaky_request, max_retries=2, backoff=0.01)
    assert resp.status_code == 200
    assert attempts == 3

    # 重试耗尽时抛出 RetryError
    attempts = 0
    with pytest.raises(RetryError):
        guard_request(flaky_request, max_retries=1, backoff=0.01)


def test_guarded_fetch_decorator() -> None:
    mock_ctx = MagicMock()

    @guarded_fetch(ctx=mock_ctx, resource="api_danbooru", default_suspend_ttl=45.0)
    def fetch_something() -> HttpResponse:
        return HttpResponse(status_code=429, headers={}, body=b"rate limited")

    with pytest.raises(RateLimitHit):
        fetch_something()

    mock_ctx.suspend_resource.assert_called_once_with("api_danbooru", 45.0)


# ==============================================================================
# 5. SnapshotStore 幂等快照测试
# ==============================================================================

def test_snapshot_store_make_key() -> None:
    # URL 规范化：Query 排序一致性
    k1 = SnapshotStore.make_key("https://api.test/v1?b=2&a=1", method="get")
    k2 = SnapshotStore.make_key("https://api.test/v1?a=1&b=2", method="GET")
    assert k1 == k2
    assert k1.startswith("GET::")

    # Body 影响 Key
    k3 = SnapshotStore.make_key("https://api.test/v1", method="POST", body=b'{"payload": 1}')
    k4 = SnapshotStore.make_key("https://api.test/v1", method="POST", body=b'{"payload": 2}')
    assert k3 != k4


def test_make_key_identity_headers_injective() -> None:
    """快照 key 的身份头段：凭证头变化必改 key，易变头不影响，与 body 段无歧义拼接。"""
    k_base = SnapshotStore.make_key("https://api.test/a")
    k_cookie = SnapshotStore.make_key("https://api.test/a", headers={"Cookie": "sid=1"})
    assert k_base != k_cookie

    # 归一化：header 名大小写不影响 key
    assert SnapshotStore.make_key("https://api.test/a", headers={"cookie": "sid=1"}) == k_cookie
    # 白名单外易变头不参与指纹
    assert SnapshotStore.make_key("https://api.test/a", headers={"User-Agent": "ua"}) == k_base
    # url / body / 身份头三维组合互不碰撞
    keys = {
        k_base,
        k_cookie,
        SnapshotStore.make_key("https://api.test/a", body=b"x"),
        SnapshotStore.make_key("https://api.test/a", body=b"x", headers={"Cookie": "sid=1"}),
        SnapshotStore.make_key("https://api.test/a", body=b"x", headers={"Cookie": "sid=2"}),
    }
    assert len(keys) == 5


def test_make_key_params_fingerprint_injective() -> None:
    """params 值指纹须类型前缀隔离，None 与 "None" 等不同形态不得共享快照 key。

    None 值参数与 requests 语义对齐全链路丢弃（wire、norm_query、指纹一致），
    与省略该参数同 key；字面 "None" 值保留参与指纹，二者不碰撞。
    """
    base = SnapshotStore.make_key("https://api.test/a")
    k_none = SnapshotStore.make_key("https://api.test/a", params={"a": None})
    k_str = SnapshotStore.make_key("https://api.test/a", params={"a": "None"})
    assert k_none != k_str

    # str() 同串的其它类型对（int 1 vs "1"）同样不得碰撞
    assert SnapshotStore.make_key("https://api.test/a", params={"a": 1}) != SnapshotStore.make_key(
        "https://api.test/a", params={"a": "1"}
    )
    # None 值丢弃后与省略参数同 key；字面 "None" 值仍独立成 key
    assert k_none == base
    assert base != k_str
    # 混合 None 值与实值的 params 与剔除 None 后等价
    assert SnapshotStore.make_key("https://api.test/a", params={"a": 1, "b": None}) == SnapshotStore.make_key(
        "https://api.test/a", params={"a": 1}
    )
    # dict 插入顺序不影响 key，相同 params 稳定同 key
    assert SnapshotStore.make_key("https://api.test/a", params={"a": 1, "b": 2}) == SnapshotStore.make_key(
        "https://api.test/a", params={"b": 2, "a": 1}
    )


def test_sqlite_snapshot_store_crud(tmp_path: Path) -> None:
    db_file = tmp_path / "snapshots.db"
    store = SQLiteSnapshotStore(db_file)

    key = store.make_key("https://api.example.com/user/100")
    assert store.has(key) is False
    assert store.get(key) is None

    store.put(
        key=key,
        url="https://api.example.com/user/100",
        status_code=200,
        headers={"Content-Type": "application/json"},
        body=b'{"id": 100, "name": "alice"}',
    )

    assert store.has(key) is True
    cached = store.get(key)
    assert cached is not None
    assert cached.status_code == 200
    assert cached.header("content-type") == "application/json"
    assert cached.json() == {"id": 100, "name": "alice"}


def test_memory_snapshot_store_crud() -> None:
    store = MemorySnapshotStore()
    key = store.make_key("https://api.example.com/status")

    store.put(
        key=key,
        url="https://api.example.com/status",
        status_code=200,
        headers={},
        body="OK",
    )
    assert store.has(key) is True
    cached = store.get(key)
    assert cached is not None
    assert cached.text == "OK"


def test_snapshot_store_cached_decorator(tmp_path: Path) -> None:
    store = SQLiteSnapshotStore(tmp_path / "snapshots.db")
    fetch_count = 0

    def mock_fetch(url: str, **kwargs: Any) -> HttpResponse:
        nonlocal fetch_count
        fetch_count += 1
        return HttpResponse(status_code=200, headers={}, body=f"data-{fetch_count}".encode("utf-8"))

    cached_fetch = store.cached(mock_fetch)

    # 首次调用触发底层 fetch 并写入缓存
    r1 = cached_fetch("https://api.test/item/1")
    assert r1.text == "data-1"
    assert fetch_count == 1

    # 第二次调用命中快照缓存，不增加 fetch_count
    r2 = cached_fetch("https://api.test/item/1")
    assert r2.text == "data-1"
    assert fetch_count == 1


def test_snapshot_cached_distinguishes_json_data_posts() -> None:
    """同 URL 不同 json_data 的 POST 必须各发一次真实网络请求（Body 单射哈希契约）。

    json_data 是 fetch_requests 的 JSON 请求体参数名，若未纳入快照 key，
    不同 JSON 体将共享同一 key 并静默命中首个请求的响应。
    """
    request_count = {"n": 0}

    class _CountingHandler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            content_len = int(self.headers.get("Content-Length", 0))
            post_body = self.rfile.read(content_len)
            request_count["n"] += 1
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"echo_len": ' + str(len(post_body)).encode() + b'}')

        def log_message(self, format: str, *args: Any) -> None:
            pass

    server = HTTPServer(("127.0.0.1", 0), _CountingHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    try:
        base_url = f"http://127.0.0.1:{server.server_port}"
        store = MemorySnapshotStore()
        cached_fetch = store.cached(fetch_requests)

        r1 = cached_fetch(f"{base_url}/api", method="POST", json_data={"action": "a"})
        r2 = cached_fetch(f"{base_url}/api", method="POST", json_data={"action": "another-with-longer-body"})
        assert request_count["n"] == 2
        assert r1.json()["echo_len"] != r2.json()["echo_len"]

        # 相同 json_data 重放命中快照，不再发网络请求
        r3 = cached_fetch(f"{base_url}/api", method="POST", json_data={"action": "a"})
        assert request_count["n"] == 2
        assert r3.json()["echo_len"] == r1.json()["echo_len"]
    finally:
        server.shutdown()


def test_snapshot_cached_body_params_fingerprint() -> None:
    """快照 key 的 body 指纹须按参数名聚合全部 body 类参数，混用互不碰撞。"""
    store = MemorySnapshotStore()
    call_count = 0

    def mock_fetch(url: str, **kwargs: Any) -> HttpResponse:
        nonlocal call_count
        call_count += 1
        return HttpResponse(status_code=200, headers={}, body=f"resp-{call_count}".encode("utf-8"))

    cached_fetch = store.cached(mock_fetch)

    # 同 URL 下不同 body 参数组合（含同内容 bytes 与 str）语义不同，key 不得碰撞
    combos: list = [
        {"data": {"k": 1}},
        {"json": {"k": 1}},
        {"json_data": {"k": 1}},
        {"body": {"k": 1}},
        {"data": {"k": 1}, "json_data": {"k": 2}},
        {"data": b"raw-bytes"},
        {"data": "raw-bytes"},
    ]
    for combo in combos:
        cached_fetch("https://api.test/post", method="POST", **combo)
    assert call_count == len(combos)

    # 完全相同的参数组合命中快照
    cached_fetch("https://api.test/post", method="POST", data={"k": 1})
    assert call_count == len(combos)


def test_snapshot_cached_distinguishes_positional_args() -> None:
    """以位置实参传参的请求必须各自真实发起，禁止静默命中同一快照。

    默认 key 纳入 bind 后的全部位置实参（与关键字参数同权重）：
    遗漏位置实参时 page-2 会零网络零告警地命中 page-1 的快照。
    """
    call_count = 0

    def mock_fetch(url: str, *args: Any, **kwargs: Any) -> HttpResponse:
        nonlocal call_count
        call_count += 1
        page = args[0] if args else "default"
        return HttpResponse(status_code=200, headers={}, body=f"page-{page}".encode("utf-8"))

    store = MemorySnapshotStore()
    cached_fetch = store.cached(mock_fetch)

    r1 = cached_fetch("https://api.test/list", 1)
    r2 = cached_fetch("https://api.test/list", 2)
    assert r1.text == "page-1"
    assert r2.text == "page-2"
    assert call_count == 2

    # 相同位置实参命中快照，不再发起底层请求
    cached_fetch("https://api.test/list", 1)
    assert call_count == 2

    # 位置实参与 body 类关键字实参组合语义各自独立，不得跨组合碰撞
    cached_fetch("https://api.test/list", 1, data={"k": 1})
    cached_fetch("https://api.test/list", 2, data={"k": 1})
    assert call_count == 4
    cached_fetch("https://api.test/list", 1, data={"k": 1})
    assert call_count == 4


def test_snapshot_cached_distinguishes_identity_headers() -> None:
    """换 Cookie/Authorization 后不得命中旧身份的快照（身份头纳入 key 指纹）。

    取舍：仅凭证/协商类头参与指纹，白名单外易变头（User-Agent 等）不参与——
    纳入全量 headers 会把同语义请求碎片化，摧毁命中率。
    """
    call_count = 0

    def mock_fetch(url: str, headers: Any = None, **kwargs: Any) -> HttpResponse:
        nonlocal call_count
        call_count += 1
        who = "anon"
        if headers:
            h = dict(headers)
            who = str(h.get("Authorization") or h.get("Cookie") or "anon").replace(" ", "")
        return HttpResponse(status_code=200, headers={}, body=f"resp-{who}".encode("utf-8"))

    store = MemorySnapshotStore()
    cached_fetch = store.cached(mock_fetch)

    r1 = cached_fetch("https://api.test/me", headers={"Authorization": "Bearer token-a"})
    r2 = cached_fetch("https://api.test/me", headers={"Authorization": "Bearer token-b"})
    assert r1.text == "resp-Bearertoken-a"
    assert r2.text == "resp-Bearertoken-b"
    assert call_count == 2

    # 白名单外易变头变化不触发重新请求（同 key 命中）
    cached_fetch("https://api.test/me", headers={"Authorization": "Bearer token-a", "User-Agent": "x"})
    assert call_count == 2

    # 无凭证头与携带凭证头 key 不同
    r_anon = cached_fetch("https://api.test/me")
    assert r_anon.text == "resp-anon"
    assert call_count == 3


def test_snapshot_cached_wrapped_fetch_urllib_429_suspension_reaches_outer_guard(
    local_http_server: str,
) -> None:
    """store.cached(fetch_urllib) 嵌套外层带 ctx 守卫时，429 挂起信号必须穿透内层落盘。

    fetch_urllib 自带 ctx=None 内层守卫，该组合是 API_GUIDE 场景 4 的官方
    推荐用法；挂起时长取自真实 Retry-After 响应头。
    """
    store = MemorySnapshotStore()
    ctx = _StrictSuspendCtx()
    cached_fetch = store.cached(fetch_urllib)

    with pytest.raises(RateLimitHit):
        with http_guard(ctx=ctx, resource="api_feed", default_suspend_ttl=60.0):
            cached_fetch(f"{local_http_server}/429")

    assert ctx.calls == [("api_feed", 15.0)]


def test_snapshot_cached_distinguishes_params_value_types() -> None:
    """同 URL 下 params 值不同形态必须各发一次真实请求，禁止静默命中同一快照。"""
    call_count = 0

    def mock_fetch(url: str, **kwargs: Any) -> HttpResponse:
        nonlocal call_count
        call_count += 1
        params = kwargs.get("params") or {}
        return HttpResponse(status_code=200, headers={}, body=f"val={params.get('a')!r}".encode("utf-8"))

    store = MemorySnapshotStore()
    cached_fetch = store.cached(mock_fetch)

    r1 = cached_fetch("https://api.test/a", params={"a": None})
    r2 = cached_fetch("https://api.test/a", params={"a": "None"})
    assert call_count == 2
    assert r1.text == "val=None"
    assert r2.text == "val='None'"

    # 相同形态仍命中快照，不重复请求
    cached_fetch("https://api.test/a", params={"a": "None"})
    assert call_count == 2


def test_snapshot_cached_distinguishes_cookies_and_auth_identity() -> None:
    """cookies/auth 关键字实参变化必须各发真实请求，禁止跨身份静默命中同一快照。

    身份经 kwargs 而非请求头传递（requests 风格 session.request 的
    cookies=/auth=）时同样改变响应语义，必须与身份头白名单同机制进入指纹，
    否则换身份方零网络消耗地拿到旧身份的响应。
    """
    call_count = 0

    def mock_fetch(url: str, **kwargs: Any) -> HttpResponse:
        nonlocal call_count
        call_count += 1
        who = (kwargs.get("cookies") or {}).get("session", "anon")
        return HttpResponse(status_code=200, headers={}, body=f"resp-{who}".encode("utf-8"))

    store = MemorySnapshotStore()
    cached_fetch = store.cached(mock_fetch)

    r_alice = cached_fetch("https://api.test/me", cookies={"session": "alice"})
    r_bob = cached_fetch("https://api.test/me", cookies={"session": "bob"})
    assert call_count == 2
    assert r_alice.text == "resp-alice"
    assert r_bob.text == "resp-bob"

    # 相同 cookies 身份命中快照
    cached_fetch("https://api.test/me", cookies={"session": "alice"})
    assert call_count == 2

    # auth 变化同样隔离身份，且与 cookies 维度互不碰撞
    cached_fetch("https://api.test/me", auth=("user", "pw-a"))
    cached_fetch("https://api.test/me", auth=("user", "pw-b"))
    cached_fetch("https://api.test/me", cookies={"session": "alice"}, auth=("user", "pw-a"))
    assert call_count == 5
    cached_fetch("https://api.test/me", auth=("user", "pw-a"))
    cached_fetch("https://api.test/me", cookies={"session": "alice"}, auth=("user", "pw-a"))
    assert call_count == 5

    # 不携带身份与携带身份互不碰撞
    r_anon = cached_fetch("https://api.test/me")
    assert r_anon.text == "resp-anon"
    assert call_count == 6


def test_snapshot_cached_skips_transient_statuses() -> None:
    """瞬态状态（408/425/429 与全部 5xx）保底不写入快照；ignore_statuses 仅可追加。"""
    store = MemorySnapshotStore()

    def mock_fetch(url: str, **kwargs: Any) -> HttpResponse:
        return HttpResponse(status_code=int(kwargs["status"]), headers={}, body=b"err")

    # ignore_statuses 传空元组，验证瞬态 4xx（408/425）与离散集遗漏的 5xx（如 501/511）同样被保底谓词拦截
    cached_fetch = store.cached(mock_fetch, ignore_statuses=())
    for status in (408, 425, 429, 500, 501, 511, 530):
        resp = cached_fetch("https://api.test/flaky", method="GET", status=status)
        assert resp.status_code == status
        assert store.has(store.make_key("https://api.test/flaky", method="GET")) is False

    # 正常响应仍写入快照
    def ok_fetch(url: str, **kwargs: Any) -> HttpResponse:
        return HttpResponse(status_code=200, headers={}, body=b"ok")

    cached_ok = store.cached(ok_fetch, ignore_statuses=())
    cached_ok("https://api.test/ok")
    assert store.has(store.make_key("https://api.test/ok", method="GET")) is True


def test_snapshot_cached_urllib_native_response_status_passthrough() -> None:
    """urllib 原生响应（仅 getcode()，无 status_code 属性）真实状态必须透传。

    状态提取默认 200 会让 getcode 回退成为死代码，非 200 响应以 200 身份
    永久写入快照，崩溃重试/离线重放恒命中失真内容。
    """
    class _FakeUrllibResponse:
        """模拟 http.client.HTTPResponse：只有 getcode/read/headers。"""

        def getcode(self) -> int:
            return 404

        def read(self) -> bytes:
            return b"not found"

        headers = {"content-type": "text/html"}

    store = MemorySnapshotStore()
    cached_fetch = store.cached(lambda url, **kwargs: _FakeUrllibResponse())

    resp = cached_fetch("https://api.test/item")
    assert resp.status_code == 404

    key = store.make_key("https://api.test/item")
    assert store.has(key) is True
    assert store.get(key).status_code == 404

    # 无状态语义的返回值（数据体）仍按成功记录
    cached_data = store.cached(lambda url, **kwargs: {"ok": True})
    assert cached_data("https://api.test/data").status_code == 200


# ==============================================================================
# 6. 内置 fetch_urllib 与 本地 HTTP 服务测试
# ==============================================================================

class _TestHttpHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/json":
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(b'{"status": "ok", "msg": "hello"}')
        elif self.path == "/429":
            self.send_response(429)
            self.send_header("Retry-After", "15")
            self.end_headers()
            self.wfile.write(b"rate limited")
        elif self.path == "/500":
            self.send_response(500)
            self.end_headers()
            self.wfile.write(b"server error")
        elif self.path.startswith("/echo"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(self.path.encode())
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self) -> None:
        content_len = int(self.headers.get("Content-Length", 0))
        post_body = self.rfile.read(content_len)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"received_bytes": ' + str(len(post_body)).encode() + b'}')

    def log_message(self, format: str, *args: Any) -> None:
        pass


@pytest.fixture(scope="module")
def local_http_server():
    server = HTTPServer(("127.0.0.1", 0), _TestHttpHandler)
    port = server.server_port
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield f"http://127.0.0.1:{port}"
    server.shutdown()


def test_fetch_urllib_success(local_http_server: str) -> None:
    resp = fetch_urllib(f"{local_http_server}/json")
    assert resp.status_code == 200
    assert resp.ok is True
    assert resp.json() == {"status": "ok", "msg": "hello"}


def test_fetch_urllib_post(local_http_server: str) -> None:
    resp = fetch_urllib(
        f"{local_http_server}/submit",
        method="POST",
        data={"user": "alice", "age": 25},
    )
    assert resp.status_code == 200
    assert resp.json()["received_bytes"] > 0


def test_fetch_urllib_429_suspension(local_http_server: str) -> None:
    mock_ctx = MagicMock()
    with pytest.raises(RateLimitHit):
        fetch_urllib(f"{local_http_server}/429", ctx=mock_ctx, resource="api_local")
    mock_ctx.suspend_resource.assert_called_once_with("api_local", 15.0)


def test_fetch_urllib_500_retry(local_http_server: str) -> None:
    with pytest.raises(RetryError):
        fetch_urllib(f"{local_http_server}/500")


def test_fetch_urllib_404_fatal(local_http_server: str) -> None:
    with pytest.raises(FatalError):
        fetch_urllib(f"{local_http_server}/non_existent")


def test_fetch_urllib_drops_none_params_like_requests(local_http_server: str) -> None:
    """params None 值与 requests 语义对齐丢弃：不上链，快照 key 与省略等价。

    requests 对 params={"k": None} 直接丢弃该参数；若 fetch_urllib 以
    str(v) 拼接上链，同一 params 映射在两后端线上请求不同而快照 key 相同，
    共用同一 SnapshotStore 的混用脚本会静默串快照。
    """
    resp = fetch_urllib(f"{local_http_server}/echo", params={"k": None, "a": "1"})
    assert resp.text == "/echo?a=1"


def test_fetch_urllib_bad_status_line_raises_retry_error() -> None:
    """残缺状态行（源站/代理提前断连）必须转换为 RetryError 退避重试。

    urllib 的 do_open 只把 OSError 包装为 URLError，残缺状态行在
    getresponse 阶段以裸 BadStatusLine 抛出——守卫必须将其归为瞬态，
    不得放行裸异常（零重试直送死信队列）。
    """
    server = socket.socket()
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    started = threading.Event()

    def _broken_server() -> None:
        try:
            started.set()
            conn, _ = server.accept()
            try:
                conn.recv(4096)
                conn.sendall(b"HTTP/1.1 x\r\n\r\n")
            finally:
                conn.close()
        except OSError:
            pass
        finally:
            server.close()

    t = threading.Thread(target=_broken_server, daemon=True)
    t.start()
    assert started.wait(timeout=5)
    try:
        with pytest.raises(RetryError):
            fetch_urllib(f"http://127.0.0.1:{port}/x", timeout=5.0)
    finally:
        t.join(timeout=5)


def test_fetch_requests_success(local_http_server: str) -> None:
    resp = fetch_requests(f"{local_http_server}/json")
    assert resp.status_code == 200
    assert resp.ok is True
    assert resp.json() == {"status": "ok", "msg": "hello"}


def test_fetch_requests_429_suspension(local_http_server: str) -> None:
    mock_ctx = MagicMock()
    with pytest.raises(RateLimitHit):
        fetch_requests(f"{local_http_server}/429", ctx=mock_ctx, resource="api_req")
    mock_ctx.suspend_resource.assert_called_once_with("api_req", 15.0)


# ==============================================================================
# 5. HttpExecutor 深模块测试
# ==============================================================================

class TestHttpExecutor:
    def test_execute_and_wrap_basic(self):
        executor = HttpExecutor(max_retries=0)
        call_count = 0

        def sample_fetch(url: str):
            nonlocal call_count
            call_count += 1
            return HttpResponse(status_code=200, headers={}, body=b"ok", url=url)

        resp = executor.execute(sample_fetch, "https://example.com/api")
        assert resp.status_code == 200
        assert resp.text == "ok"
        assert call_count == 1

        wrapped = executor.wrap(sample_fetch)
        resp2 = wrapped("https://example.com/api")
        assert resp2.status_code == 200
        assert call_count == 2

    def test_execute_with_snapshot_store(self):
        store = MemorySnapshotStore()
        executor = HttpExecutor(snapshot_store=store)
        call_count = 0

        def sample_fetch(url: str, **kwargs):
            nonlocal call_count
            call_count += 1
            return HttpResponse(status_code=200, headers={}, body=b"data", url=url)

        resp1 = executor.execute(sample_fetch, "https://example.com/cached")
        assert resp1.status_code == 200
        assert call_count == 1

        # 第二次请求直接命中快照缓存，不调用底层 sample_fetch
        resp2 = executor.execute(sample_fetch, "https://example.com/cached")
        assert resp2.status_code == 200
        assert resp2.text == "data"
        assert call_count == 1

    def test_execute_with_rate_limit_and_retry(self):
        mock_ctx = MagicMock()
        executor = HttpExecutor(ctx=mock_ctx, resource="api_custom", default_suspend_ttl=30.0)

        def failing_fetch(url: str):
            resp = MagicMock()
            resp.status_code = 429
            resp.headers = {"Retry-After": "45"}
            return resp

        with pytest.raises(RateLimitHit):
            executor.execute(failing_fetch, "https://example.com/rate_limited")

        mock_ctx.suspend_resource.assert_called_once_with("api_custom", 45.0)

    def test_execute_rate_limit_not_locally_retried(self):
        """429 必须命中 RateLimitHit 专用分支直接上抛，禁止落入本地重试循环。

        不变式：RateLimitHit 是 RetryError 子类，except 顺序颠倒会把限流吞进
        本地线性微退避——无视 Retry-After 猛打限流端点，且挂起信号逐 attempt
        重复落盘。限流等待唯一交由引擎挂起收敛。
        """
        mock_ctx = MagicMock()
        executor = HttpExecutor(ctx=mock_ctx, resource="api_x", max_retries=5, backoff=0.0)
        call_count = 0

        def rate_limited_fetch(url: str):
            nonlocal call_count
            call_count += 1
            return HttpResponse(status_code=429, headers={"Retry-After": "30"}, body=b"rate limited", url=url)

        with pytest.raises(RateLimitHit):
            executor.execute(rate_limited_fetch, "https://example.com/rl")

        assert call_count == 1
        mock_ctx.suspend_resource.assert_called_once_with("api_x", 30.0)




def test_http_guard_suspend_failure_preserves_rate_limit_signal() -> None:
    """suspend_resource 抛 ValueError 时守卫__exit__ 不得以其覆盖限流信号。

    资源名 typo（未注册名 fail-loud 的 ValueError）等挂起入口故障必须与
    限流信号本身解耦：ValueError 从 __exit__ 直接穿透会把瞬态 RateLimitHit
    替换为普通异常（烧重试预算进 DLQ），且挂起信号丢失。挂起失败降级为
    日志告警，_suspended 不置位，RateLimitHit 原样抛出。
    """

    class _RejectingCtx:
        def __init__(self) -> None:
            self.calls: list = []

        def suspend_resource(self, name: str, seconds: float) -> None:
            self.calls.append((name, seconds))
            raise ValueError(
                f"Unknown resource {name!r}; suspend request ignored"
            )

    ctx = _RejectingCtx()
    hit = RateLimitHit("HTTP 429 RateLimit hit")
    with pytest.raises(RateLimitHit) as exc_info:
        with http_guard(ctx=ctx, resource="api_typo", default_suspend_ttl=60.0):
            raise hit
    assert exc_info.value is hit
    assert ctx.calls == [("api_typo", 60.0)]
    # 挂起未真实发生：不得置位 _suspended（外层守卫语义依赖）
    assert getattr(hit, "_suspended", False) is False


def test_http_guard_suspend_type_error_preserves_rate_limit_signal() -> None:
    """挂起入口的 TypeError（如非法 ttl 形态）同样不覆盖限流信号。"""

    class _BrokenCtx:
        def suspend_resource(self, name: str, seconds: float) -> None:
            raise TypeError("seconds must be a number")

    hit = RateLimitHit("HTTP 429 RateLimit hit")
    with pytest.raises(RateLimitHit) as exc_info:
        with http_guard(ctx=_BrokenCtx(), resource="api_x", default_suspend_ttl=60.0):
            raise hit
    assert exc_info.value is hit


class TestHttpExecutorBackoffShape:
    """HttpExecutor 本地重试退避形态（与引擎指数退避策略同形）。

    线性无上限退避（backoff*attempt）在大 max_retries 下累计睡眠巨大且
    与引擎 backoff_base*2^n（封顶+抖动）策略不一致。
    """

    def _run_with_capture(self, monkeypatch, max_retries, backoff):
        import tasklite.wrappers.http as http_mod

        sleeps: list = []

        def _fake_sleep(secs):
            sleeps.append(secs)

        monkeypatch.setattr(http_mod.time, "sleep", _fake_sleep)
        # 抖动归零以锁定确定性形状（jitter 行为单独验证）
        monkeypatch.setattr(http_mod.random, "uniform", lambda a, b: 0.0)

        calls = {"n": 0}

        def _flaky(*args, **kwargs):
            calls["n"] += 1
            raise RetryError(f"transient #{calls['n']}")

        executor = http_mod.HttpExecutor(max_retries=max_retries, backoff=backoff)
        with pytest.raises(RetryError):
            executor.execute(_flaky)
        return sleeps

    def test_retry_delays_follow_exponential_shape(self, monkeypatch):
        """第 n 次重试等待 backoff*2^(n-1)（指数），而非线性 backoff*n。"""
        sleeps = self._run_with_capture(monkeypatch, max_retries=4, backoff=1.0)
        assert sleeps == [1.0, 2.0, 4.0, 8.0]

    def test_retry_delay_capped(self, monkeypatch):
        """单次等待封顶，大 backoff/大 max_retries 不再产生巨量睡眠。"""
        sleeps = self._run_with_capture(monkeypatch, max_retries=30, backoff=1000.0)
        assert sleeps
        assert all(s <= 300.0 for s in sleeps)
        assert len(sleeps) == 30

    def test_retry_delay_jittered(self, monkeypatch):
        """抖动以 ±25% 延迟幅度作用于每次等待（与引擎抖动幅度一致）。"""
        import tasklite.wrappers.http as http_mod

        jitter_calls: list = []
        captured = {"delay": 0.0}

        def _fake_uniform(a, b):
            jitter_calls.append((a, b))
            return 0.0

        monkeypatch.setattr(http_mod.time, "sleep", lambda s: None)
        monkeypatch.setattr(http_mod.random, "uniform", _fake_uniform)

        def _flaky(*args, **kwargs):
            raise RetryError("transient")

        executor = http_mod.HttpExecutor(max_retries=2, backoff=4.0)
        with pytest.raises(RetryError):
            executor.execute(_flaky)

        # 与引擎 compute_backoff 同写法：uniform(-0.25, 0.25) 后乘延迟 d=4, 8
        assert len(jitter_calls) == 2
        assert jitter_calls[0] == (-0.25, 0.25)
        assert jitter_calls[1] == (-0.25, 0.25)
