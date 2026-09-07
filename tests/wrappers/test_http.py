from __future__ import annotations

import email.utils
import io
import json
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


