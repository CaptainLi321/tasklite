"""HTTP 响应、请求、缓存与本地重试的可观察行为。"""
from __future__ import annotations

import email.utils
import io
import socket
import threading
import time
import urllib.error
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any

import pytest

from tasklite import TaskLiteError

from tasklite import Job, JobContext, StateSnapshot, TaskLite, TaskResult
from tasklite.wrappers.http import (
    HttpExecutor,
    HttpResponse,
    MemorySnapshotStore,
    SQLiteSnapshotStore,
    SnapshotStore,
    format_cookie_header,
    parse_netscape_cookies,
    requests_fetch,
    urllib_fetch,
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
# ==============================================================================


# ==============================================================================
# ==============================================================================


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

    json_data 是 requests_fetch 的 JSON 请求体参数名，若未纳入快照 key，
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
        cached_fetch = store.cached(requests_fetch)

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

    默认 key 纳入全部位置实参（与关键字参数同权重）：遗漏位置实参时
    page-2 会零网络零告警地命中 page-1 的快照。
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
# 6. 内置 urllib_fetch 与 本地 HTTP 服务测试
# ==============================================================================

class _TestHttpHandler(BaseHTTPRequestHandler):
    flaky_counts: dict[str, int] = {}

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
        elif self.path == "/flaky":
            count = self.flaky_counts.get("c", 0) + 1
            self.flaky_counts["c"] = count
            if count == 1:
                self.send_response(500)
                self.end_headers()
                self.wfile.write(b"flaky server error")
            else:
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"status": "ok", "recovered": true}')
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


def test_urllib_fetch_success(local_http_server: str) -> None:
    resp = urllib_fetch(f"{local_http_server}/json")
    assert resp.status_code == 200
    assert resp.ok is True
    assert resp.json() == {"status": "ok", "msg": "hello"}


def test_urllib_fetch_post(local_http_server: str) -> None:
    resp = urllib_fetch(
        f"{local_http_server}/submit",
        method="POST",
        data={"user": "alice", "age": 25},
    )
    assert resp.status_code == 200
    assert resp.json()["received_bytes"] > 0


def test_urllib_fetch_drops_none_params_like_requests(local_http_server: str) -> None:
    """params None 值与 requests 语义对齐丢弃：不上链，快照 key 与省略等价。

    requests 对 params={"k": None} 直接丢弃该参数；若 urllib_fetch 以
    str(v) 拼接上链，同一 params 映射在两后端线上请求不同而快照 key 相同，
    共用同一 SnapshotStore 的混用脚本会静默串快照。
    """
    resp = urllib_fetch(f"{local_http_server}/echo", params={"k": None, "a": "1"})
    assert resp.text == "/echo?a=1"


def test_requests_fetch_success(local_http_server: str) -> None:
    resp = requests_fetch(f"{local_http_server}/json")
    assert resp.status_code == 200
    assert resp.ok is True
    assert resp.json() == {"status": "ok", "msg": "hello"}


# ==============================================================================
# 7. HttpExecutor 深模块测试
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


class TestHttpExecutorRetryWaitShape:
    """HttpExecutor 本地重试等待形态（指数递增封顶 + 抖动）。

    线性无上限等待（base×attempt）在大 max_retries 下累计睡眠巨大；
    正确形状为 base×2^(n-1)（封顶 + ±25% 抖动）。
    """


    def _run_with_capture(self, monkeypatch, max_retries, retry_delay_base):
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
            raise ConnectionError(f"transient #{calls['n']}")

        executor = http_mod.HttpExecutor(max_retries=max_retries, retry_delay_base=retry_delay_base)
        with pytest.raises(ConnectionError):
            executor.execute(_flaky)
        return sleeps

    def test_retry_waits_follow_exponential_shape(self, monkeypatch):
        """第 n 次重试等待 base×2^(n-1)（指数），而非线性 base×n。"""
        sleeps = self._run_with_capture(monkeypatch, max_retries=4, retry_delay_base=1.0)
        assert sleeps == [1.0, 2.0, 4.0, 8.0]

    def test_retry_wait_capped(self, monkeypatch):
        """单次等待封顶，大 base/大 max_retries 不再产生巨量睡眠。"""
        sleeps = self._run_with_capture(monkeypatch, max_retries=30, retry_delay_base=1000.0)
        assert sleeps
        assert all(s <= 300.0 for s in sleeps)
        assert len(sleeps) == 30


# ==============================================================================
# 8. 真实核心集成测试（TaskLite + HTTP Wrapper）
# ==============================================================================

def _fetch_success_handler(job: Job, ctx: Any) -> TaskResult:
    resp = urllib_fetch(job.payload["url"])
    data = resp.json()
    return TaskResult.success(metadata={"result": data["msg"]})


class TestHttpTaskLiteIntegration:
    """真实 TaskLite 核心管线与 HTTP wrapper 集成。

    契约：
    1. HTTP 失败由 handler 显式捕获后构造 TaskResult（success/failure/retry），
       框架核心不依赖隐式异常重试；
    2. 真实集成覆盖成功、确定性失败与瞬态重试场景。
    """

    def test_http_fetch_success_in_pipeline(self, local_http_server: str, tmp_path: Path) -> None:
        """成功场景：handler 发送 HTTP 请求获取 200，构造 TaskResult.success 返回。"""
        app = TaskLite(backend="sqlite", state_path=tmp_path / "http_succ.db")
        app.register_task("http_job", _fetch_success_handler)
        app.enqueue(Job("http_job", "j1", payload={"url": f"{local_http_server}/json"}))
        app.run()

        with StateSnapshot.open(app) as snap:
            rec = snap.get_job("http_job::j1")
            assert rec is not None
            assert rec["status"] == "succeeded"
            assert rec["result"] is not None
            assert rec["result"]["metadata"]["result"] == "hello"




@pytest.mark.parametrize("code,status", [(200, "success"), (302, "success"), (404, "failure"), (429, "retry"), (503, "retry")])
def test_response_explicit_task_result(code, status):
    response = HttpResponse(status_code=code, headers={"Retry-After": "12"}, body=b"body")
    result = response.to_task_result()
    assert result.status == status
    assert result.message == f"HTTP {code}"
    assert result.metadata["status_code"] == code
    if code == 429:
        assert result.metadata["retry_after"] == 12.0


@pytest.mark.parametrize("value", ["0", "-1", "nan", "inf", "unknown", "Wed, 21 Oct 2015 07:28:00 GMT"])
def test_response_unknown_retry_after(value):
    response = HttpResponse(status_code=429, headers={"retry-after": value}, body=b"")
    assert response.to_task_result().metadata["retry_after"] is None


@pytest.mark.parametrize("fetch_name", ["urllib", "requests"])
@pytest.mark.parametrize("code", [429, 500, 404])
def test_fetch_returns_error_response(local_http_server, fetch_name, code):
    if fetch_name == "requests":
        pytest.importorskip("requests")
    fetch = urllib_fetch if fetch_name == "urllib" else requests_fetch
    response = fetch(f"{local_http_server}/{code}")
    assert response.status_code == code
    if code == 429:
        assert response.headers.get("retry-after") == "15"
        assert response.body == b"rate limited"


@pytest.mark.parametrize("code,calls", [(429, 1), (404, 1), (503, 3)])
def test_executor_returns_last_response_without_implicit_task_result(code, calls):
    count = 0
    response = HttpResponse(status_code=code, headers={}, body=b"last")

    def fetch():
        nonlocal count
        count += 1
        return response

    assert HttpExecutor(max_retries=2, retry_delay_base=0).execute(fetch) is response
    assert count == calls


def test_executor_network_recovery_and_original_exhausted_exception():
    error = ConnectionError("断线")
    response = HttpResponse(status_code=200, headers={}, body=b"ok")
    count = 0

    def fetch():
        nonlocal count
        count += 1
        if count == 1:
            raise error
        return response

    assert HttpExecutor(max_retries=1, retry_delay_base=0).execute(fetch) is response
    assert count == 2

    def fail():
        raise error

    with pytest.raises(ConnectionError) as caught:
        HttpExecutor(max_retries=2, retry_delay_base=0).execute(fail)
    assert caught.value is error


@pytest.mark.parametrize("fetch_name", ["urllib", "requests"])
def test_fetch_keeps_natural_transport_exception(monkeypatch, fetch_name):
    import tasklite.wrappers.http as http_mod

    original = TimeoutError("原始网络故障")

    def fail(*args, **kwargs):
        raise original

    if fetch_name == "urllib":
        class Opener:
            open = staticmethod(fail)

        monkeypatch.setattr(http_mod, "_build_urllib_opener", lambda *args: Opener())
        fetch = urllib_fetch
    else:
        pytest.importorskip("requests")
        class Session:
            request = staticmethod(fail)

        fetch = lambda url: requests_fetch(url, session=Session())
    with pytest.raises(TimeoutError) as caught:
        fetch("http://example.test")
    assert caught.value is original
    import traceback
    assert "fail" in [frame.name for frame in traceback.extract_tb(caught.value.__traceback__)]


def _fetch_status_handler(job, ctx):
    return urllib_fetch(job.payload["url"]).to_task_result()


@pytest.mark.parametrize("code,count", [(404, 1), (429, 2), (500, 2)])
def test_handler_explicitly_returns_http_result(local_http_server, tmp_path, code, count):
    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as app:
        app.register_task("http", _fetch_status_handler, max_retries_per_run=1)
        app.enqueue(Job("http", "a", payload={"url": f"{local_http_server}/{code}"}))
        app.run()
        with StateSnapshot.open(app) as snapshot:
            attempts = list(snapshot.iter_attempts())
            record = snapshot.get_job("http::a")
        assert record["status"] == "failed"
        assert len(attempts) == count
        assert [a["outcome"] for a in attempts] == (["retry", "failure"] if count == 2 else ["failure"])
        assert record["result"]["message"] == f"HTTP {code}"
        assert record["result"]["metadata"]["status_code"] == code
        assert record["result"]["exception"] is None
