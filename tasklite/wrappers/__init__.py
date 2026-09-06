"""通用编排模式封装（wrappers）：discovery / http。

- discovery：通用增量扫描回调封装（register_discovery + DiscoveryHandler + 协议）。
- http：官方轻量网络工具库（HttpResponse, HttpPolicy, http_guard, SnapshotStore 等）。

公共脚手架见 ``tasklite.pipeline_util``。
"""

from . import discovery, http
from .discovery import (
    DiscoveryContext,
    DiscoveryHandler,
    DiscoveryHost,
    DiscoveryJob,
    register_discovery,
    sanitize_content_id,
)
from .http import (
    HttpResponse,
    HttpPolicy,
    http_guard,
    guard_request,
    guarded_fetch,
    SnapshotStore,
    SQLiteSnapshotStore,
    MemorySnapshotStore,
    parse_netscape_cookies,
    format_cookie_header,
    fetch_urllib,
    fetch_requests,
)

__all__ = [
    "discovery",
    "http",
    "DiscoveryContext",
    "DiscoveryHandler",
    "DiscoveryHost",
    "DiscoveryJob",
    "register_discovery",
    "sanitize_content_id",
    "HttpResponse",
    "HttpPolicy",
    "http_guard",
    "guard_request",
    "guarded_fetch",
    "SnapshotStore",
    "SQLiteSnapshotStore",
    "MemorySnapshotStore",
    "parse_netscape_cookies",
    "format_cookie_header",
    "fetch_urllib",
    "fetch_requests",
]

