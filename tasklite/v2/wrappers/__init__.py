"""v2 通用编排模式封装（wrappers）：discovery / http。

- discovery：通用增量扫描回调封装（register_discovery + DiscoveryHandler
  + 框架无关三协议）。
- http：官方轻量网络工具库（HttpResponse, HttpPolicy, http_guard,
  SnapshotStore, urllib_fetch 等）。

分层红线：本包是 v2 公开面之上的适配层，可依赖 ``v2/{models,utils,engine}``
公开面；``v2/utils/`` 严禁反向 import 本包，v2 核心层严禁依赖本包与
``v2/contrib/``。本子包不进 ``tasklite.v2`` 主 ``__all__``——经
``tasklite.v2.wrappers.*`` 子包路径导入。
"""

from . import discovery, http
from .discovery import (
    DiscoveryContext,
    DiscoveryHandler,
    DiscoveryHost,
    DiscoveryJob,
    encode_content_id,
    register_discovery,
)
from .http import (
    HttpResponse,
    HttpPolicy,
    http_guard,
    guard_request,
    guarded,
    SnapshotStore,
    SQLiteSnapshotStore,
    MemorySnapshotStore,
    parse_netscape_cookies,
    format_cookie_header,
    urllib_fetch,
    requests_fetch,
)

__all__ = [
    "discovery",
    "http",
    "DiscoveryContext",
    "DiscoveryHandler",
    "DiscoveryHost",
    "DiscoveryJob",
    "encode_content_id",
    "register_discovery",
    "HttpResponse",
    "HttpPolicy",
    "http_guard",
    "guard_request",
    "guarded",
    "SnapshotStore",
    "SQLiteSnapshotStore",
    "MemorySnapshotStore",
    "parse_netscape_cookies",
    "format_cookie_header",
    "urllib_fetch",
    "requests_fetch",
]
