"""通用编排模式封装（wrappers）：discovery / http。

- discovery：通用增量扫描回调封装（register_discovery + DiscoveryHandler
  + 框架无关三协议）。
- http：官方轻量网络工具库（HttpResponse, HttpPolicy, http_guard,
  SnapshotStore, urllib_fetch 等）。

本包不设符号级再导出面，符号一律经 ``tasklite.wrappers.<模块>``
导入（如 ``from tasklite.wrappers.discovery import register_discovery``）。

分层红线：本包是公开面之上的适配层，可依赖
``tasklite/{models,utils,engine}`` 公开面；``tasklite/utils/`` 严禁反向
import 本包，核心层严禁依赖本包与 ``tasklite/contrib/``。本子包不进
``tasklite`` 主 ``__all__``——经 ``tasklite.wrappers.*`` 子包路径导入。
"""

from . import discovery, http

__all__ = ["discovery", "http"]
