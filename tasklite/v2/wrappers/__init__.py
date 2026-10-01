"""v2 通用编排模式封装（wrappers）：discovery / http。

分层红线：本包是 v2 公开面之上的适配层，可依赖 ``v2/{models,utils,engine}``
公开面；``v2/utils/`` 严禁反向 import 本包，v2 核心层严禁依赖本包与
``v2/contrib/``。本子包不进 ``tasklite.v2`` 主 ``__all__``——经
``tasklite.v2.wrappers.*`` 子包路径导入。
"""

from . import discovery
from .discovery import (
    DiscoveryContext,
    DiscoveryHandler,
    DiscoveryHost,
    DiscoveryJob,
    encode_content_id,
    register_discovery,
)

__all__ = [
    "discovery",
    "DiscoveryContext",
    "DiscoveryHandler",
    "DiscoveryHost",
    "DiscoveryJob",
    "encode_content_id",
    "register_discovery",
]
