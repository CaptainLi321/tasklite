"""通用编排模式封装（wrappers）：discovery。

- discovery：通用增量扫描回调封装（register_discovery + DiscoveryHandler + 协议）。

公共脚手架见 ``tasklite.pipeline_util``。
"""

from . import discovery
from .discovery import (
    DiscoveryContext,
    DiscoveryHandler,
    DiscoveryHost,
    DiscoveryJob,
    register_discovery,
    sanitize_content_id,
)

__all__ = [
    "discovery",
    "DiscoveryContext",
    "DiscoveryHandler",
    "DiscoveryHost",
    "DiscoveryJob",
    "register_discovery",
    "sanitize_content_id",
]
