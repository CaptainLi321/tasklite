"""Payload 与资源数值校验工具薄适配层。"""

from typing import Any
from ..taxonomy import _DEFAULT_TAXONOMY


def validate_resource_amounts(resources: dict, where: str) -> None:
    """校验资源 amount 数值（Job.__init__ 与 pipeline 注册路径共用单点）。"""
    _DEFAULT_TAXONOMY.ensure_resources_valid(resources, where)


def validate_payload(payload: dict, schema: Any) -> list:
    """Validate payload against a TypedDict schema using runtime type hints."""
    return _DEFAULT_TAXONOMY.validate_payload(payload, schema).as_error_strings()
