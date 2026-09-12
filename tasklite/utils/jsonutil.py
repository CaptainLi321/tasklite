"""JSON 序列化统一出口（契约：序列化一致性）。

所有落盘/传输 JSON 必须经本模块的 ``dumps``/``loads``：

- ``dumps``：强制 ``allow_nan=False``——拒绝 NaN/Infinity 写出非标准 JSON
  token（NaN 落盘后读取端拒绝加载 → 启动即崩）。
- ``loads``：``parse_constant`` 拒绝 NaN/Infinity/-Infinity 字面 token；
  ``parse_float`` 拒绝溢出为无穷的浮点字面量（如 ``1e400``——不经过
  parse_constant，默认解析静默产出 inf，下游 ``dumps(allow_nan=False)``
  回写时才炸或比较语义失真）；``parse_int`` 把超长整数字面量解析限制的
  裸 ``ValueError`` 包装为 ``json.JSONDecodeError``——且所有拒绝均抛
  ``json.JSONDecodeError``（非裸 ``ValueError``）——调用方既有的
  ``except json.JSONDecodeError`` 分支（sqlite_backend.load_* 等）才能
  真正捕获；裸 ValueError 会逃逸既有 except 分支。

静态契约：tests 扫描源码断言本模块之外无裸 ``json.dumps``/``json.loads``
（豁免：``models/state.py`` 的 hash 计算——非落盘/传输用途）。
"""
from __future__ import annotations

import json
import math


def _reject_constant(name: str):
    """parse_constant 回调：拒绝 NaN/Infinity，抛 JSONDecodeError。

    JSONDecodeError 是 ValueError 子类——调用方按「损坏数据」处理的分支
    （``except json.JSONDecodeError``）能捕获；裸 ValueError 会逃逸崩溃。
    """
    raise json.JSONDecodeError(
        f"JSON constant {name!r} is not allowed (NaN/Infinity rejected)", "", 0
    )


def _finite_float(s: str) -> float:
    """parse_float 回调：拒绝溢出为 ±inf 的浮点字面量。

    ``1e400`` 这类合法语法的浮点 token 不经过 parse_constant，默认解析
    静默产出 ``float('inf')``——与 NaN 同样破坏 dumps 回写与数值比较。
    统一按损坏数据拒绝（JSONDecodeError）。
    """
    val = float(s)
    if not math.isfinite(val):
        raise json.JSONDecodeError(
            f"JSON float {s!r} overflows to non-finite (rejected)", "", 0
        )
    return val


def _bounded_int(s: str) -> int:
    """parse_int 回调：把整数字面量解析限制的裸 ValueError 包装为 JSONDecodeError。

    Python 3.11+ 对超过 int↔str 转换位数上限（默认 4300 位）的整数字面量，
    ``int(s)`` 抛裸 ``ValueError``——逃逸调用方既有的 ``except
    json.JSONDecodeError`` 损坏数据分支（sqlite_backend.load_* 等）。
    统一按损坏数据拒绝，doc 参数携带字面量供定位。
    """
    try:
        return int(s)
    except ValueError as exc:
        raise json.JSONDecodeError(
            f"JSON integer literal {s!r} exceeds int conversion limit: {exc}", s, 0
        ) from exc


def dumps(obj) -> str:
    """序列化（ensure_ascii=False 保中文可读 + allow_nan=False 拒 NaN）。"""
    return json.dumps(obj, ensure_ascii=False, allow_nan=False)


def dump(obj, fp):
    """序列化到文件对象（与 dumps 同契约）。"""
    json.dump(obj, fp, ensure_ascii=False, allow_nan=False)


def loads(s: str):
    """反序列化（拒绝 NaN/Infinity/溢出浮点/超限整数，抛 JSONDecodeError）。"""
    return json.loads(
        s, parse_constant=_reject_constant, parse_float=_finite_float, parse_int=_bounded_int
    )


def load(fp):
    """从文件对象反序列化（同 loads 的拒绝语义）。"""
    return json.load(
        fp, parse_constant=_reject_constant, parse_float=_finite_float, parse_int=_bounded_int
    )
