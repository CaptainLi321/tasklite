"""tasklite.utils.jsonutil 契约测试：所有拒绝统一抛 JSONDecodeError。"""

from __future__ import annotations

import io
import json

import pytest

from tasklite.utils.jsonutil import dump, dumps, load, loads


class TestLoadsRejectsAsJSONDecodeError:
    """loads 的损坏数据拒绝必须落在 JSONDecodeError——调用方 except 分支可捕获。"""

    def test_oversized_int_literal_rejected_as_json_decode_error(self):
        # Python 3.11+ int↔str 转换位数上限（默认 4300）使裸 int() 抛 ValueError，
        # parse_int 钩子须包装为 JSONDecodeError 才能命中容灾分支
        raw = "[" + "9" * 4301 + "]"
        with pytest.raises(json.JSONDecodeError):
            loads(raw)

    def test_normal_ints_and_reject_channels_unchanged(self):
        # 未超限整数（含负数）照常解析；NaN/溢出浮点既有拒绝通道不受影响
        assert loads("[-1, 0, 4300]") == [-1, 0, 4300]
        with pytest.raises(json.JSONDecodeError):
            loads("NaN")
        with pytest.raises(json.JSONDecodeError):
            loads("1e400")

    def test_large_but_within_limit_int_roundtrip(self):
        # 限制只针对字面量位数（超上限位数拒收），限制内的大整数读写对称不受影响
        big = 10**4200
        assert loads(dumps([big])) == [big]

    def test_overflow_float_rejected(self):
        """浮点溢出拒绝：1e400 类合法语法浮点溢出必须拒绝而非静默 inf。

        parse_constant 只拦 NaN/Infinity 字面 token；``1e400`` 走 parse_float
        默认解析产出 float('inf')——与 NaN 同样破坏 dumps(allow_nan=False)
        回写与数值比较。统一按损坏数据抛 JSONDecodeError。
        """
        for bad in ('{"v": 1e400}', '{"v": -1e400}', '{"v": 2e308}', "[1e999]"):
            with pytest.raises(json.JSONDecodeError):
                loads(bad)
        # 合法值不受影响：大而有限的浮点、普通小数、int
        assert loads('{"a": 1.5, "b": 1e10, "c": -0.001}') == {
            "a": 1.5, "b": 1e10, "c": -0.001}
        # NaN/Infinity 字面 token 拒绝语义未被挤掉
        with pytest.raises(json.JSONDecodeError):
            loads("[NaN, Infinity]")

    def test_nan_and_infinity_constants_rejected(self):
        for bad in ("NaN", "Infinity", "-Infinity", "[NaN]", "[Infinity, -Infinity]"):
            with pytest.raises(json.JSONDecodeError):
                loads(bad)


class TestDumpsRejectsNonFinite:
    """dumps 强制 allow_nan=False——非有限数拒绝写出非标准 JSON token。"""

    def test_dumps_rejects_nan_and_inf(self):
        with pytest.raises(ValueError):
            dumps({"x": float("nan")})
        with pytest.raises(ValueError):
            dumps({"x": float("inf")})
        with pytest.raises(ValueError):
            dumps({"x": float("-inf")})

    def test_dumps_keeps_chinese_readable(self):
        # ensure_ascii=False：中文原样写出（可读性契约）
        assert dumps({"名": "任务"}) == '{"名": "任务"}'

    def test_roundtrip_preserves_payload(self):
        payload = {"k": [1, 2.5, "中文", None, True], "nested": {"a": {}}}
        assert loads(dumps(payload)) == payload


class TestFileObjectApi:
    """dump/load 文件对象接口与字符串接口同契约。"""

    def test_dump_and_load_roundtrip(self, tmp_path):
        target = tmp_path / "payload.json"
        payload = {"items": [1, 2], "note": "落盘"}
        with open(target, "w", encoding="utf-8") as fp:
            dump(payload, fp)
        with open(target, encoding="utf-8") as fp:
            assert load(fp) == payload

    def test_load_rejects_overflow_float_from_file(self):
        with pytest.raises(json.JSONDecodeError):
            load(io.StringIO('{"v": 1e400}'))

    def test_load_rejects_oversized_int_from_file(self):
        raw = "[" + "9" * 4301 + "]"
        with pytest.raises(json.JSONDecodeError):
            load(io.StringIO(raw))
