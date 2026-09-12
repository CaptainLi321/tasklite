"""tasklite.utils.jsonutil 契约测试：所有拒绝统一抛 JSONDecodeError。"""

import json

import pytest

from tasklite.utils.jsonutil import dumps, loads


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
        # 限制只针对字面量位数（>4300 位拒收），限制内的大整数读写对称不受影响
        big = 10 ** 4200
        assert loads(dumps([big])) == [big]
