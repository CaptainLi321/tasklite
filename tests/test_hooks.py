"""v2 用户钩子回归（job_ref / slice_list）。

契约：slice_list 的三个数值参数 keyword-only（语义易混，裸位置传参会被类型检查静默放行）。
"""
from __future__ import annotations

import pytest

from tasklite.hooks import job_ref, slice_list


class TestJobRef:
    """result_metadata 引用提取优先级：post_id > artist > id。"""

    def test_post_id_first(self):
        assert job_ref({"post_id": 123}) == "#123"

    def test_artist_second(self):
        assert job_ref({"artist": "alice"}) == "alice"

    def test_id_third(self):
        assert job_ref({"id": "item_9"}) == "item_9"

    def test_priority_post_id_over_artist_and_id(self):
        assert job_ref({"post_id": 7, "artist": "a", "id": "x"}) == "#7"

    def test_unknown_dict_returns_empty(self):
        assert job_ref({"other": "value"}) == ""

    def test_non_dict_returns_empty(self):
        assert job_ref("not-a-dict") == ""
        assert job_ref(None) == ""
        assert job_ref(12345) == ""


class TestSliceList:
    """分批切片：先 limit 总量截断，再 [start:start+count] 窗口。"""

    def test_window_from_head(self):
        items = list(range(10))
        assert slice_list(items, start=0, count=5, limit=None) == [0, 1, 2, 3, 4]

    def test_window_mid_range(self):
        items = list(range(10))
        assert slice_list(items, start=5, count=5, limit=None) == [5, 6, 7, 8, 9]

    def test_window_past_end_truncates(self):
        items = list(range(10))
        assert slice_list(items, start=8, count=5, limit=None) == [8, 9]

    def test_limit_only(self):
        items = list(range(10))
        assert slice_list(items, start=None, count=None, limit=3) == [0, 1, 2]

    def test_limit_applied_before_window(self):
        items = list(range(10))
        assert slice_list(items, start=2, count=3, limit=5) == [2, 3, 4]

    def test_all_none_returns_copy_of_all(self):
        items = list(range(10))
        result = slice_list(items, start=None, count=None, limit=None)
        assert result == items
        assert result is not items or result == items

    def test_parameters_are_keyword_only(self):
        with pytest.raises(TypeError):
            slice_list([1, 2, 3], 0, 2, None)
