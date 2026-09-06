"""tasklite 通用脚手架与标准适配器测试（纯离线）。"""

import hashlib
import io
import sys
from contextlib import redirect_stdout
import pytest
from tasklite import (
    content_fingerprint,
    job_ref,
    progress_hook,
    sanitize_job_component,
    slice_list,
)
from tasklite.pipeline import TaskLite


def test_content_fingerprint_deterministic_and_version_salted():
    a = content_fingerprint(["encode", "dir/a.mkv", 100, "sub.ass"], version="2")
    assert a == content_fingerprint(["encode", "dir/a.mkv", 100, "sub.ass"], version="2")
    assert a != content_fingerprint(["encode", "dir/a.mkv", 100, "sub.ass"], version="3")
    assert a != content_fingerprint(["encode", "dir/a.mkv", 101, "sub.ass"], version="2")
    assert len(a) == 16


def test_sanitize_job_component_escapes_uid_separator_and_path():
    assert sanitize_job_component("a::b/c\\d") == "a%3A%3Ab%2Fc%5Cd"
    assert sanitize_job_component("") == "untitled"
    assert "::" not in sanitize_job_component("x::y")


def test_sanitize_job_component_injective_no_silent_collision():
    inputs = [
        "a/b", "a:b", "a\\b", "a\tb", "ab",      # 特殊字符转义后不发生碰撞
        ":::", "///", "\\\\",                     # 纯特殊符号转义后互不碰撞
        "a%b", "a%2Fb", "a%25b",                  # 转义符本身必须被转义，防编码歧义
        "a b", "图片 01",
    ]
    outputs = [sanitize_job_component(s) for s in inputs]
    assert len(set(outputs)) == len(outputs)
    assert sanitize_job_component("a/b") == "a%2Fb"
    assert sanitize_job_component("a:b") == "a%3Ab"
    assert sanitize_job_component("a\\b") == "a%5Cb"
    assert sanitize_job_component("a\tb") == "a%09b"
    assert sanitize_job_component("a%b") == "a%25b"
    assert sanitize_job_component("a%2Fb") == "a%252Fb"
    assert sanitize_job_component("abc-_.123") == "abc-_.123"
    for out in outputs:
        assert not (set(out) & set("/\\:"))
        assert all(c.isprintable() for c in out)


def test_sanitize_job_component_long_input_truncate_with_fingerprint():
    a = "x" * 200
    b = "x" * 150 + "y" + "x" * 49
    out_a, out_b = sanitize_job_component(a), sanitize_job_component(b)
    assert len(out_a) <= 120 and len(out_b) <= 120
    assert out_a != out_b
    assert out_a.endswith("_" + hashlib.sha256(a.encode()).hexdigest()[:8])


def test_job_ref_extraction():
    assert job_ref({"post_id": 123}) == "#123"
    assert job_ref({"artist": "alice"}) == "alice"
    assert job_ref({"id": "item_9"}) == "item_9"
    assert job_ref({"other": "value"}) == ""
    assert job_ref("not-a-dict") == ""


def test_progress_hook_outputs_expected_lines():
    buf = io.StringIO()
    with redirect_stdout(buf):
        progress_hook("fetch::p1", {"post_id": 10}, success=True, going_to_retry=False)
        progress_hook("fetch::p2", {"artist": "bob"}, success=False, going_to_retry=True)
        progress_hook("fetch::p3", {}, success=False, going_to_retry=False)

    lines = buf.getvalue().splitlines()
    assert "✓ fetch #10" in lines[0]
    assert "⟳ fetch bob 失败，退避重试" in lines[1]
    assert "✗ fetch → DLQ" in lines[2]


def test_slice_list_scenarios():
    items = list(range(10))
    assert slice_list(items, start=0, count=5, limit=None) == [0, 1, 2, 3, 4]
    assert slice_list(items, start=5, count=5, limit=None) == [5, 6, 7, 8, 9]
    assert slice_list(items, start=8, count=5, limit=None) == [8, 9]
    assert slice_list(items, start=None, count=None, limit=3) == [0, 1, 2]
    assert slice_list(items, start=2, count=3, limit=5) == [2, 3, 4]
    assert slice_list(items, start=None, count=None, limit=None) == items
