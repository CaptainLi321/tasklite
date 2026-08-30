"""tasklite.pipeline_util 通用管线脚手架测试（纯离线）。"""
import hashlib
import io
import sys
from contextlib import redirect_stdout
import pytest
from tasklite.pipeline_util import (
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
    # 转义式：禁止字符按 UTF-8 字节转 %XX（'/'→%2F、':'→%3A、'\'→%5C），不再删除
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


def test_job_ref_meta_priority():
    assert job_ref({"post_id": "42", "artist": "A"}) == "#42"
    assert job_ref({"artist": "A"}) == "A"
    assert job_ref({"id": "7"}) == "7"
    assert job_ref({}) == ""


def test_progress_hook_happy_paths(capsys):
    progress_hook("t::1", {"post_id": "2"}, True, False)
    progress_hook("t::1", {"post_id": "2"}, False, False)
    progress_hook("t::1", {"post_id": "2"}, False, True)
    out = capsys.readouterr().out
    assert "✓" in out and "⟳" in out and "✗" in out


def test_slice_list():
    items = list(range(10))
    assert slice_list(items, None, None, None) == list(range(10))
    assert slice_list(items, None, None, 3) == [0, 1, 2]
    assert slice_list(items, 5, None, None) == [5, 6, 7, 8, 9]
    assert slice_list(items, 2, 4, None) == [2, 3, 4, 5]


def test_tasklite_register_transient_exceptions_and_file_transients(tmp_path):
    p = TaskLite("test_transients", state_dir=tmp_path)
    p.register_file_transients()
    assert p.transient_registry.matches(PermissionError("disk full"))
    assert p.transient_registry.matches(BlockingIOError())
    assert p.transient_registry.matches(ConnectionResetError())
    assert not p.transient_registry.matches(FileNotFoundError())



def test_tasklite_run_graceful(tmp_path):
    p = TaskLite("test_graceful", state_dir=tmp_path)
    # run_graceful 应能正常执行无任务管线并自然收尾
    p.run_graceful()
    assert p.stats.completed == 0
