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
    register_file_transients,
    clear_dlq,
    run_pipeline,
    sanitize_job_component,
    slice_list,
)

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
    # 单射性回归：旧删除式实现让这些输入两两撞车（'a/b'/'a:b'/'a\tb' 全变
    # 'ab'，':::'/'///' 全变 'untitled'）→ 同一 uid → wall 去重静默吞任务。
    # 转义式必须保证两两不同输出。
    inputs = [
        "a/b", "a:b", "a\\b", "a\tb", "ab",      # 特殊字符转义后不发生碰撞
        ":::", "///", "\\\\",                     # 纯特殊符号转义后互不碰撞
        "a%b", "a%2Fb", "a%25b",                  # 转义符本身必须被转义，防编码歧义
        "a b", "图片 01",
    ]
    outputs = [sanitize_job_component(s) for s in inputs]
    assert len(set(outputs)) == len(outputs)
    # 锁定具体转义结果，防止实现悄悄退回替换/删除式
    assert sanitize_job_component("a/b") == "a%2Fb"
    assert sanitize_job_component("a:b") == "a%3Ab"
    assert sanitize_job_component("a\\b") == "a%5Cb"
    assert sanitize_job_component("a\tb") == "a%09b"
    assert sanitize_job_component("a%b") == "a%25b"
    assert sanitize_job_component("a%2Fb") == "a%252Fb"
    # 干净成分原样透传（可读性：可打印且非禁止字符不变形）
    assert sanitize_job_component("abc-_.123") == "abc-_.123"
    # 输出可作文件名安全成分：无路径分隔符/冒号/不可打印字符
    for out in outputs:
        assert not (set(out) & set("/\\:"))
        assert all(c.isprintable() for c in out)

def test_sanitize_job_component_long_input_truncate_with_fingerprint():
    # 超长输入截断 + 8 位指纹：纯截断会让前缀相同的长输入相撞（破坏单射）
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

class _FakePipeline:
    def __init__(self):
        self.registered = []
        self.revived_kwargs = None
    def register_transient_exception(self, cls):
        self.registered.append(cls)
    def clear_dlq(self, task_types=None, keep_fatal=True):
        self.revived_kwargs = dict(task_types=task_types, keep_fatal=keep_fatal)
        return 3

def test_register_file_transients_uses_instance_method():
    p = _FakePipeline()
    register_file_transients(p)
    assert PermissionError in p.registered
    assert BlockingIOError in p.registered
    assert ConnectionResetError in p.registered
    assert FileNotFoundError not in p.registered  # 确定性错误不注册

def test_clear_dlq_wrapper():
    p = _FakePipeline()
    assert clear_dlq(p, task_types=["scan"], keep_fatal=False) == 3
    assert p.revived_kwargs == dict(task_types=["scan"], keep_fatal=False)
    assert clear_dlq(p) == 3
    assert p.revived_kwargs == dict(task_types=None, keep_fatal=True)

def test_clear_dlq_wrapper_empty_list_passthrough():
 # task_types=[] 必须如实透传（删 0 条），不得被 truthy
 # 判断改写为 None（= 清全部 DLQ 的数据删除事故）
    p = _FakePipeline()
    clear_dlq(p, task_types=[])
    assert p.revived_kwargs == dict(task_types=[], keep_fatal=True)

def test_run_pipeline_calls_stop(tmp_path):
    import io as _io
    calls = []
    class P:
        def run(self):
            calls.append("run")
        def stop(self):
            calls.append("stop")
    run_pipeline(P())
    assert calls == ["run", "stop"]
