"""文件操作授权查询、撤销、自动同意与等待生命周期验收用例集。

验收场景：
1. 一个 worker 等待授权时另一 worker 仍能完成，stop 解除等待且正常其他任务排空。
2. 目录授权可复用，撤销子路径不产生父目录例外，实际清理错误与拒绝有不同去向。
3. 迟到回复不能解锁新 Attempt；拒绝后同 Run 不再询问，下次 Run 重新判断；内存授权随实例存活。
4. TASKLITE_AUTO_APPROVE=1 只自动同意实际请求路径，持久记录，保护路径仍禁止。
5. list_authorizations / revoke_authorization 限 Run 外调用，规范路径精确撤销。
"""

import os
import threading
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from tasklite import (
    FileSpec,
    Job,
    TaskLite,
    TaskResult,
)


def _slow_declaring_handler(job, ctx):
    ctx.declare_file(
        "blocked_out",
        FileSpec("pending_auth.txt", role="output", delete_existing=True),
    )
    return TaskResult.success()


def _quick_handler(job, ctx):
    return TaskResult.success(metadata={"quick": True})


def _dir_handler(job, ctx):
    ctx.declare_file(
        "sub_file",
        FileSpec("managed_dir/sub/item.txt", role="output", delete_existing=True),
    )
    return TaskResult.success()


def _cleanup_fail_handler(job, ctx):
    ctx.declare_file(
        "clean_target",
        FileSpec("clean_me.txt", role="output", delete_on_success=True),
    )
    (Path(ctx.base_dir) / "clean_me.txt").write_text("content", encoding="utf-8")
    return TaskResult.success()


def _protected_path_handler(job, ctx):
    ctx.declare_file(
        "bad_out",
        FileSpec("t_auto_approve.db", role="output", delete_existing=True),
    )
    return TaskResult.success()


def _target_handler(job, ctx):
    ctx.declare_file(
        "dyn_file",
        FileSpec("target.txt", role="output", delete_existing=True),
    )
    return TaskResult.success()


def _good_handler(job, ctx):
    ctx.declare_file("out1", FileSpec("my_out.txt", role="output", delete_existing=True))
    return TaskResult.success()


class _MutableAuthorizer:
    def __init__(self, allowed: bool = False):
        self.allowed = allowed

    def __call__(self, path: str, target_type: str) -> bool:
        return self.allowed


def test_worker_wait_authorization_concurrency_and_stop(tmp_path, monkeypatch):
    """一个 worker 等待授权时另一 worker 仍能完成，stop 解除等待且正常其他任务排空。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_auth_wait.db", max_workers=2)

    def _authorizer(path, target_type):
        if "pending_auth.txt" in str(path):
            return None
        return True

    p.authorizer = _authorizer
    p.register_task("slow_decl", _slow_declaring_handler)
    p.register_task("quick", _quick_handler)

    p.enqueue(Job("slow_decl", "s1"))
    p.enqueue(Job("quick", "q1"))

    def _stop_monitor():
        for _ in range(50):
            time.sleep(0.05)
            if "quick::q1" in p.backend.load_successes():
                p.stop()
                break

    monitor_th = threading.Thread(target=_stop_monitor, daemon=True)
    monitor_th.start()

    (tmp_path / "pending_auth.txt").write_text("old", encoding="utf-8")
    p.run()
    monitor_th.join(timeout=2.0)

    # 1. 快速 worker 正常成功完成并结算
    successes = p.backend.load_successes()
    assert "quick::q1" in successes

    # 2. 等待授权的 worker 随 stop 解除等待，安全结束执行，回到 pending
    assert "slow_decl::s1" not in successes
    failed = p.backend.load_failed()
    assert "slow_decl::s1" not in failed

    queue = p.backend.load_queue_jobs()
    pending_uids = [j["job_uid"] for j in queue if j.get("status") == "pending"]
    assert "slow_decl::s1" in pending_uids


def test_directory_authorization_reusable_and_subpath_revocation(tmp_path, monkeypatch):
    """目录授权可复用，撤销子路径不产生父目录例外，实际清理错误与拒绝有不同去向。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_dir_auth.db")

    dir_path = (tmp_path / "managed_dir").resolve()
    sub_file = (dir_path / "sub" / "item.txt").resolve()
    sub_file.parent.mkdir(parents=True, exist_ok=True)
    sub_file.write_text("sub content", encoding="utf-8")

    # 1. 预先授予目录授权
    p.authorizer = None
    p.register_task("dir_task", _dir_handler)
    p.enqueue(Job("dir_task", "d1"))

    # 尚未授权时拒绝
    p.run()
    assert "dir_task::d1" not in p.backend.load_successes()

    # 记录目录授权
    p._runtime.store.record_authorization(dir_path, "directory")
    authorizations = p.list_authorizations()
    assert any(a["path"] == str(dir_path) and a["target_type"] == "directory" for a in authorizations)

    # 2. 撤销子路径不能覆盖父目录授权，返回 False
    revoked = p.revoke_authorization(sub_file)
    assert revoked is False
    # 子路径依然在父目录授权范围内
    assert p._runtime.store.is_path_authorized(sub_file) is True

    # 3. 运行：目录授权可复用，子路径声明执行成功
    p.run()
    assert "dir_task::d1" in p.backend.load_successes()

    # 4. 精确撤销父目录授权，子路径不再被授权
    assert p.revoke_authorization(dir_path) is True
    assert p._runtime.store.is_path_authorized(sub_file) is False


def test_cleanup_failure_vs_authorization_denial_destinations(tmp_path, monkeypatch):
    """实际清理错误记为 failed 档案，授权拒绝回到 pending 且不计入 failed。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_diff_dest.db")

    p.register_task("clean_task", _cleanup_fail_handler)
    p.enqueue(Job("clean_task", "c1"))

    # 构造实际清理错误（磁盘 IO 失败）
    with patch("tasklite.utils.artifact.ArtifactPathPolicy.cleanup", side_effect=OSError("Read-only file system")):
        p.run()

    # 清理错误落入 failed 档案
    failed = p.backend.load_failed()
    assert "clean_task::c1" in failed
    assert "Success cleanup failed" in failed["clean_task::c1"].get("error_message", "")
    assert "clean_task::c1" not in p.backend.load_successes()


def test_late_reply_cannot_unlock_attempt_and_recheck_next_run(tmp_path, monkeypatch):
    """迟到回复不能解锁新 Attempt；拒绝后同 Run 不再询问，下次 Run 重新判断；内存授权随实例存活。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="memory")

    auth_cb = _MutableAuthorizer(allowed=False)
    p.authorizer = auth_cb

    p.register_task("target_task", _target_handler)
    p.register_task("downstream", _quick_handler)

    p.enqueue(Job("target_task", "t1"))
    p.enqueue(Job("downstream", "d1", depends_on=["target_task::t1"]))

    (tmp_path / "target.txt").write_text("old data", encoding="utf-8")

    # 第一次 Run：授权拒绝
    p.run()

    # 1. target_task::t1 与下游 d1 被排除，未写入 failed 档案
    assert "target_task::t1" not in p.backend.load_successes()
    assert "downstream::d1" not in p.backend.load_successes()
    assert "target_task::t1" not in p.backend.load_failed()
    assert "downstream::d1" not in p.backend.load_failed()

    initial_attempts = len(p.backend.load_settled_attempts("target_task::t1"))
    # ADR-0011 约定：动态声明授权拒绝结算 Attempt.failure，Job 留 pending
    assert initial_attempts == 1

    # 2. 模拟伪造过期 execution_id 的迟到消息，验证管道不会以此制造/解锁 Attempt
    channel = p._runtime.channel
    channel._results["stale_exec_id"] = {
        "status": "success",
        "job_uid": "target_task::t1",
        "result": {"success": True},
    }
    # step 单步推进不会拾取过期 execution_id
    p._runtime.step()
    attempts_after_stale = p.backend.load_settled_attempts("target_task::t1")
    assert len(attempts_after_stale) == initial_attempts

    # 3. 下一次 Run 重新判断：开启授权后成功完成
    auth_cb.allowed = True
    p.run()

    assert "target_task::t1" in p.backend.load_successes()
    assert "downstream::d1" in p.backend.load_successes()
    attempts_final = p.backend.load_settled_attempts("target_task::t1")
    assert len(attempts_final) == 2
    assert attempts_final[0]["outcome"] == "failure"
    assert attempts_final[1]["outcome"] == "success"

    # 4. 内存授权随实例存活：在同一实例上持久有效
    auths = p.list_authorizations()
    assert len(auths) >= 1
    assert any("target.txt" in a["path"] for a in auths)


def test_auto_approve_persists_actual_path_and_forbids_protected_roots(tmp_path, monkeypatch):
    """TASKLITE_AUTO_APPROVE=1 只自动同意实际请求路径，持久记录，保护路径仍禁止。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    state_path = tmp_path / "t_auto_approve.db"
    p = TaskLite(backend="sqlite", state_path=state_path)

    p.register_task("good_task", _good_handler)
    p.register_task("bad_task", _protected_path_handler)

    p.enqueue(Job("good_task", "g1"))
    (tmp_path / "my_out.txt").write_text("old", encoding="utf-8")
    p.run()

    # 1. 实际请求路径被自动同意并持久记录
    assert "good_task::g1" in p.backend.load_successes()
    auths = p.list_authorizations()
    expected_path = str((tmp_path / "my_out.txt").resolve())
    assert any(a["path"] == expected_path for a in auths)
    # 不会预先全量授权未请求的路径或整个根目录
    unrelated_path = str((tmp_path / "unrelated.txt").resolve())
    assert not any(a["path"] == unrelated_path for a in auths)

    # 2. 保护路径（例如 state 数据库文件）即使 TASKLITE_AUTO_APPROVE=1 也严禁授权
    p.enqueue(Job("bad_task", "b1"))
    p.run()

    # bad_task 声明重叠受保护目录，直接被拒绝
    assert "bad_task::b1" not in p.backend.load_successes()
    assert not p._runtime.store.is_path_authorized(state_path)


def test_list_and_revoke_authorizations_surface_and_run_guard(tmp_path, monkeypatch):
    """list_authorizations 与 revoke_authorization 规范格式、精确匹配与 Run 守卫。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="sqlite", state_path=tmp_path / "t_mgmt_guard.db")

    f1 = tmp_path / "doc.txt"
    f1.write_text("data", encoding="utf-8")

    # 1. 空授权查询
    assert p.list_authorizations() == []

    # 2. 记录一条授权并验证结构字段样本
    p._runtime.store.record_authorization(f1, "file")
    records = p.list_authorizations()
    assert len(records) == 1
    rec = records[0]
    assert rec["path"] == str(f1.resolve())
    assert rec["target_type"] == "file"
    assert isinstance(rec["authorized_at"], str)

    # 3. 精确撤销
    assert p.revoke_authorization(f1) is True
    # 再次撤销不存在的记录返回 False
    assert p.revoke_authorization(f1) is False
    assert p.list_authorizations() == []

    # 4. Run 运行期守卫：运行中调用 list/revoke 抛 RuntimeError
    with patch.object(type(p), "is_running", new_callable=lambda: property(lambda self: True)):
        with pytest.raises(RuntimeError, match="is only allowed outside run"):
            p.list_authorizations()

        with pytest.raises(RuntimeError, match="is only allowed outside run"):
            p.revoke_authorization("foo.txt")
