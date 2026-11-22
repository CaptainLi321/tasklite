"""FileSpec 主进程执行与通信、授权与清理生命周期验收用例集。

覆盖验收清单：
1. from_output 保留配置引用且解析出绝对路径，未发布名称或缺失 input 直接失败。
2. 同声明重复不重复删除，缺失删除目标视为完成；input 禁前删，但成功/失败删除按显式策略允许。
3. 成功清理失败不发布成功/派生/游标，失败清理附加信息不掩盖原原因；配置与实际文件记录分开。
4. 拒绝/stop 期间其他在途可结算；受阻 worker 结束后才回 pending，同 Run 不重复派发。
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from tasklite import (
    FileSpec,
    Job,
    StateSnapshot,
    TaskLite,
    TaskLiteError,
    TaskResult,
)
from tasklite.resource import CapacityResource
from tasklite.testing import authorizer, cleanup_hook, in_process_workers


def _producer_handler(job, ctx):
    out = ctx.declare_file("report", FileSpec("report.txt", role="output", delete_existing=True))
    out.write_text("hello world", encoding="utf-8")
    return TaskResult.success(metadata={"bytes": len("hello world")})


def _consumer_handler(job, ctx):
    src = ctx.file_path("source")
    content = src.read_text(encoding="utf-8")
    return TaskResult.success(metadata={"content": content})


def _noop_handler(job, ctx):
    return TaskResult.success()


def _marker_handler(job, ctx):
    Path(job.payload["marker"]).write_text("started", encoding="utf-8")
    return TaskResult.success()


def _snapshot_map(pipeline: TaskLite) -> dict[str, dict]:
    with StateSnapshot.open(pipeline) as snap:
        return {row["job_uid"]: row for row in snap.iter_jobs()}


def _attempts(pipeline: TaskLite, uid: str) -> list[dict]:
    with StateSnapshot.open(pipeline) as snap:
        return list(snap.iter_attempts(job_uid=uid))


def test_from_output_reference_and_resolution(tmp_path, monkeypatch):
    """from_output 保留配置引用且 Attempt 记录解析出规范绝对路径。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")
    p.register_task("producer", _producer_handler)
    p.register_task("consumer", _consumer_handler)

    producer_job = Job("producer", "p1")
    consumer_job = Job(
        "consumer",
        "c1",
        depends_on=[producer_job.uid],
        files={"source": FileSpec.from_output(producer_job.uid, "report", role="input")},
    )

    p.enqueue(producer_job)
    p.enqueue(consumer_job)
    with in_process_workers(p):
        p.run(install_signals=False)

    jobs = _snapshot_map(p)
    assert jobs[producer_job.uid]["status"] == "succeeded"
    assert jobs[consumer_job.uid]["status"] == "succeeded"
    assert jobs[consumer_job.uid]["result"]["metadata"]["content"] == "hello world"

    attempt, = _attempts(p, consumer_job.uid)
    expected_path = str((tmp_path / "report.txt").resolve())
    assert attempt["job"]["files"]["source"]["producer_uid"] == producer_job.uid
    assert attempt["job"]["files"]["source"]["output_name"] == "report"
    assert attempt["files"]["source"]["path"] == expected_path
    assert attempt["files"]["source"]["role"] == "input"
    p.close()


def test_from_output_unpublished_name_fails_immediately(tmp_path, monkeypatch):
    """from_output 引用生产者未发布产物名称直接失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")
    p.register_task("producer", _producer_handler)
    p.register_task("consumer", _consumer_handler)

    producer_job = Job("producer", "p1")
    consumer_job = Job(
        "consumer",
        "c1",
        depends_on=[producer_job.uid],
        files={"source": FileSpec.from_output(producer_job.uid, "nonexistent", role="input")},
    )

    p.enqueue(producer_job)
    p.enqueue(consumer_job)
    with in_process_workers(p):
        p.run(install_signals=False)

    rec = _snapshot_map(p)[consumer_job.uid]
    assert rec["status"] == "failed"
    assert "did not publish output" in (rec["result"]["message"] or "")
    p.close()


def test_from_output_missing_disk_input_fails_immediately(tmp_path, monkeypatch):
    """上游虽已发布但磁盘 input 产物缺失直接失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")
    p.register_task("producer", _producer_handler)
    p.register_task("consumer", _consumer_handler)

    producer_job = Job("producer", "p1")
    consumer_job = Job(
        "consumer",
        "c1",
        depends_on=[producer_job.uid],
        files={"source": FileSpec.from_output(producer_job.uid, "report", role="input")},
    )

    p.enqueue(producer_job)
    with in_process_workers(p):
        p.run(install_signals=False)

    report_file = tmp_path / "report.txt"
    if report_file.exists():
        report_file.unlink()

    p.enqueue(consumer_job)
    with in_process_workers(p):
        p.run(install_signals=False)

    rec = _snapshot_map(p)[consumer_job.uid]
    assert rec["status"] == "failed"
    assert "Missing input file" in (rec["result"]["message"] or "")
    p.close()


def _idempotent_handler(job, ctx):
    spec = FileSpec("idempotent.txt", role="output", delete_existing=True)
    p1 = ctx.declare_file("same", spec)
    p1.write_text("initial", encoding="utf-8")

    p2 = ctx.declare_file("same", spec)
    assert p1 == p2
    assert p2.read_text(encoding="utf-8") == "initial"

    bad_spec = FileSpec("idempotent.txt", role="cache")
    with pytest.raises(TaskLiteError):
        ctx.declare_file("same", bad_spec)

    return TaskResult.success()


def test_duplicate_declaration_idempotent_and_conflict(tmp_path, monkeypatch):
    """同声明重复不重复删除，缺失删除目标视为完成；同名异声明报冲突。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")
    p.register_task("idem", _idempotent_handler)
    p.enqueue(Job("idem", "j1"))
    with in_process_workers(p):
        p.run(install_signals=False)

    assert _snapshot_map(p)["idem::j1"]["status"] == "succeeded"
    p.close()


def _clean_input_handler(job, ctx):
    ctx.declare_file(
        "temp_in",
        FileSpec("to_clean.txt", role="input", delete_on_success=True),
    )
    return TaskResult.success()


def test_input_role_cleanup_policies(tmp_path, monkeypatch):
    """input 禁前删，但成功/失败删除按显式策略允许。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    with pytest.raises(TaskLiteError, match="input FileSpec cannot set delete_existing=True"):
        FileSpec("input.txt", role="input", delete_existing=True)

    in_file = tmp_path / "to_clean.txt"
    in_file.write_text("temporary input", encoding="utf-8")

    p = TaskLite(backend="memory")
    p.register_task("clean_task", _clean_input_handler)
    p.enqueue(Job("clean_task", "c1"))
    with in_process_workers(p):
        p.run(install_signals=False)

    assert _snapshot_map(p)["clean_task::c1"]["status"] == "succeeded"
    assert not in_file.exists()
    p.close()


def _fail_cleanup_handler(job, ctx):
    ctx.declare_file(
        "del_file",
        FileSpec("undeletable.txt", role="output", delete_on_success=True),
    )
    ctx.spawn(Job("spawned", "s1"))
    ctx.set_cursor("cur_key", "cur_val")
    return TaskResult.success()


@pytest.mark.parametrize("cleanup_error", [OSError, ValueError, NotImplementedError])
def test_success_cleanup_failure_blocks_success_spawn_and_cursor(tmp_path, monkeypatch, cleanup_error):
    """成功清理失败不发布成功/派生/游标，配置与实际文件记录分开。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")
    p.register_task("fail_clean", _fail_cleanup_handler)
    p.register_task("spawned", _noop_handler)
    p.enqueue(Job("fail_clean", "fc1"))

    with in_process_workers(p), cleanup_hook(p, cleanup_error("清理故障")):
        target = tmp_path / "undeletable.txt"
        target.write_text("data", encoding="utf-8")
        p.run(install_signals=False)

    jobs = _snapshot_map(p)
    assert jobs["fail_clean::fc1"]["status"] == "failed"
    assert "Success cleanup failed" in (jobs["fail_clean::fc1"]["result"]["message"] or "")
    attempts = _attempts(p, "fail_clean::fc1")
    assert len(attempts) == 1 and attempts[0]["outcome"] == "failure"
    assert attempts[0]["result"]["exception"]["type"] == cleanup_error.__name__
    assert target.read_text(encoding="utf-8") == "data"
    assert "spawned::s1" not in jobs
    assert p.get_cursor("cur_key") is None
    assert attempts[0]["files"]["del_file"]["path"] == str(target)
    p.close()


def _failing_handler(job, ctx):
    dummy = Path(ctx.declare_file(
        "fail_spec",
        FileSpec("dummy.txt", role="output", delete_on_failure=True),
    ))
    dummy.write_text("fail data", encoding="utf-8")
    raise RuntimeError("Business computation failed")


@pytest.mark.parametrize("cleanup_error", [OSError, ValueError, NotImplementedError])
def test_failure_cleanup_failure_preserves_original_cause(tmp_path, monkeypatch, cleanup_error):
    """handler 已失败且失败清理再次失败：保留原始失败原因，附加清理错误。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")
    p.register_task("failing", _failing_handler)
    p.enqueue(Job("failing", "f1"))

    with in_process_workers(p), cleanup_hook(p, cleanup_error("I/O error during cleanup")):
        p.run(install_signals=False)

    rec = _snapshot_map(p)["failing::f1"]
    assert rec["status"] == "failed"
    error_msg = rec["result"]["message"] or ""
    assert "Business computation failed" in error_msg
    assert "cleanup failed" in error_msg
    assert rec["result"]["exception"]["type"] == "RuntimeError"
    assert rec["result"]["annotations"]["cleanup_exception"]["type"] == cleanup_error.__name__
    attempt, = _attempts(p, "failing::f1")
    assert attempt["outcome"] == "failure"
    assert attempt["result"]["exception"]["type"] == "RuntimeError"
    assert attempt["result"]["annotations"]["cleanup_exception"]["type"] == cleanup_error.__name__
    p.close()


def _auth_denied_handler(job, ctx):
    ctx.declare_file(
        "protected_out",
        FileSpec("sensitive.txt", role="output", delete_existing=True),
    )
    return TaskResult.success()


def _deny_sensitive_authorizer(path, target_type):
    return "sensitive.txt" not in str(path)


def test_authorization_denied_reverts_to_pending_and_excludes_downstream(tmp_path, monkeypatch):
    """拒绝期间其他在途可结算；受阻 worker 结束后才回 pending，同 Run 不重复派发。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="memory")
    p.register_task("protected", _auth_denied_handler)
    p.register_task("downstream", _noop_handler)
    p.register_task("independent", _noop_handler)

    p.enqueue(Job("protected", "p1"))
    p.enqueue(Job("downstream", "d1", depends_on=["protected::p1"]))
    p.enqueue(Job("independent", "ind1"))

    (tmp_path / "sensitive.txt").write_text("secret", encoding="utf-8")

    with in_process_workers(p), authorizer(p, _deny_sensitive_authorizer):
        p.run(install_signals=False)

    jobs = _snapshot_map(p)
    assert jobs["independent::ind1"]["status"] == "succeeded"
    assert jobs["protected::p1"]["status"] == "pending"
    assert jobs["downstream::d1"]["status"] == "pending"
    p.close()


def _overlapping_outputs_handler(job, ctx):
    root = Path(job.payload["root"])
    root.mkdir(exist_ok=True)
    (root / "report").write_text("report")
    if job.payload["dynamic"]:
        ctx.declare_file("alias", FileSpec(root / "report", role="output"))
        ctx.declare_file("directory", FileSpec(root, role="output"))
    return TaskResult.success()


@pytest.mark.parametrize("dynamic", [False, True])
def test_different_output_names_may_share_or_overlap_paths(tmp_path, dynamic):
    """不同文件名允许共用路径或父子路径，结算必须完整发布全部名称。"""
    root = tmp_path / "bundle"
    p = TaskLite(backend="memory")
    p.register_task("work", _overlapping_outputs_handler)
    files = {"report": FileSpec(root / "report", role="output")}
    if not dynamic:
        files.update(alias=FileSpec(root / "report", role="output"), directory=FileSpec(root, role="output"))
    p.enqueue(Job("work", "one", files=files, payload={"root": str(root), "dynamic": dynamic}))
    with in_process_workers(p):
        p.run(install_signals=False)
    rec = _snapshot_map(p)["work::one"]
    assert rec["status"] == "succeeded"
    assert rec["files"]["report"]["path"] == str(root / "report")
    assert rec["files"]["alias"]["path"] == str(root / "report")
    assert rec["files"]["directory"]["path"] == str(root)
    attempt, = _attempts(p, "work::one")
    assert set(attempt["files"]) == {"report", "alias", "directory"}
    p.close()


def _dynamic_delete_handler(job, ctx):
    try:
        ctx.declare_file("target", FileSpec(job.payload["path"], role="output", delete_existing=True))
    except Exception:
        return TaskResult.success()
    return TaskResult.success()


@pytest.mark.parametrize("wait,cleanup_error", [
    (False, OSError),
    (True, OSError),
    (False, ValueError),
    (True, NotImplementedError),
])
def test_approved_waiting_declaration_cleanup_error_fails_job(
    tmp_path, monkeypatch, wait, cleanup_error,
):
    """等待后获准的前删故障必须失败，不能冒充授权拒绝回 pending。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("valuable")
    p = TaskLite(backend="memory")
    decisions = iter([None, True] if wait else [True])
    p.register_task("work", _dynamic_delete_handler)
    p.enqueue(Job("work", "one", payload={"path": str(target)}))
    with authorizer(p, lambda *args: next(decisions)), cleanup_hook(p, cleanup_error("清理磁盘拒绝写入")):
        p.run(install_signals=False)
    rec = _snapshot_map(p)["work::one"]
    assert rec["status"] == "failed"
    assert "清理磁盘拒绝写入" in (rec["result"]["message"] or "")
    attempt, = _attempts(p, "work::one")
    assert attempt["outcome"] == "failure"
    assert attempt["result"]["exception"]["type"] == cleanup_error.__name__
    assert "delete_target" in attempt["result"]["exception"]["traceback"]
    assert attempt["files"]["target"]["path"] == str(target)
    assert target.read_text() == "valuable"
    p.close()


@pytest.mark.parametrize("cleanup_error", [OSError, ValueError, NotImplementedError])
def test_pre_dispatch_cleanup_failure_settles_without_starting_worker(
    tmp_path, monkeypatch, cleanup_error,
):
    """静态前删失败经统一结算登记失败，保留声明且不启动 handler。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    target = tmp_path / "target"
    target.write_text("valuable", encoding="utf-8")
    marker = tmp_path / "started"
    p = TaskLite(backend="memory")
    p.register_task("work", _marker_handler)
    p.enqueue(Job(
        "work",
        "one",
        payload={"marker": str(marker)},
        files={"target": FileSpec(target, role="output", delete_existing=True)},
    ))

    with in_process_workers(p), cleanup_hook(p, cleanup_error("静态前删故障")):
        p.run(install_signals=False)

    assert not marker.exists()
    assert target.read_text(encoding="utf-8") == "valuable"
    rec = _snapshot_map(p)["work::one"]
    assert rec["status"] == "failed"
    assert "Pre-deletion failed" in (rec["result"]["message"] or "")
    assert rec["result"]["exception"]["type"] == cleanup_error.__name__
    attempt, = _attempts(p, "work::one")
    assert attempt["outcome"] == "failure"
    assert attempt["attempt_no"] == 1
    assert attempt["files"]["target"]["path"] == str(target)
    p.close()


def test_pre_dispatch_removes_broken_symlink(tmp_path, monkeypatch):
    """断链链接按存在的目录项前删，不因其目标缺失被跳过。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    link = tmp_path / "broken-link"
    link.symlink_to(tmp_path / "missing-target")
    marker = tmp_path / "started"
    p = TaskLite(backend="memory")
    p.register_task("work", _marker_handler)
    p.enqueue(Job(
        "work",
        "one",
        payload={"marker": str(marker)},
        files={"target": FileSpec(link, role="cache", delete_existing=True)},
    ))
    with in_process_workers(p):
        p.run(install_signals=False)

    assert marker.exists()
    assert not os.path.lexists(link)
    assert _snapshot_map(p)["work::one"]["status"] == "succeeded"
    p.close()


def test_mark_running_failure_keeps_file_and_worker_unstarted(tmp_path, monkeypatch):
    """running 写失败保留 pending 队列与目标文件，不进入前删或 spawn。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    target = tmp_path / "target"
    target.write_text("valuable", encoding="utf-8")
    marker = tmp_path / "started"
    p = TaskLite(backend="memory")
    p.register_task("work", _marker_handler)
    p.enqueue(Job(
        "work",
        "one",
        payload={"marker": str(marker)},
        files={"target": FileSpec(target, role="output", delete_existing=True)},
    ))

    original = OSError("write failed")
    original_traceback = None

    def fail_running_write(*args, **kwargs):
        nonlocal original_traceback
        try:
            raise original
        except OSError as error:
            original_traceback = error.__traceback__
            raise

    with in_process_workers(p), patch.object(
        p._runtime.store.backend, "mark_job_running", side_effect=fail_running_write
    ):
        with pytest.raises(OSError, match="write failed") as caught:
            p.run(install_signals=False)
    assert caught.value is original
    assert caught.value.__cause__ is None
    tracebacks = []
    current = caught.value.__traceback__
    while current is not None:
        tracebacks.append(current)
        current = current.tb_next
    assert original_traceback in tracebacks

    assert not marker.exists()
    assert target.read_text(encoding="utf-8") == "valuable"
    assert _snapshot_map(p)["work::one"]["status"] == "pending"
    assert _attempts(p, "work::one") == []
    assert p._resources["__workers__"].used == 0.0
    p.close()


def test_worker_start_failure_releases_resources_after_running_commit(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    marker = tmp_path / "started"
    p = TaskLite(backend="memory")
    capacity = CapacityResource("slot", max_capacity=1.0)
    p.register_resource(capacity)
    p.register_task("work", _marker_handler, resources={"slot": 1.0})
    p.enqueue(Job("work", "one", payload={"marker": str(marker)}))

    with in_process_workers(p), patch.object(
        p._runtime.channel, "prepare_spawn", side_effect=OSError("worker process start failed")
    ):
        with pytest.raises(OSError, match="worker process start failed"):
            p.run(install_signals=False)

    assert not marker.exists()
    assert p._resources["slot"].used == 0.0
    assert p._resources["__workers__"].used == 0.0
    assert _snapshot_map(p)["work::one"]["status"] == "running"
    assert _attempts(p, "work::one") == []
    p.close()


def _retry_cleanup_handler(job, ctx):
    path = Path(ctx.declare_file(
        "retry_file",
        FileSpec("retry.txt", role="output", delete_on_failure=True),
    ))
    path.write_text("retry data", encoding="utf-8")
    return TaskResult.retry(message="retry requested")


@pytest.mark.parametrize("cleanup_error", [OSError, ValueError, NotImplementedError])
def test_retry_cleanup_failure_settles_once_without_retry(tmp_path, monkeypatch, cleanup_error):
    """重试清理失败直接终结，单条 Attempt 同时保留重试原因与清理诊断。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    p = TaskLite(backend="memory")
    p.register_task("retry", _retry_cleanup_handler)
    p.enqueue(Job("retry", "one", max_retries_per_run=3))

    with in_process_workers(p), cleanup_hook(p, cleanup_error("重试清理故障")):
        p.run(install_signals=False)

    rec = _snapshot_map(p)["retry::one"]
    assert rec["status"] == "failed"
    assert "Retry cleanup failed" in (rec["result"]["message"] or "")
    assert rec["result"]["annotations"]["cleanup_original_message"] == "retry requested"
    assert rec["result"]["annotations"]["cleanup_exception"]["type"] == cleanup_error.__name__
    attempt, = _attempts(p, "retry::one")
    assert attempt["outcome"] == "failure"
    assert attempt["files"]["retry_file"]["path"] == str(tmp_path / "retry.txt")
    assert (tmp_path / "retry.txt").read_text(encoding="utf-8") == "retry data"
    p.close()


def test_pre_dispatch_authorization_waits_without_holding_resources(tmp_path, monkeypatch):
    """派发前待决授权留队、不占资源，等待期间仍结算已有执行。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("valuable")
    p = TaskLite(backend="memory", max_workers=2)
    p.register_resource(CapacityResource("disk", max_capacity=1))
    resource = p._resources["disk"]
    p.register_task("independent", _noop_handler)
    p.register_task("protected", _noop_handler, resources={"disk": 1})
    calls = []
    scheduler = p._runtime.scheduler
    scan = scheduler.scan_next_runnable

    def forbidden_scan(*args, **kwargs):
        raise AssertionError("待决授权期间禁止重新选择其他派发候选")

    def authorize(*args):
        calls.append(args)
        assert resource.used == 0
        assert _attempts(p, "protected::one") == []
        jobs = _snapshot_map(p)
        assert jobs["protected::one"]["status"] == "pending"
        if len(calls) == 1 or jobs["independent::one"]["status"] != "succeeded":
            assert jobs["independent::later"]["status"] != "succeeded"
            monkeypatch.setattr(scheduler, "scan_next_runnable", forbidden_scan)
            return None
        monkeypatch.setattr(scheduler, "scan_next_runnable", scan)
        return True

    p.enqueue(Job("independent", "one"))
    p.enqueue(Job("protected", "one", files={"target": FileSpec(target, role="cache", delete_existing=True)}))
    p.enqueue(Job("independent", "later"))
    with in_process_workers(p), authorizer(p, authorize):
        p.run(install_signals=False)
    assert len(calls) >= 2
    jobs = _snapshot_map(p)
    assert all(jobs[uid]["status"] == "succeeded" for uid in ("independent::one", "protected::one", "independent::later"))
    attempt, = _attempts(p, "protected::one")
    assert attempt["outcome"] == "success" and attempt["attempt_no"] == 1
    assert not target.exists()
    p.close()


def _legacy_delete_handler(job, ctx):
    getattr(ctx, job.payload["method"])(job.payload["path"], delete_existing=True)
    return TaskResult.success()


@pytest.mark.parametrize("method", ["declare_output", "declare_cache"])
def test_removed_file_declaration_cannot_delete_without_authorization(tmp_path, monkeypatch, method):
    """被移除的声明入口不得绕过主进程授权删除既有文件。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "valuable"
    target.write_text("valuable")
    p = TaskLite(backend="memory")
    calls = []
    p.register_task("work", _legacy_delete_handler)
    p.enqueue(Job("work", "one", payload={"path": str(target), "method": method}))
    with in_process_workers(p), authorizer(p, lambda *args: calls.append(args) or False):
        p.run(install_signals=False)
    assert target.read_text() == "valuable"
    assert calls == []
    assert _snapshot_map(p)["work::one"]["result"]["exception"]["type"] == "AttributeError"
    p.close()


def _legacy_output_message_handler(job, ctx):
    from tasklite.utils.jsonutil import dumps

    ctx._pipe_sender._conn.send_bytes(dumps({
        "job_uid": job.uid, "execution_id": ctx._pipe_sender.execution_id,
        "run_id": ctx._pipe_sender.run_id,
        "attempt_no": ctx._pipe_sender.attempt_no,
        "kind": "output", "payload": {
            "path": job.payload["path"], "delete_on_success": True,
            "delete_on_failure": True,
        },
    }).encode())
    return TaskResult.success()


def test_removed_output_message_cannot_authorize_cleanup(tmp_path, monkeypatch):
    """单向产物帧不能作为绕过请求回复授权的删除入口。"""
    monkeypatch.delenv("TASKLITE_DANGEROUS_AUTO_APPROVE", raising=False)
    target = tmp_path / "valuable"
    target.write_text("valuable")
    with TaskLite(backend="memory") as p:
        p.register_task("work", _legacy_output_message_handler)
        p.enqueue(Job("work", "one", payload={"path": str(target)}))
        with in_process_workers(p), authorizer(p, lambda *args: False):
            with pytest.raises(ValueError, match="unknown worker pipe message kind"):
                p.run(install_signals=False)
        assert target.read_text() == "valuable"
        assert _snapshot_map(p)["work::one"]["status"] == "running"


def _rename_output_handler(job, ctx):
    mode = job.payload["declaration_mode"]
    delete_on_success = job.payload["delete_on_success"]
    temp_path = Path(job.payload["temp_path"])
    final_path = Path(job.payload["final_path"])

    if mode == "dynamic":
        declared = Path(ctx.declare_file(
            "result_file",
            FileSpec(temp_path, role="output", delete_on_success=delete_on_success),
        ))
        assert declared == temp_path
    else:
        declared = ctx.file_path("result_file")
        assert declared == temp_path

    declared.write_text("generated content", encoding="utf-8")
    declared.rename(final_path)
    return TaskResult.success()


@pytest.mark.parametrize("mode", ["static", "dynamic"])
def test_output_delete_on_success_allows_worker_rename(tmp_path, monkeypatch, mode):
    """成功删除的输出允许工作进程在返回前原子 rename 移走，缺失视为已移除并发布原声明。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    temp_file = tmp_path / "temp_output.txt"
    final_file = tmp_path / "final_output.txt"

    job_kwargs = {}
    if mode == "static":
        job_kwargs["files"] = {
            "result_file": FileSpec(temp_file, role="output", delete_on_success=True),
        }

    with TaskLite(backend="memory") as pipeline:
        pipeline.register_task("worker_task", _rename_output_handler)
        pipeline.enqueue(Job(
            "worker_task",
            "j1",
            payload={
                "declaration_mode": mode,
                "delete_on_success": True,
                "temp_path": str(temp_file),
                "final_path": str(final_file),
            },
            **job_kwargs,
        ))
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)

        assert final_file.exists()
        assert final_file.read_text(encoding="utf-8") == "generated content"
        assert not temp_file.exists()

        rec = _snapshot_map(pipeline)["worker_task::j1"]
        assert rec["status"] == "succeeded"
        assert rec["files"]["result_file"]["path"] == str(temp_file)

        attempt, = _attempts(pipeline, "worker_task::j1")
        assert attempt["outcome"] == "success"
        assert attempt["attempt_no"] == 1


@pytest.mark.parametrize("mode", ["static", "dynamic"])
def test_output_without_delete_on_success_missing_fails(tmp_path, monkeypatch, mode):
    """非成功删除的输出若在结算前缺失或被移走，必须判为失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_DANGEROUS_AUTO_APPROVE", "1")
    temp_file = tmp_path / "temp_output.txt"
    final_file = tmp_path / "final_output.txt"

    job_kwargs = {}
    if mode == "static":
        job_kwargs["files"] = {
            "result_file": FileSpec(temp_file, role="output", delete_on_success=False),
        }

    with TaskLite(backend="memory") as pipeline:
        pipeline.register_task("worker_task", _rename_output_handler)
        pipeline.enqueue(Job(
            "worker_task",
            "j1",
            payload={
                "declaration_mode": mode,
                "delete_on_success": False,
                "temp_path": str(temp_file),
                "final_path": str(final_file),
            },
            **job_kwargs,
        ))
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)

        assert final_file.exists()
        assert not temp_file.exists()

        rec = _snapshot_map(pipeline)["worker_task::j1"]
        assert rec["status"] == "failed"
        assert f"Missing output {temp_file}" in (rec["result"]["message"] or "")
        attempt, = _attempts(pipeline, "worker_task::j1")
        assert attempt["outcome"] == "failure"
