"""FileSpec 主进程执行与通信、授权与清理生命周期必须验收用例集。

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
    TaskLite,
    TaskLiteError,
    TaskResult,
)


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


def test_from_output_reference_and_resolution(tmp_path, monkeypatch):
    """from_output 保留配置引用且 Attempt 记录解析出规范绝对路径。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
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
    p.run()

    # 1. successes 记录
    successes = p.backend.load_successes()
    assert producer_job.uid in successes
    assert consumer_job.uid in successes
    assert successes[consumer_job.uid]["content"] == "hello world"

    # 2. 消费者 job 快照保留引用本身，Attempt 记录解析绝对路径
    attempts = p.backend.load_settled_attempts(consumer_job.uid)
    assert len(attempts) == 1
    attempt = attempts[0]
    expected_path = str((tmp_path / "report.txt").resolve())

    # job 配置快照保留 from_output 引用
    assert attempt["job"]["files"]["source"]["producer_uid"] == producer_job.uid
    assert attempt["job"]["files"]["source"]["output_name"] == "report"

    # 外层 files 记录已解析规范绝对路径
    assert attempt["files"]["source"]["path"] == expected_path
    assert attempt["files"]["source"]["role"] == "input"


def test_from_output_unpublished_name_fails_immediately(tmp_path, monkeypatch):
    """from_output 引用生产者未发布产物名称直接失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
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
    p.run()

    failed = p.backend.load_failed()
    assert consumer_job.uid in failed
    err_msg = failed[consumer_job.uid].get("error_message", "")
    assert "did not publish output" in err_msg


def test_from_output_missing_disk_input_fails_immediately(tmp_path, monkeypatch):
    """上游虽已发布但磁盘 input 产物缺失直接失败。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
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
    p.run()

    # 模拟外部将产物删除
    report_file = tmp_path / "report.txt"
    if report_file.exists():
        report_file.unlink()

    p.enqueue(consumer_job)
    p.run()

    failed = p.backend.load_failed()
    assert consumer_job.uid in failed
    err_msg = failed[consumer_job.uid].get("error_message", "")
    assert "Missing input file" in err_msg


def _idempotent_handler(job, ctx):
    spec = FileSpec("idempotent.txt", role="output", delete_existing=True)
    p1 = ctx.declare_file("same", spec)
    p1.write_text("initial", encoding="utf-8")

    # 同名同配置再次声明：幂等且不重复前置删除
    p2 = ctx.declare_file("same", spec)
    assert p1 == p2
    assert p2.read_text(encoding="utf-8") == "initial"

    # 同名异配置声明：必须报冲突异常
    bad_spec = FileSpec("idempotent.txt", role="cache")
    try:
        ctx.declare_file("same", bad_spec)
        raise AssertionError("Expected conflict")
    except TaskLiteError:
        pass

    return TaskResult.success()


def test_duplicate_declaration_idempotent_and_conflict(tmp_path, monkeypatch):
    """同声明重复不重复删除，缺失删除目标视为完成；同名异声明报冲突。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
    p.register_task("idem", _idempotent_handler)
    p.enqueue(Job("idem", "j1"))
    p.run()

    assert "idem::j1" in p.backend.load_successes()


def _clean_input_handler(job, ctx):
    ctx.declare_file(
        "temp_in",
        FileSpec("to_clean.txt", role="input", delete_on_success=True),
    )
    return TaskResult.success()


def test_input_role_cleanup_policies(tmp_path, monkeypatch):
    """input 禁前删，但成功/失败删除按显式策略允许。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    # 1. input 声明 delete_existing 构造期/校验即拒绝
    with pytest.raises(ValueError, match="input FileSpec cannot set delete_existing=True"):
        FileSpec("input.txt", role="input", delete_existing=True)

    # 2. input 支持 delete_on_success
    in_file = tmp_path / "to_clean.txt"
    in_file.write_text("temporary input", encoding="utf-8")

    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
    p.register_task("clean_task", _clean_input_handler)
    p.enqueue(Job("clean_task", "c1"))
    p.run()

    assert "clean_task::c1" in p.backend.load_successes()
    assert not in_file.exists()


def _fail_cleanup_handler(job, ctx):
    ctx.declare_file(
        "del_file",
        FileSpec("undeletable.txt", role="output", delete_on_success=True),
    )
    ctx.spawn(Job("spawned", "s1"))
    ctx.set_cursor("cur_key", "cur_val")
    return TaskResult.success()


def test_success_cleanup_failure_blocks_success_spawn_and_cursor(tmp_path, monkeypatch):
    """成功清理失败不发布成功/派生/游标，配置与实际文件记录分开。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
    p.register_task("fail_clean", _fail_cleanup_handler)
    p.register_task("spawned", _noop_handler)
    p.enqueue(Job("fail_clean", "fc1"))

    # 构造清理失败：通过 mock policy.cleanup 抛 OSError
    with patch("tasklite.utils.artifact.ArtifactPathPolicy.cleanup", side_effect=OSError("Disk write protected")):
        target = tmp_path / "undeletable.txt"
        target.write_text("data", encoding="utf-8")
        p.run()

    # 必须结算为 failed
    failed = p.backend.load_failed()
    assert "fail_clean::fc1" in failed
    err_msg = failed["fail_clean::fc1"].get("error_message", "")
    assert "Success cleanup failed" in err_msg

    # 不发布成功、不入队派生、不写入游标
    assert "fail_clean::fc1" not in p.backend.load_successes()
    assert "spawned::s1" not in p.backend.load_successes()
    assert "spawned::s1" not in p.backend.load_failed()
    cursors = p.backend.load_cursors()
    assert "cur_key" not in cursors


def _failing_handler(job, ctx):
    ctx.declare_file(
        "fail_spec",
        FileSpec("dummy.txt", role="output", delete_on_failure=True),
    )
    (Path(ctx.base_dir) / "dummy.txt").write_text("fail data", encoding="utf-8")
    raise RuntimeError("Business computation failed")


def test_failure_cleanup_failure_preserves_original_cause(tmp_path, monkeypatch):
    """handler 已失败且失败清理再次失败：保留原始失败原因，附加清理错误。"""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("TASKLITE_AUTO_APPROVE", "1")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")
    p.register_task("failing", _failing_handler)
    p.enqueue(Job("failing", "f1"))

    with patch("tasklite.utils.artifact.ArtifactPathPolicy.cleanup", side_effect=OSError("I/O error during cleanup")):
        p.run()

    failed = p.backend.load_failed()
    assert "failing::f1" in failed
    meta = failed["failing::f1"]
    error_msg = meta.get("error_message", "")
    assert "Business computation failed" in error_msg
    assert "cleanup failed" in error_msg


def _auth_denied_handler(job, ctx):
    ctx.declare_file(
        "protected_out",
        FileSpec("sensitive.txt", role="output", delete_existing=True),
    )
    return TaskResult.success()


def _deny_sensitive_authorizer(path, target_type):
    if "sensitive.txt" in str(path):
        return False
    return True


def test_authorization_denied_reverts_to_pending_and_excludes_downstream(tmp_path, monkeypatch):
    """拒绝/stop期间其他在途可结算；受阻worker结束后才回pending，同Run不重复派发。"""
    monkeypatch.chdir(tmp_path)
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db")

    p.authorizer = _deny_sensitive_authorizer
    p.register_task("protected", _auth_denied_handler)
    p.register_task("downstream", _noop_handler)
    p.register_task("independent", _noop_handler)

    p.enqueue(Job("protected", "p1"))
    p.enqueue(Job("downstream", "d1", depends_on=["protected::p1"]))
    p.enqueue(Job("independent", "ind1"))

    (tmp_path / "sensitive.txt").write_text("secret", encoding="utf-8")

    # 执行当前 Run
    p.run()

    # 1. 独立作业正常成功
    assert "independent::ind1" in p.backend.load_successes()

    # 2. 受阻作业不落 failed 档案，回到 pending 队列
    failed = p.backend.load_failed()
    assert "protected::p1" not in failed
    assert "downstream::d1" not in failed

    # 验证 queue 中 protected::p1 仍为 pending
    queue_jobs = p.backend.load_queue_jobs()
    pending_uids = [j["job_uid"] for j in queue_jobs if j.get("status") == "pending"]
    assert "protected::p1" in pending_uids

    # 下游 d1 在同 Run 内被排除，没有被派发
    assert "downstream::d1" not in p.backend.load_successes()


def _overlapping_outputs_handler(job, ctx):
    root = Path(job.payload["root"])
    root.mkdir(exist_ok=True)
    (root / "report").write_text("report")
    if job.payload["dynamic"]:
        ctx.declare_file("alias", FileSpec(root / "report", role="output"))
        ctx.declare_file("directory", FileSpec(root, role="output"))
    return TaskResult.success()


@pytest.mark.parametrize("dynamic", [False, True])
@pytest.mark.parametrize("backend", ["sqlite", "memory"])
def test_different_output_names_may_share_or_overlap_paths(tmp_path, dynamic, backend):
    """不同文件名允许共用路径或父子路径，结算必须完整发布全部名称。"""
    root = tmp_path / "bundle"
    p = TaskLite(backend=backend, **({"state_path": tmp_path / "state.db"} if backend == "sqlite" else {}))
    p.register_task("work", _overlapping_outputs_handler)
    files = {"report": FileSpec(root / "report", role="output")}
    if not dynamic:
        files.update(alias=FileSpec(root / "report", role="output"), directory=FileSpec(root, role="output"))
    p.enqueue(Job("work", "one", files=files, payload={"root": str(root), "dynamic": dynamic}))
    p.run()
    assert p.backend.load_successes()["work::one"]["artifacts"] == {
        "report": {"path": str(root / "report")},
        "alias": {"path": str(root / "report")},
        "directory": {"path": str(root)},
    }
    attempt, = p.backend.load_settled_attempts("work::one")
    assert set(attempt["files"]) == {"report", "alias", "directory"}
    p.close()


def _dynamic_delete_handler(job, ctx):
    try:
        ctx.declare_file("target", FileSpec(job.payload["path"], role="output", delete_existing=True))
    except Exception:
        return TaskResult.success()
    return TaskResult.success()


@pytest.mark.parametrize("backend", ["sqlite", "memory"])
@pytest.mark.parametrize("wait", [False, True])
def test_approved_waiting_declaration_cleanup_error_fails_job(tmp_path, monkeypatch, backend, wait):
    """等待后获准的前删故障必须失败，不能冒充授权拒绝回 pending。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("valuable")
    p = TaskLite(backend=backend, **({"state_path": tmp_path / "state.db"} if backend == "sqlite" else {}))
    decisions = iter([None, True] if wait else [True])
    p.authorizer = lambda *args: next(decisions)
    p.register_task("work", _dynamic_delete_handler)
    p.enqueue(Job("work", "one", payload={"path": str(target)}))
    with patch("tasklite.utils.artifact.ArtifactPathPolicy.remove_existing", side_effect=OSError("清理磁盘拒绝写入")):
        p.run()
    failure = p.backend.load_failed()["work::one"]
    assert "清理磁盘拒绝写入" in failure["error_message"]
    attempt, = p.backend.load_settled_attempts("work::one")
    assert attempt["outcome"] == "failure"
    assert attempt["result"]["exception"]["type"] == "OSError"
    assert "remove_existing" in attempt["result"]["exception"]["traceback"]
    assert "清理磁盘拒绝写入" in attempt["error_message"]
    assert p.backend.load_queue_jobs() == []
    assert target.read_text() == "valuable"
    p.close()


def test_pre_dispatch_authorization_waits_without_holding_resources(tmp_path, monkeypatch):
    """派发前待决授权留队、不占资源，等待期间仍结算已有执行。"""
    from tasklite.resource import CapacityResource

    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "target"
    target.write_text("valuable")
    p = TaskLite(backend="sqlite", state_path=tmp_path / "state.db", max_workers=2)
    resource = CapacityResource("disk", max_capacity=1)
    p.register_resource(resource)
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
        assert p.backend.load_settled_attempts("protected::one") == []
        row = next(row for row in p.backend.load_queue_jobs() if row["job_uid"] == "protected::one")
        assert row["status"] == "pending"
        if len(calls) == 1 or "independent::one" not in p.backend.load_successes():
            assert "independent::later" not in p.backend.load_successes()
            monkeypatch.setattr(scheduler, "scan_next_runnable", forbidden_scan)
            return None
        monkeypatch.setattr(scheduler, "scan_next_runnable", scan)
        return True

    p.authorizer = authorize
    p.enqueue(Job("independent", "one"))
    p.enqueue(Job("protected", "one", files={"target": FileSpec(target, role="output", delete_existing=True)}))
    p.enqueue(Job("independent", "later"))
    p.run()
    assert len(calls) >= 2
    assert set(p.backend.load_successes()) == {"independent::one", "protected::one", "independent::later"}
    attempt, = p.backend.load_settled_attempts("protected::one")
    assert attempt["outcome"] == "success" and attempt["attempt_no"] == 1
    assert not target.exists()
    p.close()


def _legacy_delete_handler(job, ctx):
    getattr(ctx, job.payload["method"])(job.payload["path"], delete_existing=True)
    return TaskResult.success()


@pytest.mark.parametrize("method", ["declare_output", "declare_cache"])
@pytest.mark.parametrize("backend", ["sqlite", "memory"])
def test_removed_file_declaration_cannot_delete_without_authorization(tmp_path, monkeypatch, method, backend):
    """被移除的声明入口不得绕过主进程授权删除既有文件。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "valuable"
    target.write_text("valuable")
    p = TaskLite(backend=backend, **({"state_path": tmp_path / "state.db"} if backend == "sqlite" else {}))
    calls = []
    p.authorizer = lambda *args: calls.append(args) or False
    p.register_task("work", _legacy_delete_handler)
    p.enqueue(Job("work", "one", payload={"path": str(target), "method": method}))
    p.run()
    assert target.read_text() == "valuable"
    assert calls == []
    assert "AttributeError" == p.backend.load_failed()["work::one"]["exception"]["type"]
    p.close()


def _legacy_output_message_handler(job, ctx):
    from tasklite.utils.jsonutil import dumps

    ctx._pipe_sender._conn.send_bytes(dumps({
        "job_uid": job.uid, "execution_id": ctx._pipe_sender.execution_id,
        "kind": "output", "payload": {
            "path": job.payload["path"], "delete_on_success": True,
            "delete_on_failure": True,
        },
    }).encode())
    return TaskResult.success()


def test_removed_output_message_cannot_authorize_cleanup(tmp_path, monkeypatch):
    """单向产物帧不能作为绕过请求回复授权的删除入口。"""
    monkeypatch.delenv("TASKLITE_AUTO_APPROVE", raising=False)
    target = tmp_path / "valuable"
    target.write_text("valuable")
    with TaskLite(backend="sqlite", state_path=tmp_path / "state.db") as p:
        p.authorizer = lambda *args: False
        p.register_task("work", _legacy_output_message_handler)
        p.enqueue(Job("work", "one", payload={"path": str(target)}))
        with pytest.raises(ValueError, match="unknown worker pipe message kind"):
            p.run()
        assert target.read_text() == "valuable"
        assert p.backend.load_successes() == {}
