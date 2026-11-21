"""极简运维 CLI（``python -m tasklite``）单元测试与真实子进程端到端测试。"""
from __future__ import annotations

import os
from pathlib import Path
import sqlite3
import subprocess
import sys

import pytest

import tasklite
from tasklite import Job, StateSnapshot, TaskLite, TaskResult
from tasklite.__main__ import main
from tasklite.testing import in_process_workers


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def _succeed_handler(job: Job, ctx: object) -> TaskResult:
    return TaskResult.success(metadata={"job_id": job.job_id})


def _fail_handler(job: Job, ctx: object) -> TaskResult:
    return TaskResult.failure(message=f"failed-{job.job_id}")


def _create_empty_db(db_path: Path) -> None:
    """初始化空 Schema 6 状态库。"""
    with TaskLite(backend="sqlite", state_path=db_path):
        pass


def _create_dag_db(db_path: Path) -> tuple[str, str, str]:
    """构造含两级成功依赖链与独立失败作业的状态库。

    返回 ``(upstream_uid, downstream_uid, failed_uid)``。
    """
    upstream = Job("step_a", "up")
    downstream = Job("step_b", "down", depends_on=[upstream.uid])
    failing = Job("step_fail", "bad", max_retries_per_run=0)
    with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
        pipeline.register_task("step_a", _succeed_handler)
        pipeline.register_task("step_b", _succeed_handler)
        pipeline.register_task("step_fail", _fail_handler)
        pipeline.enqueue(upstream)
        pipeline.enqueue(downstream)
        pipeline.enqueue(failing)
        with in_process_workers(pipeline):
            pipeline.run(install_signals=False)
    return upstream.uid, downstream.uid, failing.uid


def _run_cli_subprocess(*cli_args: str) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = (
        f"{PROJECT_ROOT}{os.pathsep}{existing_pythonpath}"
        if existing_pythonpath
        else str(PROJECT_ROOT)
    )
    env["PYTHONUTF8"] = "1"
    return subprocess.run(
        [sys.executable, "-m", "tasklite", *cli_args],
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
        timeout=60,
    )


class TestCliSurfaceContract:
    """CLI 仅保留 3 个子命令、无 --json、不污染根包导出面。"""

    def test_root_all_unchanged(self) -> None:
        assert len(tasklite.__all__) == 10
        assert "main" not in tasklite.__all__

    @pytest.mark.parametrize("bad_subcmd", ["add", "replace"])
    def test_rejects_job_constructing_subcommands(self, bad_subcmd: str) -> None:
        with pytest.raises(SystemExit) as exc_info:
            main([bad_subcmd])
        assert exc_info.value.code == 2

    def test_status_rejects_json_flag(self, tmp_path: Path) -> None:
        db_path = tmp_path / "state.db"
        _create_empty_db(db_path)
        with pytest.raises(SystemExit) as exc_info:
            main(["status", str(db_path), "--json"])
        assert exc_info.value.code == 2


class TestCliStatus:
    """status 子命令：只读状态报告与错误路径防建空库。"""

    def test_status_empty_db(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        db_path = tmp_path / "empty.db"
        _create_empty_db(db_path)

        rc = main(["status", str(db_path)])
        captured = capsys.readouterr()

        assert rc == 0
        assert captured.err == ""
        assert "pending" in captured.out
        assert "succeeded" in captured.out

    def test_status_four_states(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        db_path = tmp_path / "four_states.db"
        upstream_uid, downstream_uid, failed_uid = _create_dag_db(db_path)

        # 追加一条 pending 与一条遗留 running 记录以覆盖全部四态
        pending_job = Job("step_c", "queued")
        running_job = Job("step_d", "inflight")
        with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
            pipeline.register_task("step_c", _succeed_handler)
            pipeline.register_task("step_d", _succeed_handler)
            pipeline.enqueue(pending_job)
            pipeline.enqueue(running_job)

        conn = sqlite3.connect(db_path)
        try:
            conn.execute(
                "UPDATE queue SET status = 'running', run_id = 'run-test', "
                "attempt_no = 1, started_at = '2026-01-01T00:00:00Z' "
                "WHERE job_uid = ?",
                (running_job.uid,),
            )
            conn.commit()
        finally:
            conn.close()

        rc = main(["status", str(db_path)])
        captured = capsys.readouterr()

        assert rc == 0
        assert captured.err == ""
        assert "step_a" in captured.out
        assert "step_b" in captured.out
        assert "step_fail" in captured.out
        assert "step_c" in captured.out
        assert "step_d" in captured.out
        assert failed_uid in captured.out
        assert upstream_uid != downstream_uid

    def test_status_nonexistent_path_fails_without_creating_file(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        missing = tmp_path / "missing.db"
        rc = main(["status", str(missing)])
        captured = capsys.readouterr()

        assert rc == 1
        assert captured.out == ""
        assert "does not exist" in captured.err
        assert not missing.exists()

    def test_status_directory_and_schema_mismatch_return_nonzero(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        rc_dir = main(["status", str(tmp_path)])
        err_dir = capsys.readouterr().err
        assert rc_dir == 1
        assert "directory" in err_dir.lower()

        bad_db = tmp_path / "foreign.db"
        conn = sqlite3.connect(bad_db)
        try:
            conn.execute("CREATE TABLE other (id INTEGER)")
            conn.commit()
        finally:
            conn.close()

        rc_bad = main(["status", str(bad_db)])
        err_bad = capsys.readouterr().err
        assert rc_bad == 1
        assert err_bad.strip() != ""


class TestCliSetStatus:
    """set-status 子命令：dry-run 预览、--apply 落盘、互斥参数与防建空库。"""

    def test_recursive_flag_required_and_mutually_exclusive(self, tmp_path: Path) -> None:
        db_path = tmp_path / "dag.db"
        upstream_uid, _, _ = _create_dag_db(db_path)

        with pytest.raises(SystemExit) as missing_exc:
            main(["set-status", str(db_path), upstream_uid, "pending"])
        assert missing_exc.value.code == 2

        with pytest.raises(SystemExit) as conflict_exc:
            main([
                "set-status",
                str(db_path),
                upstream_uid,
                "pending",
                "--recursive",
                "--no-recursive",
            ])
        assert conflict_exc.value.code == 2

    def test_dry_run_previews_without_modifying_db(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "dag.db"
        upstream_uid, downstream_uid, _ = _create_dag_db(db_path)

        rc = main(["set-status", str(db_path), upstream_uid, "pending", "--recursive"])
        captured = capsys.readouterr()

        assert rc == 0
        assert captured.err == ""
        assert "dry_run=True" in captured.out
        assert upstream_uid in captured.out
        assert downstream_uid in captured.out
        assert "--apply" in captured.out

        with StateSnapshot.open(db_path) as snap:
            up_job = snap.get_job(upstream_uid)
            down_job = snap.get_job(downstream_uid)
            assert up_job is not None and up_job["status"] == "succeeded"
            assert down_job is not None and down_job["status"] == "succeeded"

    def test_apply_recursive_and_no_recursive_with_note(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "dag.db"
        upstream_uid, downstream_uid, _ = _create_dag_db(db_path)

        # 仅改下游为 failed 并写入 note
        rc_single = main([
            "set-status",
            str(db_path),
            downstream_uid,
            "failed",
            "--no-recursive",
            "--note",
            "manual triage",
            "--apply",
        ])
        out_single = capsys.readouterr().out
        assert rc_single == 0
        assert "dry_run=False" in out_single

        with StateSnapshot.open(db_path) as snap:
            up_job = snap.get_job(upstream_uid)
            down_job = snap.get_job(downstream_uid)
            assert up_job is not None and up_job["status"] == "succeeded"
            assert down_job is not None and down_job["status"] == "failed"
            assert down_job["result"]["annotations"]["note"] == "manual triage"

        # 级联重置上游与下游为 pending
        rc_rec = main([
            "set-status",
            str(db_path),
            upstream_uid,
            "pending",
            "--recursive",
            "--apply",
        ])
        assert rc_rec == 0

        with StateSnapshot.open(db_path) as snap:
            up_job = snap.get_job(upstream_uid)
            down_job = snap.get_job(downstream_uid)
            assert up_job is not None and up_job["status"] == "pending"
            assert down_job is not None and down_job["status"] == "pending"

    def test_nonexistent_state_path_never_creates_empty_db(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        missing = tmp_path / "nested" / "never_create.db"
        rc = main([
            "set-status",
            str(missing),
            "step_a::up",
            "pending",
            "--no-recursive",
            "--apply",
        ])
        captured = capsys.readouterr()

        assert rc == 1
        assert "does not exist" in captured.err
        assert not missing.exists()
        assert not missing.parent.exists()

    def test_nonexistent_uid_returns_nonzero(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "dag.db"
        _create_dag_db(db_path)

        rc = main([
            "set-status",
            str(db_path),
            "step_a::missing_job",
            "pending",
            "--no-recursive",
        ])
        captured = capsys.readouterr()

        assert rc == 1
        assert "does not exist" in captured.err


class TestCliRemove:
    """remove 子命令：三表清理、指定分区、拒绝 attempts、孤儿校验阻断与预览。"""

    def test_remove_default_entries_dry_run_and_apply(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "dag.db"
        upstream_uid, downstream_uid, failed_uid = _create_dag_db(db_path)

        rc_preview = main(["remove", str(db_path), failed_uid, "--no-recursive"])
        preview_out = capsys.readouterr().out
        assert rc_preview == 0
        assert "dry_run=True" in preview_out
        assert "--apply" in preview_out

        with StateSnapshot.open(db_path) as snap:
            assert snap.get_job(failed_uid) is not None

        rc_apply = main(["remove", str(db_path), failed_uid, "--no-recursive", "--apply"])
        assert rc_apply == 0

        with StateSnapshot.open(db_path) as snap:
            assert snap.get_job(failed_uid) is None
            assert snap.get_job(upstream_uid) is not None
            assert snap.get_job(downstream_uid) is not None
            # Attempt 审计轨迹绝不删除
            assert len(list(snap.iter_attempts(job_uid=failed_uid))) == 1

    def test_remove_specific_entries_and_reject_attempts(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "entries.db"
        _, _, failed_uid = _create_dag_db(db_path)
        queued_job = Job("step_q", "only_queue")
        with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
            pipeline.register_task("step_q", _succeed_handler)
            pipeline.enqueue(queued_job)

        # 拒绝 attempts
        rc_bad = main([
            "remove",
            str(db_path),
            failed_uid,
            "--entries",
            "attempts",
            "--no-recursive",
            "--apply",
        ])
        err_bad = capsys.readouterr().err
        assert rc_bad == 1
        assert "attempts" in err_bad

        # 仅删 queue 分区
        rc_q = main([
            "remove",
            str(db_path),
            queued_job.uid,
            "--entries",
            "queue",
            "--no-recursive",
            "--apply",
        ])
        assert rc_q == 0
        with StateSnapshot.open(db_path) as snap:
            assert snap.get_job(queued_job.uid) is None
            assert snap.get_job(failed_uid) is not None

        # 指定 successes,failures 清理失败记录
        rc_sf = main([
            "remove",
            str(db_path),
            failed_uid,
            "--entries",
            "successes,failures",
            "--no-recursive",
            "--apply",
        ])
        assert rc_sf == 0
        with StateSnapshot.open(db_path) as snap:
            assert snap.get_job(failed_uid) is None

    def test_remove_upstream_blocks_orphan_prints_diff_and_does_not_commit(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        db_path = tmp_path / "orphan.db"
        upstream_uid, downstream_uid, _ = _create_dag_db(db_path)

        rc = main([
            "remove",
            str(db_path),
            upstream_uid,
            "--no-recursive",
            "--apply",
        ])
        captured = capsys.readouterr()

        assert rc == 1
        # stdout 完整打印 diff 与校验明细，stderr 打印汇总错误
        assert "[VALIDATION]" in captured.out
        assert upstream_uid in captured.out
        assert downstream_uid in captured.out
        assert "validation failed" in captured.err.lower()

        # 零落盘：上游与下游均原样保留
        with StateSnapshot.open(db_path) as snap:
            up_job = snap.get_job(upstream_uid)
            down_job = snap.get_job(downstream_uid)
            assert up_job is not None and up_job["status"] == "succeeded"
            assert down_job is not None and down_job["status"] == "succeeded"

    def test_remove_nonexistent_state_path_never_creates_empty_db(
        self,
        tmp_path: Path,
        capsys: pytest.CaptureFixture[str],
    ) -> None:
        missing = tmp_path / "absent.db"
        rc = main(["remove", str(missing), "step_a::up", "--no-recursive", "--apply"])
        captured = capsys.readouterr()

        assert rc == 1
        assert "does not exist" in captured.err
        assert not missing.exists()


class TestCliSubprocessEndToEnd:
    """真实子进程 ``python -m tasklite`` 端到端验证。"""

    def test_subprocess_status_set_status_and_remove_workflow(self, tmp_path: Path) -> None:
        db_path = tmp_path / "e2e.db"
        upstream_uid, downstream_uid, failed_uid = _create_dag_db(db_path)

        # 1. status
        res_status = _run_cli_subprocess("status", str(db_path))
        assert res_status.returncode == 0, res_status.stderr
        assert "step_a" in res_status.stdout
        assert failed_uid in res_status.stdout

        # 2. set-status dry-run -> 不落盘
        res_dry = _run_cli_subprocess(
            "set-status",
            str(db_path),
            upstream_uid,
            "pending",
            "--recursive",
        )
        assert res_dry.returncode == 0, res_dry.stderr
        assert "dry_run=True" in res_dry.stdout
        with StateSnapshot.open(db_path) as snap:
            up_job = snap.get_job(upstream_uid)
            assert up_job is not None and up_job["status"] == "succeeded"

        # 3. set-status --apply -> 原子落盘
        res_apply = _run_cli_subprocess(
            "set-status",
            str(db_path),
            upstream_uid,
            "pending",
            "--recursive",
            "--apply",
        )
        assert res_apply.returncode == 0, res_apply.stderr
        with StateSnapshot.open(db_path) as snap:
            up_job = snap.get_job(upstream_uid)
            down_job = snap.get_job(downstream_uid)
            assert up_job is not None and up_job["status"] == "pending"
            assert down_job is not None and down_job["status"] == "pending"

        # 4. remove 删上游不级联 -> 阻断落盘并打印 diff
        res_orphan = _run_cli_subprocess(
            "remove",
            str(db_path),
            upstream_uid,
            "--no-recursive",
            "--apply",
        )
        assert res_orphan.returncode == 1
        assert "[VALIDATION]" in res_orphan.stdout
        assert "validation failed" in res_orphan.stderr.lower()

        # 5. remove 级联删上游与下游 -> 成功落盘
        res_rm = _run_cli_subprocess(
            "remove",
            str(db_path),
            upstream_uid,
            "--recursive",
            "--apply",
        )
        assert res_rm.returncode == 0, res_rm.stderr
        with StateSnapshot.open(db_path) as snap:
            assert snap.get_job(upstream_uid) is None
            assert snap.get_job(downstream_uid) is None
            assert snap.get_job(failed_uid) is not None

        # 6. 不存在路径不建空库
        missing = tmp_path / "subprocess_missing.db"
        res_missing = _run_cli_subprocess("status", str(missing))
        assert res_missing.returncode == 1
        assert not missing.exists()
