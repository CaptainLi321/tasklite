"""控制流异常通道回归测试：_commit_failed_crash 永不正常返回。

``_commit_failed_crash`` 的 DLQ 分支抛 ``_JobTerminated``（异常通道唯一化），
调用方 ``except _JobTerminated`` 承接后停止处理，主循环继续下一个 job。
避免已 DLQ 的 job 被再次执行（资源泄漏 + 双重副作用）。
"""

import sqlite3
import json

import pytest

from tasklite.pipeline import TaskLite
from tasklite.models.job import Job
from tasklite.models.state import PipelineState

from tests.helpers import make_fake_process_class, make_pipeline, patch_multiprocessing_for_fakes


class TestJobTerminatedNoFallThrough:
    def test_no_handler_commit_failure_dlq_no_fallthrough(self, tmp_path, monkeypatch):
        """no-handler 分支 commit 连续失败达阈值 → job 进 DLQ，且不再 acquire/submit。

        历史 _commit_failed_crash DLQ 分支返回后，no-handler 分支继续
        fall-through 到 acquire + submit，已 DLQ 的 job 被再次执行。
        本测试：注入 _commit_failures=2 的 job + no-handler（无 handler 注册），
        且 submit 必须不被调用（否则断言失败）。
        """
        submitted = []

        class TrackingProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                submitted.append(True)
                self._alive = True

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=TrackingProcess)

        pipeline = make_pipeline(tmp_path)
        # 不注册任何 handler → no-handler 分支
        jd = Job("nohandler", "j1", payload={}).to_dict()
        jd["runtime"] = {"_commit_failures": 2}
        conn = sqlite3.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('nohandler::j1', 0, ?)",
                     (json.dumps(jd),))
        conn.commit()
        conn.close()

        # commit_job_failure 第 1 次返回 False（触发 _commit_failed_crash），
        # 第 2 次调用真实方法（DLQ 阈值分支的提交成功）——模拟「环境故障恢复」。
        call_count = [0]
        real_commit = pipeline.backend.commit_job_failure

        def fake_commit(*a, **kw):
            call_count[0] += 1
            if call_count[0] >= 2:
                return real_commit(*a, **kw)
            return False
        monkeypatch.setattr(pipeline.backend, "commit_job_failure", fake_commit)

        pipeline.run()

        # job 进 DLQ（COMMIT_FAILURE_DLQ）
        failed = pipeline.backend.load_failed()
        assert "nohandler::j1" in failed, "连续 commit 失败达阈值必须 DLQ"
        assert failed["nohandler::j1"]["error"] == "COMMIT_FAILURE_DLQ"
        assert submitted == [], \
            f"fall-through 违规: DLQ 后的 job 不应再被 submit（实际 submit 了 {len(submitted)} 次）"
        assert pipeline.stats["failed"] == 1

    def test_payload_validation_commit_failure_dlq_no_submit(self, tmp_path, monkeypatch):
        """payload-validation 分支 DLQ 阈值命中 → 已 acquire 资源被释放，且不 submit。

        同一契约的另一实例：payload-validation 失败后 commit 失败达阈值，
        DLQ 分支返回后 fall-through 到 submit（资源已 release 但 job 又被执行）。
        """
        submitted = []

        class TrackingProcess:
            def __init__(self, target=None, args=(), **kwargs):
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                submitted.append(True)
                self._alive = True

            def join(self, timeout=None): self._alive = False
            def is_alive(self): return self._alive
            def kill(self): self._alive = False

        patch_multiprocessing_for_fakes(monkeypatch, fake_process_class=TrackingProcess)

        from tasklite.taxonomy import validate_payload
        from typing import TypedDict

        class Schema(TypedDict):
            url: str

        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("h", lambda j, c: True, payload_schema=Schema)

        # payload 不含 url → 校验失败
        jd = Job("h", "j1", payload={"nope": 1}).to_dict()
        jd["runtime"] = {"_commit_failures": 2}
        conn = sqlite3.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('h::j1', 0, ?)",
                     (json.dumps(jd),))
        conn.commit()
        conn.close()

        # payload-validation 分支的 commit_job_failure 第 1 次返回 False
        # （触发 _commit_failed_crash），第 2 次调用真实方法（DLQ 提交成功）。
        call_count = [0]
        real_commit = pipeline.backend.commit_job_failure

        def fake_commit(*a, **kw):
            call_count[0] += 1
            if call_count[0] >= 2:
                return real_commit(*a, **kw)
            return False
        monkeypatch.setattr(pipeline.backend, "commit_job_failure", fake_commit)

        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "h::j1" in failed
        assert failed["h::j1"]["error"] == "COMMIT_FAILURE_DLQ"
        assert submitted == [], \
            f"fall-through 违规: payload-validation DLQ 后不应再 submit（实际 {len(submitted)} 次）"

    def test_apply_result_commit_failure_dlq_continues_loop(self, tmp_path, monkeypatch):
        """_apply_result 路径 DLQ 阈值命中 → 主循环继续处理队列中其余 job。"""
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("h", lambda j, c: (True, {}))

        jd = Job("h", "j1", payload={}).to_dict()
        jd["runtime"] = {"_commit_failures": 2}
        conn = sqlite3.connect(tmp_path / "state" / "test_pipeline_state.db")
        conn.execute("DELETE FROM queue")
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('h::j1', 0, ?)",
                     (json.dumps(jd),))
        conn.execute("INSERT INTO queue (uid, seq, job_data) VALUES ('h::j2', 1, ?)",
                     (json.dumps(Job("h", "j2", payload={}).to_dict()),))
        conn.commit()
        conn.close()

        real_commit_success = pipeline.backend.commit_job_success

        def fake_commit_success(uid, result_meta, **kw):
            # 只让 j1 的 commit 失败（进入 _commit_failed_crash → DLQ 阈值），
            # j2 走真实提交——验证主循环在 j1 DLQ 后继续处理 j2。
            if uid.startswith("h::j1"):
                return False
            return real_commit_success(uid, result_meta, **kw)
        monkeypatch.setattr(pipeline.backend, "commit_job_success", fake_commit_success)

        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=make_fake_process_class("success"),
        )

        # j1 commit 失败 3 次 → DLQ（不 crash）；j2 正常成功进 wall。
        # 若 _JobTerminated 未被正确捕获，run 会抛异常或 j2 永远不执行。
        pipeline.run()

        failed = pipeline.backend.load_failed()
        assert "h::j1" in failed, "j1 连续 commit 失败必须 DLQ"
        wall = pipeline.backend.load_wall()
        assert "h::j2" in wall, "j1 DLQ 后主循环必须继续处理 j2"


class TestControlFlowSignalsStructural:
    """实施陷阱 2.1：控制流信号必须继承 BaseException（结构性保证）。

    若继承 Exception，「承重墙」依赖 except 顺序（调用纪律）——任何
    ``except Exception`` 兜底都可能误吞信号，把已 DLQ 的 job requeue
    （防复发设计）。BaseException 让 ``except Exception`` 在类型系统层面
    捕不到它们：承重墙从调用纪律变成结构保证。
    """

    def test_signals_inherit_base_exception(self):
        """_JobTerminated / _CommitCrashSignal 必须继承 BaseException。"""
        from tasklite.exceptions import _CommitCrashSignal, _JobTerminated
        assert issubclass(_JobTerminated, BaseException), \
            "_JobTerminated 必须继承 BaseException（except Exception 捕不到）"
        assert issubclass(_CommitCrashSignal, BaseException), \
            "_CommitCrashSignal 必须继承 BaseException（except Exception 捕不到）"

    def test_signal_not_caught_by_except_exception(self):
        """except Exception 在类型系统层面捕不到控制流信号。"""
        from tasklite.exceptions import _CommitCrashSignal, _JobTerminated

        for sig_cls in (_JobTerminated, _CommitCrashSignal):
            try:
                raise sig_cls("test")
            except Exception as e:
                raise AssertionError(
                    f"{sig_cls.__name__} 被 except Exception 捕获——"
                    f"继承 Exception 而非 BaseException（结构性保证失效）: {e!r}"
                )
            except BaseException:
                pass  # 正确：只有 BaseException 能捕到


class TestFencingMetaFailLoud:
    """实施陷阱 2.4：run_id 持久化失败必须 fail-loud（启动失败）。"""

    def test_run_id_meta_failure_fails_loud(self, tmp_path, monkeypatch):
        """set_meta 失败 → run() 抛异常，而非静默降级为无 fence 运行。"""
        p = make_pipeline(tmp_path)
        p.register_handler("h", lambda job, ctx: (True, {}))

        def boom(*a, **k):
            raise RuntimeError("meta table is broken")
        monkeypatch.setattr(p.backend, "set_meta", boom)

        import pytest
        with pytest.raises(RuntimeError, match="meta table is broken"):
            p.run()


class TestCommitSkipCrashTerminalPreservation:
    """架构修复（commit_skip 终态模型）：清理型终态失败绝不污染既有终态。

    commit_skip 失败安全处理：
    第 3 次失败会把 wall 里的成功记录翻转成 DLQ（从未重跑过的任务被判死）。
    新契约：skip 命中 = uid 已有 wall/failed 终态，磁盘删除失败是环境故障，
    只能「requeue + 崩溃」交给下次 run 的加载期过滤重试；wall/failed 不变。
    """

    def _make_pipeline(self, tmp_path, monkeypatch, *, terminal_in_wall):
        from tasklite.models.state import PipelineState
        from types import SimpleNamespace
        pipeline = make_pipeline(tmp_path)
        pipeline.register_handler("h", lambda j, c: (True, {}))
        jd = Job("h", "j1", payload={}).to_dict()
        if terminal_in_wall:
            wall = {"h::j1": {"run_count": 7, "last_run_at": "2020-01-01T00:00:00+00:00"}}
            failed = {}
        else:
            wall = {}
            failed = {"h::j1": {"error": "earlier"}}
        state = PipelineState(wall, failed, {}, [jd])
        pipeline.runtime.ctx.set_state(state)
        monkeypatch.setattr(pipeline.backend, "commit_skip", lambda uid: False)
        return pipeline, SimpleNamespace(
            runnable_idx=0, pending_dep_failure=None, kind="runnable",
        )

    def test_commit_skip_failure_preserves_wall_success(self, tmp_path, monkeypatch):
        """wall 成功终态 + commit_skip 失败 → _CommitCrashSignal，wall 不翻 DLQ。"""
        import pytest
        from tasklite.exceptions import _CommitCrashSignal
        pipeline, sched = self._make_pipeline(
            tmp_path, monkeypatch, terminal_in_wall=True,
        )
        with pytest.raises(_CommitCrashSignal, match="commit_skip"):
            pipeline.runtime._dispatch.dispatch_job(sched)
        state = pipeline.runtime.state
        assert "h::j1" in state.wall, "wall 成功记录必须保留，不得被清理失败翻转"
        assert state.wall["h::j1"]["run_count"] == 7
        assert "h::j1" not in state.failed
        assert [d.get("job_id") for d in state.queue] == ["j1"], (
            "崩溃契约必须把 job 重新入队，下次 run 的加载期过滤消化残留"
        )

    def test_commit_skip_failure_preserves_failed_terminal(self, tmp_path, monkeypatch):
        """failed 终态 + commit_skip 失败 → 同样只崩溃，failed 记录原样保留。"""
        import pytest
        from tasklite.exceptions import _CommitCrashSignal
        pipeline, sched = self._make_pipeline(
            tmp_path, monkeypatch, terminal_in_wall=False,
        )
        with pytest.raises(_CommitCrashSignal):
            pipeline.runtime._dispatch.dispatch_job(sched)
        state = pipeline.runtime.state
        assert state.failed["h::j1"] == {"error": "earlier"}
        assert "h::j1" not in state.wall
        assert [d.get("job_id") for d in state.queue] == ["j1"]

    def test_commit_skip_crash_contract_breach_fails_loud(self, tmp_path, monkeypatch):
        """防御测试（D-1）：_commit_skip_crash 意外正常返回时必须 fail-loud。"""
        pipeline, sched = self._make_pipeline(
            tmp_path, monkeypatch, terminal_in_wall=False,
        )
        monkeypatch.setattr(pipeline.store, "commit_skip_crash", lambda uid, jd: None)
        import pytest
        with pytest.raises(AssertionError, match="unexpectedly returned normally"):
            pipeline.runtime._dispatch.dispatch_job(sched)


class TestCommitFailedCrashContractBreach:
    """验证 _reject_and_commit 统一出口在各种 commit 失败场景下的行为。

    三处拒绝路径（dep-failed / no-handler / payload 校验失败）都经
    _reject_and_commit 统一处理。当 commit_failed_crash 的 3-strike DLQ
    成功时（_JobTerminated），_reject_and_commit 内部吞掉异常并正常返回；
    当 commit_failed_crash 正常返回（不应发生但防御性处理），调用方也
    正常返回——不再 fail-loud 抛 AssertionError。
    """

    def _pipeline_with_state(self, tmp_path):
        from tasklite.models.state import PipelineState
        pipeline = make_pipeline(tmp_path)
        jd = Job("t", "j1", payload={}).to_dict()
        jd["runtime"] = {"_commit_failures": 0}
        state = PipelineState({}, {}, {}, [jd])
        pipeline.runtime.ctx.set_state(state)
        return pipeline, jd

    def test_dep_failed_commit_failure_handled(self, tmp_path, monkeypatch):
        """dep-failed 路径：commit 失败 + commit_failed_crash 正常返回
        → _reject_and_commit 正常返回，dispatch_dep_failed 返回 True。"""
        pipeline, jd = self._pipeline_with_state(tmp_path)
        monkeypatch.setattr(pipeline.backend, "commit_job_failure", lambda uid, meta: False)
        monkeypatch.setattr(
            pipeline.store, "commit_failed_crash",
            lambda uid, reason, job_dict: None,
        )
        # 不再抛异常，正常返回 True 表示已处理
        result = pipeline.runtime._dispatch.dispatch_dep_failed("t::j1", jd, "parent::p1")
        assert result is True

    def test_no_handler_commit_failure_handled(self, tmp_path, monkeypatch):
        """no-handler 路径：commit 失败 + commit_failed_crash 正常返回
        → 正常返回 True。"""
        pipeline, jd = self._pipeline_with_state(tmp_path)
        monkeypatch.setattr(pipeline.backend, "commit_job_failure", lambda uid, meta: False)
        monkeypatch.setattr(
            pipeline.store, "commit_failed_crash",
            lambda uid, reason, job_dict: None,
        )
        result = pipeline.runtime._dispatch.dispatch_no_handler("t::j1", jd, "t")
        assert result is True

    def test_payload_validation_commit_failure_handled(self, tmp_path, monkeypatch):
        """payload 校验失败路径：commit 失败 + commit_failed_crash 正常返回
        → dispatch_job 正常返回 None。"""
        from typing import TypedDict
        from types import SimpleNamespace

        class Schema(TypedDict):
            url: str

        pipeline, jd = self._pipeline_with_state(tmp_path)
        pipeline.register_handler("t", lambda j, c: True, payload_schema=Schema)
        jd["payload"] = {"nope": 1}
        monkeypatch.setattr(pipeline.backend, "commit_job_failure", lambda uid, meta: False)
        monkeypatch.setattr(
            pipeline.store, "commit_failed_crash",
            lambda uid, reason, job_dict: None,
        )

        sched = SimpleNamespace(runnable_idx=0, pending_dep_failure=None, kind="runnable")
        result = pipeline.runtime._dispatch.dispatch_job(sched)
        assert result is None

