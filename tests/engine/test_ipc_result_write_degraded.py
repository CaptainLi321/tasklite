"""worker 结果落盘降级（磁盘满/瞬态 IO 故障）回归测试。

背景：结果落盘若在 worker 各分支裸调 write_result_atomic，磁盘满/瞬态
IO 故障时 OSError 直接穿透 → worker 裸崩退出（无结果文件）→ 父进程按
崩溃收割且不设 retry_requested → **成功执行的 job 被误判 DLQ、成果永久
丢失**。现全部出口统一走 _write_result_with_degradation：完整写 →
短暂停顿后重试完整写 → 降级写最小结果（success 改写 retry 让 job 重跑，
其他 status 保持原语义，lock_conflict 结构化字段保留）→ 仍失败记 error
后放弃（worker 无结果退出，drain 按崩溃语义收割）。

覆盖矩阵：
- success 完整写两连败 → 降级 retry 结果落盘（重跑而非静默丢失）
- 锁冲突分支完整写失败 → 降级结果保留 lock_conflict 结构化字段
- 全部写失败（含降级写）→ worker 不抛 OSError、无结果文件
- 端到端：降级 retry 结果被 drain 消费 → job 回队重跑成功，不进 DLQ
"""

import pytest

from tasklite.engine import channel as channel_mod
from tasklite.engine.channel import (
    _mp_worker_wrapper, read_result_file, result_path,
)
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.utils.ipc import ArtifactJournal
from tests.helpers import patch_multiprocessing_for_fakes

# fencing 执行代标识（32-hex run_id + 序号）
_INCARNATION = "deadbeefdeadbeefdeadbeefdeadbeef.1"


def _is_full_payload(result_dict):
    """区分完整 payload 与降级 payload（降级写的 error 带 DEGRADED 标记）。"""
    if "raw_result" in result_dict:
        return True
    # 锁冲突分支的完整 payload：error 以 "LOCK_CONFLICT:" 开头且带完整描述
    return str(result_dict.get("error", "")).startswith("LOCK_CONFLICT:")


def _make_ctx(job):
    return TaskContext(job, set(), set(), {}, incarnation=_INCARNATION)


class TestWorkerResultWriteDegraded:
    """worker 落盘降级链的单元级验证（直接内联执行 worker 包装）。"""

    def test_success_payload_write_failure_degrades_to_retry(self, tmp_path, monkeypatch):
        """完整结果（含 raw_result/new_jobs）写两连败 → 降级写 retry 结果。

        降级语义：status 从 "success" 改写 "retry"——成果虽未能落盘，
        但 job 走重跑（at-least-once 契约以重跑吸收副作用），而非被
        父进程按崩溃收割误判 DLQ。
        """
        real_write = ArtifactJournal.write_result_atomic
        calls = []

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            calls.append(result_dict)
            # 完整 payload 模拟磁盘满（大文件写不下）；
            # 降级 payload（小、无 raw_result）写成功
            if _is_full_payload(result_dict):
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict,
                              incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)

        job = Job("h", "a")
        # worker 不得让 OSError 穿透（裸崩 = 成功 job 被误判 DLQ）
        _mp_worker_wrapper(lambda j, c: (True, {"v": 1}), job,
                           _make_ctx(job), str(tmp_path))

        res = read_result_file(result_path(str(tmp_path), "h::a", _INCARNATION))
        assert res is not None, "降级写必须成功落盘（小 payload 可写）"
        assert res["status"] == "retry", (
            f"success 降级必须改写 retry（重跑而非丢失）: {res}"
        )
        assert "IPC_RESULT_WRITE_DEGRADED" in res["error"]
        # 完整写恰好尝试两次（初次 + 停顿后重试），之后才降级
        full_attempts = [c for c in calls if _is_full_payload(c)]
        assert len(full_attempts) == 2, (
            f"完整写应先试两次再降级: {[c.get('status') for c in calls]}"
        )
        assert len(calls) == 3, "降级写只补一次（calls[2] 为最小 retry 结果）"

    def test_lock_conflict_payload_write_failure_keeps_structured_field(
        self, tmp_path, monkeypatch,
    ):
        """锁冲突分支完整写失败 → 降级结果保留 lock_conflict 结构化字段。

        判定端读 lock_conflict 字段而非 error 前缀（业务 RetryError 消息
        可能撞 "LOCK_CONFLICT" 前缀）——降级写丢字段会把框架锁冲突误当
        业务 retry，烧 max_retries 预算。
        """
        real_write = ArtifactJournal.write_result_atomic

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            if _is_full_payload(result_dict):
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict,
                              incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)
        # 拿锁失败 = 另有执行体持锁 → 走锁冲突分支
        monkeypatch.setattr(
            "tasklite.utils.lockfile.try_acquire_lock",
            lambda *a, **k: None,
        )

        job = Job("h", "a")
        _mp_worker_wrapper(lambda j, c: (True, {}), job,
                           _make_ctx(job), str(tmp_path))

        res = read_result_file(result_path(str(tmp_path), "h::a", _INCARNATION))
        assert res is not None, "锁冲突完整写失败后必须降级写最小 retry 结果"
        assert res["status"] == "retry"
        assert res.get("lock_conflict") is True, (
            f"降级写必须保留 lock_conflict 结构化字段: {res}"
        )
        assert "IPC_RESULT_WRITE_DEGRADED" in res["error"]

    def test_all_writes_fail_worker_exits_without_crash(self, tmp_path, monkeypatch):
        """完整写 + 降级写全部失败 → worker 不抛 OSError、无结果文件。

        两级挽救均失效（磁盘满到连小 payload 都写不下）时，worker 以无
        结果退出，由 drain 按崩溃语义收割——与历史行为一致，但绝不允许
        OSError 穿透 worker 包装（裸崩会跳过 finally 之外的清理语义）。
        """

        def refusing_write(journal_self, uid, result_dict, incarnation=None):
            raise OSError(28, "No space left on device")

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", refusing_write)

        job = Job("h", "a")
        # 不得抛异常：穿透会让 worker 以未分类崩溃退出
        _mp_worker_wrapper(lambda j, c: (True, {}), job,
                           _make_ctx(job), str(tmp_path))

        assert not result_path(str(tmp_path), "h::a", _INCARNATION).exists(), (
            "全部写失败时不应存在结果文件"
        )


class TestDegradedRetryRequeuesNotDlq:
    """端到端：降级 retry 结果经 drain 消费 → job 回队重跑成功。"""

    def test_degraded_retry_requeues_job_not_dlq(self, tmp_path, monkeypatch):
        """完整写失败 → 降级 retry 结果 → drain 消费 → 回队重跑成功。

        断言 job 以 retry 语义回队（stats["retried"]==1）且重跑成功进
        降级落盘机制防止有效结果误入 DLQ：
        worker 裸崩、成功成果被误判 NO_IPC_RESULT 永久丢失。
        """
        from tasklite import TaskLite

        real_write = ArtifactJournal.write_result_atomic
        # 完整写的失败预算恰好两次：worker 的初次完整写 + 停顿后重试；
        # 随后的降级写（小 payload）与重跑后的完整写恢复正常
        full_fail_budget = [2]

        def flaky_write(journal_self, uid, result_dict, incarnation=None):
            if _is_full_payload(result_dict) and full_fail_budget[0] > 0:
                full_fail_budget[0] -= 1
                raise OSError(28, "No space left on device")
            return real_write(journal_self, uid, result_dict,
                              incarnation=incarnation)

        monkeypatch.setattr(ArtifactJournal, "write_result_atomic", flaky_write)

        # 内联执行 worker 包装：start() 在测试进程内直接运行真实
        # _mp_worker_wrapper（含降级写），drain 统一走文件轮询消费
        class InlineWorkerProcess:
            def __init__(self, target=None, args=(), kwargs=None, **_kw):
                self.target = target
                self.args = args
                self._alive = False
                self.exitcode = 0

            def start(self):
                self.target(*self.args)

            def join(self, timeout=None):
                self._alive = False

            def is_alive(self):
                return self._alive

            def kill(self):
                self._alive = False
                self.exitcode = -9

        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=InlineWorkerProcess)

        pipeline = TaskLite(
            name="degraded_ipc", state_dir=tmp_path / "state",
            backend="sqlite", max_workers=1,
        )
        pipeline.register_handler("h", lambda j, c: (True, {}))
        pipeline.enqueue([Job("h", "a")])
        pipeline.run()

        assert pipeline.stats["retried"] == 1, (
            f"降级 retry 必须被消费并计入重试: {pipeline.stats}"
        )
        assert "h::a" in pipeline.backend.load_wall(), (
            "降级 retry 后重跑必须成功进 wall（成果不得丢失）"
        )
        assert "h::a" not in pipeline.backend.load_failed(), (
            "写盘故障不得把成功执行的 job 误判进 DLQ"
        )
