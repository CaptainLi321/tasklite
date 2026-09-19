"""结果文件认证令牌回归测试。

威胁模型：具备 ipc_dir 写权限的本地攻击者可伪造结果文件（status=success +
raw_result / new_jobs / cursor_updates），经崩溃恢复认领或收割解码注入
wall 投毒、子任务注入与游标投毒。防御：每 run 随机令牌经 WorkerLaunchSpec
下发 worker、随结果落盘，主进程读取侧强校验；不匹配按无结果处理
（瞬态、零预算），伪造文件绝不进入解码管线。
"""

import json

from tasklite.testing import fake_ctx
from tasklite.engine.channel import (
    ExecutionChannel,
    JobHandle,
    WorkerLaunchSpec,
    _mp_worker_wrapper,
)
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.utils.injective import safe_uid_filename

from tests.helpers import (
    make_fake_process_class,
    make_pipeline,
    ok_handler,
    patch_multiprocessing_for_fakes,
)
_TOKEN = "b" * 64
_INC = "a" * 32 + ".1"


def _write_result_file(channel, job, payload, incarnation=_INC):
    p = channel.journal.result_path(job.uid, incarnation)
    p.write_text(json.dumps(payload), encoding="utf-8")
    return p


class _DeadProcess:
    """已退出且带正退出码的 stub 进程（触发无结果瞬态路径）。"""

    exitcode = 1

    def is_alive(self):
        return False

    def kill(self):
        pass

    def join(self, timeout=None):
        pass


class TestResultAuthTokenWorkerSide:
    def test_worker_writes_auth_token_into_result(self, tmp_path):
        """worker 侧把 spec 携带的令牌写入结果文件（读取侧校验的前提）。"""
        job = Job("t", "x")
        _mp_worker_wrapper(WorkerLaunchSpec(
            handler=lambda j, c: (True, {"ok": 1}), job=job,
            task_ctx=fake_ctx(job),
            incarnation=_INC, ipc_dir=str(tmp_path), timeout=60.0,
            result_token=_TOKEN,
        ))
        p = tmp_path / f"{safe_uid_filename(job.uid)}.{_INC}.result.json"
        data = json.loads(p.read_text(encoding="utf-8"))
        assert data["auth"] == _TOKEN
        assert data["status"] == "success"

    def test_worker_error_payloads_carry_auth_token(self, tmp_path):
        """retry/fatal/error 各状态通道的 payload 同样携带令牌（降级不豁免）。"""
        for handler, want_status in (
            (lambda j, c: (_ for _ in ()).throw(TimeoutError("t")), "retry"),
            (lambda j, c: (_ for _ in ()).throw(SystemExit(2)), "error"),
        ):
            job = Job("t", f"y{want_status}")
            _mp_worker_wrapper(WorkerLaunchSpec(
                handler=handler, job=job,
                task_ctx=fake_ctx(job),
                incarnation=_INC, ipc_dir=str(tmp_path), timeout=60.0,
                result_token=_TOKEN,
            ))
            p = tmp_path / f"{safe_uid_filename(job.uid)}.{_INC}.result.json"
            data = json.loads(p.read_text(encoding="utf-8"))
            assert data["status"] == want_status
            assert data["auth"] == _TOKEN

    def test_degraded_write_inherits_auth_token(self, tmp_path, monkeypatch):
        """两级降级路径必须继承令牌：lock_conflict 保真语义不被认证拒绝破坏。"""
        from tasklite.utils.ipc import ArtifactJournal

        job = Job("t", "degrade")
        j = ArtifactJournal(tmp_path)
        real_write = j.write_result_atomic
        attempts = {"n": 0}

        def fail_full_then_degrade(*a, **kw):
            # 仅前两次（完整写 + 重试）失败，降级写放行——模拟真实两级降级
            attempts["n"] += 1
            if attempts["n"] <= 2:
                raise OSError("disk on fire")
            return real_write(*a, **kw)

        monkeypatch.setattr(j, "write_result_atomic", fail_full_then_degrade)
        j.write_result_with_degradation(
            job.uid, {"status": "retry", "transient_kind": "lock_conflict", "auth": _TOKEN}
        )
        p = tmp_path / f"{safe_uid_filename(job.uid)}.result.json"
        data = json.loads(p.read_text(encoding="utf-8"))
        assert data["auth"] == _TOKEN, "降级结果丢失令牌会被读取侧误拒"
        assert data["transient_kind"] == "lock_conflict"


class TestResultAuthTokenReaderSide:
    def _poison_payload(self):
        return {
            "status": "success",
            "raw_result": {"poisoned": True},
            "new_jobs": [{"task_type": "evil", "job_id": "injected", "payload": {}}],
            "cursor_updates": {"cursor": "poisoned"},
        }

    def test_claim_rejects_forged_result_without_token(self, tmp_path):
        """伪造残留（无令牌）不被认领：wall/cursor/new_jobs 注入链不可达。"""
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        _write_result_file(channel, job, self._poison_payload())

        result = channel.claim_stale_result(job.uid, job)

        assert result is None, "无令牌的伪造结果必须按无结果丢弃"

    def test_claim_rejects_result_with_wrong_token(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        payload = self._poison_payload()
        payload["auth"] = "f" * 64
        _write_result_file(channel, job, payload)

        assert channel.claim_stale_result(job.uid, job) is None

    def test_claim_accepts_result_with_matching_token(self, tmp_path):
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        payload = {"status": "success", "auth": _TOKEN, "raw_result": {"ok": 1}}
        _write_result_file(channel, job, payload)

        result = channel.claim_stale_result(job.uid, job)

        assert result is not None and result.success
        assert result.result_meta == {"ok": 1}

    def test_reap_treats_forged_result_as_transient_no_result(self, tmp_path):
        """收割路径令牌不匹配按无结果处理：NO_IPC_RESULT 瞬态重试，不投毒。"""
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        _write_result_file(channel, job, self._poison_payload())
        handle = JobHandle(
            uid=job.uid, process=_DeadProcess(), deadline=1e18, timeout=10.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INC,
        )

        completed = channel.reap_completed([handle])

        assert len(completed) == 1
        r = completed[0][1]
        assert r.success is False, "伪造结果绝不能驱动成功提交"
        assert r.retry_requested is True, "令牌不匹配走无结果瞬态（可重试）"
        assert "poisoned" not in json.dumps(r.result_meta)

    def test_abort_excludes_forged_result_from_completed(self, tmp_path):
        """中止分类路径令牌不匹配的结果不进入 completed（不提交伪造终态）。"""
        channel = ExecutionChannel(tmp_path)
        channel.result_token = _TOKEN
        job = Job("t", "x")
        _write_result_file(channel, job, self._poison_payload())
        handle = JobHandle(
            uid=job.uid, process=_DeadProcess(), deadline=1e18, timeout=10.0,
            job=job, ipc_dir=str(tmp_path), incarnation=_INC,
        )

        outcome = channel.abort_in_flight([handle])

        assert outcome.completed == [], "伪造结果不得经中止分类提交"


class TestResultAuthTokenAssembly:
    def test_run_assembly_rotates_and_propagates_token(self, tmp_path, monkeypatch):
        """生产装配契约：run 启动生成新随机令牌并同步到执行通道。"""
        p = make_pipeline(tmp_path)
        p.register_handler("h", ok_handler)
        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=make_fake_process_class("success")
        )
        p.enqueue([Job("h", "asm")])

        assert p._runtime.channel.result_token is None, "run 启动前认证未启用"
        p.run()

        token = p._runtime.channel.result_token
        assert token is not None and len(token) >= 32, "run 装配必须启用结果认证"
        assert token == p._runtime._session.result_token

        # 新 run 令牌轮换
        p2 = make_pipeline(tmp_path, name="rotate_pipeline")
        p2.register_handler("h", ok_handler)
        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=make_fake_process_class("success")
        )
        p2.enqueue([Job("h", "asm2")])
        p2.run()
        assert p2._runtime.channel.result_token != token, "令牌必须每 run 轮换"

    def test_end_to_end_result_committed_under_auth(self, tmp_path, monkeypatch):
        """端到端：令牌随 spec 下发 worker、随结果落盘、读取侧校验通过。"""
        p = make_pipeline(tmp_path)
        p.register_handler("h", ok_handler)
        patch_multiprocessing_for_fakes(
            monkeypatch, fake_process_class=make_fake_process_class("success")
        )
        p.enqueue([Job("h", "e2e")])
        p.run()

        assert "h::e2e" in p.backend.load_wall(), (
            "令牌链路（spec 下发→落盘→读取校验）任一环断裂都会把正常任务误判为无结果"
        )
