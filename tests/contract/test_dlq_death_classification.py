"""DLQ error_type 分类契约测试。

锁定不变式:
1. 死亡归因串族作为 DLQ 行主错误落盘时 error_type 不落 unknown;
2. 旧落盘行(映射表演进前写入)经 list_dlq 查询期重算追溯获得正确分类
   (零数据迁移);
3. 任意业务未知错误串保持 unknown 兜底(映射行不得吞掉兜底)。
"""
import pytest

from tasklite.engine.channel import ExecutionChannel
from tasklite.engine.store import StateStore
from tasklite.models.job import Job
from tasklite.pipeline import TaskLite
from tasklite.taxonomy import (
    ERR_NO_IPC_RESULT,
    ERR_PROCESS_CRASH_PREFIX,
    ERR_PROCESS_SIGNAL_DEATH,
    ERR_TIMEOUT_PREFIX,
)


@pytest.fixture()
def pipeline(tmp_path):
    return TaskLite(name="t", state_dir=str(tmp_path), backend="sqlite")


class TestDlqErrorTypeContract:
    def test_death_strings_landed_via_apply_failure_not_unknown(self, pipeline):
        """非瞬态死亡串直落 DLQ（apply_failure 路径）→ error_type 非 unknown。"""
        store: StateStore = pipeline._runtime.store
        for error in (
            f"{ERR_TIMEOUT_PREFIX}5s)",
            f"{ERR_PROCESS_CRASH_PREFIX}-9",
            ERR_NO_IPC_RESULT,
            f"{ERR_PROCESS_SIGNAL_DEATH}: killed by SIGKILL (-9)",
        ):
            job = Job("t", error[:12].replace(" ", "_").replace("(", "_").replace(")", "_"))
            store.apply_failure(job.uid, {"error": error}, cascade=False)

        entries = {e.uid: e for e in pipeline.list_dlq()}
        assert entries
        for entry in entries.values():
            assert entry.error_type != "unknown", (
                f"死亡串族落 DLQ 不得归 unknown: {entry.meta.get('error')!r}"
            )

    def test_terminal_timeout_via_channel_failure_not_unknown(self, pipeline, tmp_path):
        """端到端:非瞬态超时终局经 ExecutionResult → DLQ,fatal 型分类。"""
        from types import SimpleNamespace
        import time as _time

        from tasklite.engine.channel import JobHandle

        channel: ExecutionChannel = pipeline._runtime.channel
        channel.ipc_dir = str(tmp_path / "ipc")
        job = Job("t", "timeout-job")
        handle = JobHandle(
            uid=job.uid, process=SimpleNamespace(exitcode=None),
            deadline=_time.monotonic() + 60, timeout=30.0,
            job=job, ipc_dir=str(tmp_path / "ipc"), incarnation="inc",
        )
        result = channel._build_terminal_failure(handle.process, handle, is_timeout=True)
        assert result.retry_requested is False

        store: StateStore = pipeline._runtime.store
        store.apply_failure(job.uid, result.result_meta, cascade=False)
        entries = list(pipeline.list_dlq())
        assert len(entries) == 1
        assert entries[0].error_type == "fatal"

    def test_legacy_rows_retroactively_classified(self, pipeline):
        """映射表演进前落盘的死亡串行经查询期重算追溯获益(零数据迁移)。"""
        legacy_job = Job("t", "legacy-old-row")
        # 模拟旧行:error_type 被旧分类写成 unknown(不经 normalize 直写)
        pipeline.backend.append_failed(
            legacy_job.uid,
            {"error": f"{ERR_PROCESS_CRASH_PREFIX}-9", "error_type": "unknown"},
        )
        rows = {e.meta.get("error"): e for e in pipeline.list_dlq()}
        legacy = rows[f"{ERR_PROCESS_CRASH_PREFIX}-9"]
        assert legacy.error_type == "transient_exhausted"

    def test_unknown_business_error_still_unknown(self, pipeline):
        """对偶兜底:任意业务未知串不因映射扩充被误分类。"""
        store: StateStore = pipeline._runtime.store
        job = Job("t", "biz")
        store.apply_failure(job.uid, {"error": "my custom handler failure"}, cascade=False)
        entries = list(pipeline.list_dlq())
        assert len(entries) == 1
        assert entries[0].error_type == "unknown"
