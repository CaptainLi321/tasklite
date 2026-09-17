"""死亡归因决策表(attribute_process_death)的完备性表驱动测试。

锁定不变式:
1. exitcode × timed_out × timeout_is_transient 全组合的 meta 形状 / 错误串 /
   瞬态语义 / dlq_error_type;
2. 决策表产出的错误串族必须被 _classify_string 映射消费(死亡串不落 unknown);
3. 瞬态军规:环境死亡烧预算(retry_requested=True)、非瞬态超时终局。
"""
import pytest

from tasklite.taxonomy import (
    ERR_NO_IPC_RESULT,
    ERR_PROCESS_CRASH_PREFIX,
    ERR_PROCESS_SIGNAL_DEATH,
    ERR_TIMEOUT_PREFIX,
    DeathAttribution,
    ErrorCategory,
    ErrorTaxonomy,
    ERROR_TYPE_FATAL,
    ERROR_TYPE_TRANSIENT_EXHAUSTED,
    classify_error_type,
)


@pytest.fixture()
def taxonomy():
    return ErrorTaxonomy()


class TestDeathAttributionTable:
    def test_transient_timeout_retries_without_meta(self, taxonomy):
        att = taxonomy.attribute_process_death(
            None, timed_out=True, timeout_seconds=5.0, timeout_is_transient=True
        )
        assert att.retry_requested is True
        assert att.result_meta == {}
        assert att.retry_error == f"{ERR_TIMEOUT_PREFIX}5.0s)"
        assert att.dlq_error_type == ERROR_TYPE_TRANSIENT_EXHAUSTED

    def test_non_transient_timeout_is_terminal_fatal(self, taxonomy):
        att = taxonomy.attribute_process_death(
            None, timed_out=True, timeout_seconds=3.0, timeout_is_transient=False
        )
        assert att.retry_requested is False
        assert att.result_meta == {"error": f"{ERR_TIMEOUT_PREFIX}3.0s)"}
        assert att.retry_error is None
        assert att.dlq_error_type == ERROR_TYPE_FATAL

    @pytest.mark.parametrize("code", [-9, -11, -15])
    def test_negative_exitcode_is_signal_death(self, taxonomy, code):
        att = taxonomy.attribute_process_death(
            code, timed_out=False, timeout_is_transient=False
        )
        assert att.retry_requested is True
        assert att.result_meta["error"] == f"{ERR_PROCESS_CRASH_PREFIX}{code}"
        assert "signal" in att.result_meta
        assert att.result_meta["oom_hint"] is (code == -9)
        assert att.retry_error.startswith(ERR_PROCESS_SIGNAL_DEATH)
        assert att.dlq_error_type == ERROR_TYPE_TRANSIENT_EXHAUSTED

    def test_signal_name_resolved_for_known_signal(self, taxonomy):
        att = taxonomy.attribute_process_death(-9, timed_out=False, timeout_is_transient=False)
        assert att.result_meta["signal"] == "SIGKILL"

    def test_unknown_signal_number_falls_back_to_signo(self, taxonomy):
        att = taxonomy.attribute_process_death(-99, timed_out=False, timeout_is_transient=False)
        assert att.result_meta["signal"] == "SIGNO99"

    @pytest.mark.parametrize("code", [1, 2, 42])
    def test_positive_exitcode_is_env_crash(self, taxonomy, code):
        att = taxonomy.attribute_process_death(
            code, timed_out=False, timeout_is_transient=False
        )
        assert att.retry_requested is True
        assert att.result_meta == {"error": f"{ERR_PROCESS_CRASH_PREFIX}{code}"}
        assert "signal" not in att.result_meta
        assert "oom_hint" not in att.result_meta
        assert att.retry_error == f"PROCESS_CRASH_EXIT: worker exited with code {code}"
        assert att.dlq_error_type == ERROR_TYPE_TRANSIENT_EXHAUSTED

    @pytest.mark.parametrize("code", [None, 0])
    def test_zero_or_missing_exitcode_is_no_ipc_result(self, taxonomy, code):
        att = taxonomy.attribute_process_death(
            code, timed_out=False, timeout_is_transient=False
        )
        assert att.retry_requested is True
        assert att.result_meta == {"error": ERR_NO_IPC_RESULT}
        assert att.retry_error == (
            f"{ERR_NO_IPC_RESULT}: worker exited without writing a result file"
        )
        assert att.dlq_error_type == ERROR_TYPE_TRANSIENT_EXHAUSTED


class TestDeathStringClassification:
    """决策表产出的串族必须经 classify/DLQ 分类链得到非 unknown 分类。"""

    @pytest.mark.parametrize(
        "error_str, expected_type",
        [
            (f"{ERR_TIMEOUT_PREFIX}5s)", ERROR_TYPE_FATAL),
            (f"{ERR_PROCESS_CRASH_PREFIX}-9", ERROR_TYPE_TRANSIENT_EXHAUSTED),
            (ERR_NO_IPC_RESULT, ERROR_TYPE_TRANSIENT_EXHAUSTED),
            (f"{ERR_PROCESS_SIGNAL_DEATH}: killed by SIGKILL (-9)", ERROR_TYPE_TRANSIENT_EXHAUSTED),
        ],
    )
    def test_death_strings_classified(self, taxonomy, error_str, expected_type):
        cl = taxonomy.classify({"error": error_str})
        assert cl.dlq_error_type == expected_type
        assert classify_error_type({"error": error_str}) == expected_type

    def test_arbitrary_business_string_still_unknown(self, taxonomy):
        assert classify_error_type({"error": "some handler message"}) == "unknown"

    def test_timeout_string_marks_fatal(self, taxonomy):
        cl = taxonomy.classify({"error": f"{ERR_TIMEOUT_PREFIX}5s)"})
        assert cl.is_fatal is True
        assert cl.category == ErrorCategory.FATAL

    def test_decision_table_rows_carry_consistent_dlq_types(self, taxonomy):
        """决策表每行的 dlq_error_type 与其错误串的分类结论一致——两表同源。"""
        rows = [
            taxonomy.attribute_process_death(None, timed_out=True, timeout_seconds=1.0,
                                             timeout_is_transient=True),
            taxonomy.attribute_process_death(None, timed_out=True, timeout_seconds=1.0,
                                             timeout_is_transient=False),
            taxonomy.attribute_process_death(-9, timed_out=False),
            taxonomy.attribute_process_death(1, timed_out=False),
            taxonomy.attribute_process_death(None, timed_out=False),
        ]
        for att in rows:
            assert isinstance(att, DeathAttribution)
            error = att.result_meta.get("error")
            if error:
                assert classify_error_type({"error": error}) == att.dlq_error_type


class TestHarvestPathParity:
    """收割路径对拍不变式：同一根因经「有结果文件解码路径」与「无结果文件
    终局构造路径」必须得到相同的 meta 形状 / retry_error / 瞬态语义。"""

    @pytest.fixture()
    def channel(self):
        from tasklite.engine.channel import ExecutionChannel
        return ExecutionChannel()

    @staticmethod
    def _handle(job):
        from tasklite.engine.channel import JobHandle
        import time
        return JobHandle(
            uid=job.uid, process=None,
            deadline=time.monotonic() + 60, timeout=30.0,
            job=job, ipc_dir=None, incarnation="inc",
        )

    @pytest.mark.parametrize("exitcode", [-9, -11, 1, 0, None])
    def test_decode_and_terminal_paths_agree(self, channel, exitcode):
        from types import SimpleNamespace

        from tasklite.engine.channel import _decode_ipc_result
        from tasklite.models.job import Job

        job = Job("t", "a")
        p = SimpleNamespace(exitcode=exitcode)

        decoded = _decode_ipc_result({}, p, job)
        terminal = channel._build_terminal_failure(p, self._handle(job), is_timeout=False)

        assert decoded.result_meta == terminal.result_meta
        assert decoded.retry_error == terminal.retry_error
        assert decoded.retry_requested == terminal.retry_requested

    def test_signal_death_meta_is_uniform(self, channel):
        """-9 双路径均携带 signal/oom_hint 结构化键(形状分歧消除)。"""
        from types import SimpleNamespace

        from tasklite.engine.channel import _decode_ipc_result
        from tasklite.models.job import Job

        job = Job("t", "a")
        p = SimpleNamespace(exitcode=-9)
        decoded = _decode_ipc_result({}, p, job)
        assert decoded.result_meta["signal"] == "SIGKILL"
        assert decoded.result_meta["oom_hint"] is True
