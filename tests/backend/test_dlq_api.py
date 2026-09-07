"""状态管理 API 测试。

- DLQ 管理 API（list_dlq/clear_dlq/clear_history）+ error_type/failed_at 落库
- 种子化 API（seed_wall/seed_cursor）+ sanitize_content_id 公开
- bulk 3-strike（commit_bulk_failure 失败也计数，达阈值转 DLQ）

规则：所有管理 API 仅限 run() 之外调用（改变 is_known 判定基础）。
"""

import pytest
from tasklite import TaskLite, Job
from tasklite.backend.base import classify_error_type
from tasklite.backend.sqlite_backend import SQLiteStateBackend
from tasklite.taxonomy import (
    ERR_DEPENDENCY_DEADLOCK, ERR_DISPATCH_FAILURE, ERR_JOB_DEPENDENCY, ERR_MAX_RETRIES,
    ERROR_TYPE_DEADLOCK, ERROR_TYPE_DEPENDENCY, ERROR_TYPE_DISPATCH,
    ERROR_TYPE_FATAL, ERROR_TYPE_TRANSIENT_EXHAUSTED, ERROR_TYPE_UNKNOWN,
)
from tasklite.pipeline import _CommitCrashSignal
from tasklite.wrappers.discovery import sanitize_content_id


def _pipeline(tmp_path, name="r1"):
    return TaskLite(name=name, state_dir=tmp_path / "state", backend="sqlite")


def _ok_handler(job, ctx):
    return True


# ══════════════════════════════════════════════════════════════════════
# error_type 分类（classify_error_type 纯函数 + 落库）
# ══════════════════════════════════════════════════════════════════════


class TestErrorTypeClassification:
    def test_fatal_flag_wins(self):
        assert classify_error_type({"error": "boom", "fatal": True}) == ERROR_TYPE_FATAL

    def test_framework_error_codes(self):
        assert classify_error_type({"error": ERR_DEPENDENCY_DEADLOCK}) == ERROR_TYPE_DEADLOCK
        assert classify_error_type({"error": ERR_JOB_DEPENDENCY}) == ERROR_TYPE_DEPENDENCY
        assert classify_error_type({"error": ERR_MAX_RETRIES}) == ERROR_TYPE_TRANSIENT_EXHAUSTED

    def test_prefix_match(self):
        # 框架错误码是 error 前缀（如 "DEPENDENCY_DEADLOCK: ..." 带详情）
        assert classify_error_type(
            {"error": f"{ERR_DEPENDENCY_DEADLOCK}: cycle a::b"}
        ) == ERROR_TYPE_DEADLOCK

    def test_unknown_error_falls_to_unknown(self):
        assert classify_error_type({"error": "arbitrary handler message"}) == ERROR_TYPE_UNKNOWN
        assert classify_error_type({}) == ERROR_TYPE_UNKNOWN

    def test_non_dict_meta_defensive(self):
        """meta 非 dict（None/脏数据/损坏行）不得让 list_dlq() 查询整条崩——
        pre-fix：meta.get 抛 AttributeError。防御：归 unknown 不打断查询。"""
        assert classify_error_type(None) == ERROR_TYPE_UNKNOWN
        assert classify_error_type("boom") == ERROR_TYPE_UNKNOWN
        assert classify_error_type(["x"]) == ERROR_TYPE_UNKNOWN
        assert classify_error_type(42) == ERROR_TYPE_UNKNOWN

    def test_error_type_and_failed_at_persisted(self, tmp_path):
        """_write_dlq_row 落库时统一补 error_type + failed_at（单一出口）。"""
        p = _pipeline(tmp_path)
        p.backend.append_failed("t::x", {"error": ERR_DEPENDENCY_DEADLOCK})
        meta = p.backend.load_failed()["t::x"]
        assert meta["error_type"] == ERROR_TYPE_DEADLOCK
        assert meta["failed_at"], "必须带失败时间戳"

    def test_fatal_written_with_fatal_type(self, tmp_path):
        """fatal: true 的条目（FatalError 路径）error_type=fatal。"""
        p = _pipeline(tmp_path)
        p.backend.append_failed("t::f", {"error": "boom", "fatal": True})
        assert p.backend.load_failed()["t::f"]["error_type"] == ERROR_TYPE_FATAL

    def test_dlq_end_to_end_corrupt_rows_do_not_crash(self, tmp_path):
        """非字典损坏行兼容：合法 JSON 标量损坏行（手改/遗留）不炸 list_dlq()/clear_dlq()。

        pre-fix：dict("boom")/dict(42) 抛 TypeError/ValueError → list_dlq() 整条崩；
        clear_dlq 的 meta.get 抛 AttributeError。端到端验证（此前只有
        classify_error_type 纯函数防御，无 list_dlq() 层覆盖）。
        """
        import sqlite3
        p = _pipeline(tmp_path)
        db_path = p.backend.path
        conn = sqlite3.connect(db_path)
        for uid, payload in (("t::str", '"boom"'), ("t::int", "42"),
                             ("t::bool", "true"), ("t::list", '[1,2]')):
            conn.execute(
                "INSERT OR REPLACE INTO failed_dlq (uid, payload) "
                "VALUES (?, ?)", (uid, payload)
            )
        conn.commit()
        conn.close()

        entries = p.list_dlq()
        uids = {e.uid for e in entries}
        assert {"t::str", "t::int", "t::bool", "t::list"} <= uids
        for e in entries:
            assert e.error_type == ERROR_TYPE_UNKNOWN
            assert e.meta == {}
        # clear_dlq 对损坏行不炸（可删除 = 修复手段）
        n = p.clear_dlq(task_types=["t"])
        assert n == 4, f"all corrupt rows should be revivable, got {n}"
        assert p.list_dlq() == []

    def test_dispatch_failure_classified(self):
        """ERR_DISPATCH_FAILURE 归 dispatch 而非 unknown——
        list_dlq() 可按「派发失败」过滤（此前归 unknown，排障无法按根因归类）。"""
        assert classify_error_type({"error": ERR_DISPATCH_FAILURE}) == ERROR_TYPE_DISPATCH

    def test_dispatch_failure_prefix_match(self):
        """dispatch 错误码前缀匹配（与 COMMIT_FAILURE 分支对称）。"""
        assert classify_error_type(
            {"error": f"{ERR_DISPATCH_FAILURE}: unpickleable handler"}
        ) == ERROR_TYPE_DISPATCH

    def test_dispatch_type_persisted(self, tmp_path):
        """dispatch error_type 经 _write_dlq_row 落库（单一出口）。"""
        p = _pipeline(tmp_path)
        p.backend.append_failed("t::d", {"error": ERR_DISPATCH_FAILURE, "failures": 3})
        meta = p.backend.load_failed()["t::d"]
        assert meta["error_type"] == ERROR_TYPE_DISPATCH
        assert meta["failures"] == 3, "原有字段不得丢失"


# ══════════════════════════════════════════════════════════════════════
# list_dlq 只读查询
# ══════════════════════════════════════════════════════════════════════


class TestDlqQuery:
    def test_dlq_returns_structured_entries(self, tmp_path):
        p = _pipeline(tmp_path)
        p.backend.append_failed("t::a", {"error": ERR_DEPENDENCY_DEADLOCK})
        p.backend.append_failed("t::b", {"error": "boom", "fatal": True})
        p.backend.append_failed("t::b", {"error": "boom", "fatal": True})  # attempts=2

        entries = p.list_dlq()
        by_uid = {e.uid: e for e in entries}
        assert set(by_uid) == {"t::a", "t::b"}
        assert by_uid["t::a"].error_type == ERROR_TYPE_DEADLOCK
        assert by_uid["t::a"].attempts == 1
        assert by_uid["t::a"].failed_at
        assert by_uid["t::b"].error_type == ERROR_TYPE_FATAL
        assert by_uid["t::b"].attempts == 2
        assert by_uid["t::a"].meta["error"] == ERR_DEPENDENCY_DEADLOCK

    def test_dlq_empty(self, tmp_path):
        p = _pipeline(tmp_path)
        assert p.list_dlq() == []


# ══════════════════════════════════════════════════════════════════════
# clear_dlq（清除 = 删 DLQ + 重新 enqueue）
# ══════════════════════════════════════════════════════════════════════


class TestReviveFailed:
    def _seed(self, p):
        p.backend.append_failed("dl::a", {"error": "boom", "fatal": True})
        p.backend.append_failed("dl::b", {"error": "net", "fatal": False})
        p.backend.append_failed("other::c", {"error": "x"})

    def test_keeps_fatal_by_default(self, tmp_path):
        p = _pipeline(tmp_path)
        self._seed(p)
        assert p.clear_dlq() == 2  # b、c 清除，fatal 的 a 保留
        assert set(p.backend.load_failed()) == {"dl::a"}

    def test_keep_fatal_false_deletes_all(self, tmp_path):
        p = _pipeline(tmp_path)
        self._seed(p)
        assert p.clear_dlq(keep_fatal=False) == 3
        assert p.backend.load_failed() == {}

    def test_task_types_filter(self, tmp_path):
        p = _pipeline(tmp_path)
        self._seed(p)
        assert p.clear_dlq(task_types=["dl"]) == 1  # 只 dl::b（a 是 fatal 保留）
        assert set(p.backend.load_failed()) == {"dl::a", "other::c"}

    def test_no_match_returns_zero(self, tmp_path):
        p = _pipeline(tmp_path)
        assert p.clear_dlq(task_types=["nope"]) == 0

    def test_revive_then_enqueue_reruns(self, tmp_path):
        """清除语义：删 DLQ 后 enqueue 同名任务可重跑（is_known 不再算已知）。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        p.backend.append_failed("t::x", {"error": "net"})
        # 清除前 enqueue → 被去重跳过（is_known 含 failed）
        p.enqueue([Job("t", "x")])
        # 清除后 enqueue → 正常执行
        assert p.clear_dlq() == 1
        p.enqueue([Job("t", "x")])
        p.run()
        assert "t::x" in p.backend.load_wall()


# ══════════════════════════════════════════════════════════════════════
# clear_history（wall/DLQ 删除通道）
# ══════════════════════════════════════════════════════════════════════


class TestForget:
    def test_forget_both_wall_and_failed(self, tmp_path):
        p = _pipeline(tmp_path)
        p.seed_wall(["t::a", "t::b", "other::c"])
        p.backend.append_failed("t::b", {"error": "x"})
        p.backend.append_failed("t::d", {"error": "x"})

        assert p.clear_history("t::") == 4 # wall 2 + failed 2
        assert p.backend.load_wall() == {"other::c": {}}
        assert p.backend.load_failed() == {}

    def test_forget_exact_uid(self, tmp_path):
        p = _pipeline(tmp_path)
        p.seed_wall(["t::a", "t::b"])
        assert p.clear_history("t::a") == 1
        assert set(p.backend.load_wall()) == {"t::b"}

    def test_forget_only_wall(self, tmp_path):
        p = _pipeline(tmp_path)
        p.seed_wall(["t::a"])
        p.backend.append_failed("t::a", {"error": "x"})
        assert p.clear_history("t::a", where=("wall",)) == 1
        assert p.backend.load_wall() == {}
        assert set(p.backend.load_failed()) == {"t::a"}

    def test_prefix_requires_double_colon(self, tmp_path):
        """防前缀误匹配：无 :: 结尾的 pattern 只精确匹配完整 uid。"""
        p = _pipeline(tmp_path)
        p.seed_wall(["download::x", "downloads::y"])
        # "download" 既不是完整 uid、也不以 :: 结尾 → 不匹配任何条目
        assert p.clear_history("download") == 0
        assert set(p.backend.load_wall()) == {"download::x", "downloads::y"}
        # "download::" 以 :: 结尾 → 只匹配前缀（不误伤 downloads::y）
        assert p.clear_history("download::") == 1
        assert set(p.backend.load_wall()) == {"downloads::y"}

    def test_forget_uid_list(self, tmp_path):
        p = _pipeline(tmp_path)
        p.seed_wall(["t::a", "t::b", "t::c"])
        assert p.clear_history(["t::a", "t::c"]) == 2
        assert set(p.backend.load_wall()) == {"t::b"}


# ══════════════════════════════════════════════════════════════════════
# 种子化 API + sanitize 公开
# ══════════════════════════════════════════════════════════════════════


class TestSeeding:
    def test_seed_wall_makes_known(self, tmp_path):
        """seed_wall 后 enqueue 同 uid 被 is_known 去重跳过（存档迁移语义）。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        assert p.seed_wall(["t::archived_1", "t::archived_2"]) == 2

        p.enqueue([Job("t", "archived_1"), Job("t", "fresh")])
        p.run()
        wall = p.backend.load_wall()
        assert "t::fresh" in wall
        assert wall["t::archived_1"] == {}, "种子条目 meta 应为空 dict"
        assert p.stats["completed"] == 1  # 只有 fresh 真正执行

    def test_seed_wall_rejects_bad_uid(self, tmp_path):
        p = _pipeline(tmp_path)
        with pytest.raises(ValueError, match="task_type::job_id"):
            p.seed_wall(["no-colon"])

    def test_seed_cursor_readable_via_get_cursor(self, tmp_path):
        p = _pipeline(tmp_path)
        p.seed_cursor("progress", "2026-01-01")
        assert p.backend.load_cursors()["progress"] == "2026-01-01"

    def test_sanitize_content_id_public(self):
        """sanitize_content_id 公开入口在 wrappers.discovery。"""
        from tasklite.wrappers.discovery import sanitize_content_id as s2
        assert sanitize_content_id is s2

    def test_sanitize_clean_id_unchanged(self):
        """单射转义：干净 id 原样输出、零后缀（可读性）。"""
        assert sanitize_content_id("12345") == "12345"
        assert sanitize_content_id("normal-1.2_abc") == "normal-1.2_abc"

    def test_sanitize_dirty_id_escaped_injectively(self):
        """脏 id 百分号转义；不同输入派生不同 job_id（单射）。"""
        assert sanitize_content_id("a::b") == "a%3A%3Ab"  # :: → %3A%3A（Job 分隔符安全）
        assert sanitize_content_id("a/b") == "a%2Fb"
        assert sanitize_content_id("a::b") != sanitize_content_id("a/b")
        assert sanitize_content_id("a::b") != sanitize_content_id("a%3A%3Ab"), \
            "字面 %3A%3A 必须被 %25 吸收，不与转义序列混淆（单射关键）"
        assert sanitize_content_id("a%3A%3Ab") == "a%253A%253Ab"

    def test_sanitize_all_symbols_falls_back(self):
        """全符号输入仍派生确定结果（不空串）。"""
        assert sanitize_content_id("///") == "%2F%2F%2F"
        assert sanitize_content_id("///") == sanitize_content_id("///"), "确定性"

    def test_sanitize_overlong_truncates_with_hash(self):
        """超长 id：截断 + 8 位 hash（唯一保留 hash 的场景），不碰撞。"""
        long_a = "A" * 115 + "/1"
        long_b = "A" * 115 + "/2"
        assert len(sanitize_content_id(long_a)) <= 120
        assert sanitize_content_id(long_a) != sanitize_content_id(long_b), \
            "截断不得导致长 content_id 碰撞（否则内容被 wall 去重吞掉）"


# ══════════════════════════════════════════════════════════════════════
# bulk 3-strike（commit_bulk_failure 失败也计数）
# ══════════════════════════════════════════════════════════════════════


class TestBulkFailureThreeStrike:
    def _init_state(self, p):
        """初始化 _state（run() 内才做；单元测试直调 _commit_bulk_failed_crash 需要）。"""
        from tasklite.models.state import PipelineState
        p._state = PipelineState(
            p.backend.load_wall(), p.backend.load_failed(),
            p.backend.load_cursors(), p.backend.load_queue(),
        )

    def test_commit_bulk_failed_crash_counts_then_dlqs(self, tmp_path):
        """_commit_bulk_failed_crash：逐 job 计数，达阈值单条 DLQ 成功。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        self._init_state(p)
        jd = Job("t", "a").to_dict()
        uids_metas = [("t::a", {"error": ERR_DEPENDENCY_DEADLOCK})]

        # 同 jd 对象连续调用（计数原地递增，模拟跨 boot 持久化累计）
        queue, kept = p._failure.commit_bulk_failed_crash("test", uids_metas, [jd])
        assert kept is True
        assert queue[0]["runtime"]["_commit_failures"] == 1
        queue, kept = p._failure.commit_bulk_failed_crash("test", uids_metas, [jd])
        assert kept is True
        assert queue[0]["runtime"]["_commit_failures"] == 2
        # 达阈值 → 单条 DLQ 成功 → 不保留
        queue, kept = p._failure.commit_bulk_failed_crash("test", uids_metas, [jd])
        assert kept is False
        assert queue == []
        assert "t::a" in p.backend.load_failed()

    def test_dlq_also_fails_keeps_in_queue(self, tmp_path, monkeypatch):
        """防御：达阈值但单条 DLQ 也失败 → 保留队列（下次 boot 再试）。"""
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        self._init_state(p)
        jd = Job("t", "a").to_dict()
        jd["runtime"] = {"_commit_failures": 3}  # 已连续失败 3 次
        monkeypatch.setattr(p.backend, "commit_job_failure", lambda uid, meta: False)

        queue, kept = p._failure.commit_bulk_failed_crash(
            "test", [("t::a", {"error": "x"})], [jd]
        )
        assert kept is True, "DLQ 失败必须保留（不静默丢失）"
        assert queue[0]["runtime"]["_commit_failures"] == 4

    def test_single_commit_dlq_also_fails_falls_back_to_crash(self, tmp_path, monkeypatch):
        """单条 `_commit_failed_crash` 达阈值后，
        DLQ 提交也失败 → fall-through 到 crash 路径（requeue + _CommitCrashSignal）。

        bulk 变体由 test_dlq_also_fails_keeps_in_queue 覆盖，但单条路径
        （failure.py 中的 DLQ 兜底提交路径）此前无触发测试——删掉 fall-through 测试全绿。
        """
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        self._init_state(p)
        jd = Job("t", "a").to_dict()
        jd["runtime"] = {"_commit_failures": 2}  # +1 后达阈值 3
        jd["runtime"]["_backoff_until"] = 0.0
        jd["runtime"]["_backoff_wall_deadline"] = 0.0
        # 注释修正：直接调 `_commit_failed_crash`（unit 级，
        # 非 run 真实路径）——下方 enqueue 仅用于确保磁盘队列有该 uid
        # 基线状态，断言目标是内存 `_state` 队列。
        p.enqueue([Job("t", "a", payload={})])
        monkeypatch.setattr(p.backend, "commit_job_failure", lambda uid, meta: False)

        # _commit_failures=2 → failures=3 达阈值 → DLQ 也失败 → 回 crash
        with pytest.raises(_CommitCrashSignal):
            p._failure.commit_failed_crash("t::a", "test", jd)
        # job 必须 requeue 到内存队列（不静默丢失），计数递增到 3
        q = p._state.queue
        assert len(q) == 1, f"job must be requeued after DLQ-also-fails, got {q}"
        assert q[0]["runtime"]["_commit_failures"] == 3

    def test_deadlock_integration_3strike(self, tmp_path, monkeypatch):
        """集成：死锁 bulk 失败 → 前两次 crash、第三次达阈值 DLQ 正常结束。

        验证修复：持久性 DB 故障下不再无限崩溃循环。
        """
        p = _pipeline(tmp_path)
        p.register_handler("t", _ok_handler)
        # 依赖环死锁：a 依赖 b、b 依赖 a
        p.enqueue([Job("t", "a", depends_on=["t::b"]),
                   Job("t", "b", depends_on=["t::a"])])
        monkeypatch.setattr(
            p.backend, "commit_bulk_failure", lambda uids_metas: False
        )

        # 第 1、2 次：计数 1、2 → 崩溃（_CommitCrashSignal 上抛）
        for expected_failures in (1, 2):
            with pytest.raises(_CommitCrashSignal):
                p.run()
            q = p.backend.load_queue()
            assert all(jd["runtime"]["_commit_failures"] == expected_failures for jd in q), q

        # 第 3 次：达阈值 → 单条 DLQ 成功 → 不崩溃，run 正常结束
        p.run()
        failed = p.backend.load_failed()
        assert set(failed) == {"t::a", "t::b"}
        assert p.backend.load_queue() == []
