"""TaskLite.edit() 原子图修整事务全量验收测试（ADR-0012 §3.1 & §6）。

覆盖：
1. 六大科研演进场景（换盘符、游离缓存纳管、分支改名与 replace_many 顺移/互换、
   调大 timeout 局部重跑、中间插层、双管线拆分）；
2. 空 TaskLite（未调用 register_task）直接执行 set_status / remove；
3. 4 项硬 Error 单次全量攒错（含动态 output 放行与 OSError 原样上抛 vs diff 不崩）；
4. 4 类只读 Warning（diff 输出与 logger.warning 同步，不阻断提交）；
5. 遗留 running 单次补写去重、未触碰 running 零扰动与身份损坏拒绝；
6. attempts 永不可删与 job_sequences 永不重置；
7. tx.before / tx.after / tx.diff() 退出后可读且与落盘重开快照 100% 同形；
8. 生命周期守卫、异常放弃、单写者互斥与 SQLite / Memory 双后端 100% 对等。
"""
from __future__ import annotations

from contextlib import closing
import copy
import logging
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from tasklite import FileSpec, Job, StateSnapshot, TaskLite, TaskLiteError, TaskResult
from tasklite.backend.base import GraphEditCommit
from tasklite.edit import EditTransaction
from tasklite.testing import in_process_workers, running, seed_running_job
from tasklite.utils.lockfile import release_lock, try_acquire_lock


def _ok_handler(job: Job, ctx: Any) -> TaskResult:
    return TaskResult.success(metadata={"id": job.job_id, "payload": job.payload})


def _dynamic_producer(job: Job, ctx: Any) -> TaskResult:
    out_path = Path(ctx.declare_file("dyn_out", FileSpec(job.payload["out"], role="output")))
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text("dynamic-data", encoding="utf-8")
    return TaskResult.success()


def _consumer_handler(job: Job, ctx: Any) -> TaskResult:
    src = ctx.file_path("src")
    return TaskResult.success(metadata={"content": src.read_text(encoding="utf-8")})


def _make_pipeline(backend: str, tmp_path: Path, name: str = "state.db") -> TaskLite:
    return TaskLite(
        backend=backend,  # type: ignore[arg-type]
        state_path=(tmp_path / name) if backend == "sqlite" else None,
    )


@pytest.fixture(params=["memory", "sqlite"])
def backend_kind(request: pytest.FixtureRequest) -> str:
    return str(request.param)


def _snapshot_dump(snap: StateSnapshot) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]]]:
    jobs = {j["job_uid"]: j for j in snap.iter_jobs()}
    attempts = list(snap.iter_attempts())
    return jobs, attempts


# ── 一、六大科研演进场景（ADR-0012 §6）──────────────────────────────────


class TestResearchEvolutionScenarios:
    """ADR-0012 §6 六大典型科研长流水线演进场景（双后端对等）。"""

    def test_scenario1_drive_relocation_inherits_states_and_rolls_back_on_missing_file(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        old_dir = tmp_path / "disk_d"
        new_dir = tmp_path / "disk_e"
        old_dir.mkdir()
        new_dir.mkdir()
        (old_dir / "a.dat").write_text("alpha", encoding="utf-8")
        (new_dir / "a.dat").write_text("alpha", encoding="utf-8")
        # 刻意先漏移 b.dat 以验证整单回滚

        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("step", _ok_handler)
            p.register_task("consume", _consumer_handler)

            with p.edit(dry_run=False) as tx:
                tx.add(
                    Job("step", "a", files={"out": FileSpec(old_dir / "a.dat", role="output")}),
                    status="succeeded",
                )
                tx.add(
                    Job("step", "b", files={"out": FileSpec(old_dir / "a.dat", role="output")}),
                    status="succeeded",
                )
                tx.add(
                    Job("step", "c", payload={"v": 1}),
                    status="failed",
                )
                tx.add(
                    Job(
                        "consume",
                        "d",
                        depends_on=["step::a"],
                        files={"src": FileSpec.from_output("step::a", "out", role="input")},
                    ),
                    status="pending",
                )

            def build_jobs(root: Path) -> list[Job]:
                return [
                    Job("step", "a", files={"out": FileSpec(root / "a.dat", role="output")}),
                    Job("step", "b", files={"out": FileSpec(root / "b.dat", role="output")}),
                    Job("step", "c", payload={"v": 2}),
                    Job(
                        "consume",
                        "d",
                        depends_on=["step::a"],
                        files={"src": FileSpec.from_output("step::a", "out", role="input")},
                    ),
                ]

            # 1 个文件漏移（b.dat 缺失）-> 整单回滚，旧成功路径完全无损
            with pytest.raises(TaskLiteError, match="b.dat"):
                with p.edit(dry_run=False, note="relocate D -> E") as tx:
                    for job in build_jobs(new_dir):
                        if job.uid in tx:
                            tx.replace(job.uid, job)

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("step::a")["files"]["out"]["path"] == str((old_dir / "a.dat").resolve())  # type: ignore[index]
                assert snap.get_job("step::c")["job"]["payload"] == {"v": 1}  # type: ignore[index]

            # 补齐 b.dat 后重跑迁移脚本 -> 全部自动继承原状态且产出指向新盘符
            (new_dir / "b.dat").write_text("beta", encoding="utf-8")
            with p.edit(dry_run=False, note="relocate D -> E") as tx:
                for job in build_jobs(new_dir):
                    if job.uid in tx:
                        tx.replace(job.uid, job)

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("step::a")["status"] == "succeeded"  # type: ignore[index]
                assert snap.get_job("step::b")["status"] == "succeeded"  # type: ignore[index]
                assert snap.get_job("step::c")["status"] == "failed"  # type: ignore[index]
                assert snap.get_job("consume::d")["status"] == "pending"  # type: ignore[index]
                assert snap.get_job("step::a")["files"]["out"]["path"] == str((new_dir / "a.dat").resolve())  # type: ignore[index]

            # 下游消费新盘符产物正常执行成功
            with in_process_workers(p):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snap:
                rec = snap.get_job("consume::d")
                assert rec is not None and rec["status"] == "succeeded"
                assert rec["result"]["metadata"]["content"] == "alpha"

    def test_scenario2_adopt_loose_cache_publishes_output_and_prevents_overwrite(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        cache_file = tmp_path / "cached_model.bin"
        cache_file.write_text("pretrained-weights", encoding="utf-8")

        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("pretrain", _ok_handler)
            p.register_task("eval", _consumer_handler)

            cache_job = Job(
                "pretrain",
                "base",
                files={
                    "weights": FileSpec(
                        cache_file,
                        role="output",
                        delete_existing=True,
                        delete_on_failure=True,
                    ),
                    "scratch": FileSpec(
                        tmp_path / "nonexistent_tmp.bin",
                        role="output",
                        delete_on_success=True,
                    ),
                    "raw_in": FileSpec(
                        tmp_path / "missing_input.txt",
                        role="input",
                    ),
                },
            )
            with p.edit(dry_run=False, note="adopt historical cache") as tx:
                tx.add(cache_job, status="succeeded")
                # 防覆盖既有同 UID 任务
                with pytest.raises(TaskLiteError, match="already exists"):
                    tx.add(cache_job, status="succeeded")
                tx.add(
                    Job(
                        "eval",
                        "e1",
                        depends_on=["pretrain::base"],
                        files={"src": FileSpec.from_output("pretrain::base", "weights", role="input")},
                    ),
                    status="pending",
                )

            # 零磁盘删除副作用、不伪造 Attempt、仅发布保留型 output
            assert cache_file.exists()
            with StateSnapshot.open(p) as snap:
                adopted = snap.get_job("pretrain::base")
                assert adopted is not None and adopted["status"] == "succeeded"
                assert adopted["result"]["annotations"] == {
                    "completion_source": "manual",
                    "note": "adopt historical cache",
                }
                assert "weights" in adopted["files"]
                assert "scratch" not in adopted["files"]
                assert list(snap.iter_attempts(job_uid="pretrain::base")) == []

            with in_process_workers(p):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snap:
                ev = snap.get_job("eval::e1")
                assert ev is not None and ev["status"] == "succeeded"
                assert ev["result"]["metadata"]["content"] == "pretrained-weights"

    def test_scenario3_branch_rename_and_replace_many_shift_and_swap(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        f1 = tmp_path / "s1.txt"
        f2 = tmp_path / "s2.txt"
        f1.write_text("one", encoding="utf-8")
        f2.write_text("two", encoding="utf-8")

        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("step", _ok_handler)
            with p.edit(dry_run=False) as tx:
                tx.add(Job("step", "1", files={"o": FileSpec(f1, role="output")}), status="succeeded")
                tx.add(Job("step", "2", depends_on=["step::1"], files={"o": FileSpec(f2, role="output")}), status="succeeded")
                tx.add(Job("step", "summary", depends_on=["step::2"]), status="pending")

            # 分支改名时漏迁下游 summary -> 孤儿检查当场拦截
            with pytest.raises(TaskLiteError, match="lost existing dependency"):
                with p.edit(dry_run=False) as tx:
                    tx.replace_many({
                        "step::1": Job("step", "v2_1", files={"o": FileSpec(f1, role="output")}),
                        "step::2": Job("step", "v2_2", depends_on=["step::v2_1"], files={"o": FileSpec(f2, role="output")}),
                    })

            # 编号顺移 step::1 -> step::2, step::2 -> step::3，同时更新 summary
            with p.edit(dry_run=False) as tx:
                tx.replace_many({
                    "step::1": Job("step", "2", files={"o": FileSpec(f1, role="output")}),
                    "step::2": Job("step", "3", depends_on=["step::2"], files={"o": FileSpec(f2, role="output")}),
                    "step::summary": Job("step", "summary", depends_on=["step::3"]),
                })
                diff_text = tx.diff()
                assert "step::1 -> step::2" in diff_text
                assert "step::2 -> step::3" in diff_text
                assert "[- REMOVED]\n  (none)" in diff_text

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("step::1") is None
                assert snap.get_job("step::2")["status"] == "succeeded"  # type: ignore[index]
                assert snap.get_job("step::3")["status"] == "succeeded"  # type: ignore[index]
                assert snap.get_job("step::summary")["job"]["depends_on"] == ["step::3"]  # type: ignore[index]

            # 互换两个独立节点 UID（step::2 <-> step::3 无依赖冲突）
            with p.edit(dry_run=False) as tx:
                tx.replace_many({
                    "step::2": Job("step", "3", payload={"swapped": "from_2"}, files={"o": FileSpec(f1, role="output")}),
                    "step::3": Job("step", "2", payload={"swapped": "from_3"}, files={"o": FileSpec(f2, role="output")}),
                })
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("step::3")["job"]["payload"] == {"swapped": "from_2"}  # type: ignore[index]
                assert snap.get_job("step::2")["job"]["payload"] == {"swapped": "from_3"}  # type: ignore[index]

    def test_scenario4_increase_timeout_and_local_recursive_rerun(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("up", _ok_handler, timeout_seconds=10)
            p.register_task("down", _ok_handler)

            p.enqueue(Job("up", "1", payload={"v": 1}))
            p.enqueue(Job("down", "1", depends_on=["up::1"]))
            p.enqueue(Job("down", "2", depends_on=["down::1"]))
            with in_process_workers(p):
                p.run(install_signals=False)

            with p.edit(dry_run=False, note="bump timeout") as tx:
                tx.set_status("up::1", "pending", recursive=True)
                tx.replace("up::1", Job("up", "1", payload={"v": 2}, timeout_seconds=7200))

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("up::1")["status"] == "pending"  # type: ignore[index]
                assert snap.get_job("up::1")["job"]["timeout_seconds"] == 7200  # type: ignore[index]
                assert snap.get_job("down::1")["status"] == "pending"  # type: ignore[index]
                assert snap.get_job("down::2")["status"] == "pending"  # type: ignore[index]

            with in_process_workers(p):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snap:
                for uid in ("up::1", "down::1", "down::2"):
                    assert snap.get_job(uid)["status"] == "succeeded"  # type: ignore[index]
                    assert len(list(snap.iter_attempts(job_uid=uid))) == 2

    def test_scenario5_insert_middle_layer_4_to_5(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        mid_file = tmp_path / "mid.parquet"
        mid_file.write_text("mid-layer", encoding="utf-8")

        with _make_pipeline(backend_kind, tmp_path) as p:
            for layer in ("l1", "mid", "l2", "l3"):
                p.register_task(layer, _ok_handler)
            p.enqueue(Job("l1", "1"))
            p.enqueue(Job("l2", "1", depends_on=["l1::1"]))
            p.enqueue(Job("l3", "1", depends_on=["l2::1"]))
            with in_process_workers(p):
                p.run(install_signals=False)

            with p.edit(dry_run=False, note="insert mid layer") as tx:
                tx.add(
                    Job(
                        "mid",
                        "1",
                        depends_on=["l1::1"],
                        files={"tbl": FileSpec(mid_file, role="output")},
                    ),
                    status="succeeded",
                )
                tx.replace(
                    "l2::1",
                    Job(
                        "l2",
                        "1",
                        depends_on=["mid::1"],
                        files={"in_tbl": FileSpec.from_output("mid::1", "tbl", role="input")},
                    ),
                )

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("mid::1")["status"] == "succeeded"  # type: ignore[index]
                l2 = snap.get_job("l2::1")
                assert l2 is not None and l2["status"] == "succeeded"
                assert l2["job"]["depends_on"] == ["mid::1"]

    def test_scenario6_dual_pipeline_split_removes_upstream_and_replaces_cut_node(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        l2_out = tmp_path / "l2_output.bin"
        l2_out.write_text("stage-2-artifact", encoding="utf-8")

        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("l1", _ok_handler)
            p.register_task("l2", _ok_handler)
            p.register_task("l3", _consumer_handler)

            with p.edit(dry_run=False) as tx:
                tx.add(Job("l1", "1"), status="succeeded")
                tx.add(
                    Job("l2", "1", depends_on=["l1::1"], files={"art": FileSpec(l2_out, role="output")}),
                    status="succeeded",
                )
                tx.add(
                    Job(
                        "l3",
                        "1",
                        depends_on=["l2::1"],
                        files={"src": FileSpec.from_output("l2::1", "art", role="input")},
                    ),
                    status="pending",
                )

            # 拆分管线：移除前段 l1/l2，将切割面 l3 改为普通 role="input" 根节点
            with p.edit(dry_run=False, note="split pipeline at l3") as tx:
                assert tx.remove("l1::1", recursive=False) == 1
                assert tx.remove("l2::1", recursive=False) == 1
                tx.replace(
                    "l3::1",
                    Job(
                        "l3",
                        "1",
                        depends_on=[],
                        files={"src": FileSpec(l2_out, role="input")},
                    ),
                )

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("l1::1") is None
                assert snap.get_job("l2::1") is None
                assert snap.get_job("l3::1")["job"]["depends_on"] == []  # type: ignore[index]

            with in_process_workers(p):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snap:
                l3 = snap.get_job("l3::1")
                assert l3 is not None and l3["status"] == "succeeded"
                assert l3["result"]["metadata"]["content"] == "stage-2-artifact"


# ── 二、空 TaskLite 零 TaskRegistry 依赖（CLI 与纯运维脚本）─────────────


class TestZeroRegistryDependency:
    """不调用 register_task 时 set_status / remove / diff / before / after 完全可用。"""

    def test_empty_pipeline_can_set_status_and_remove_without_register_task(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "ops.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p:
            p.register_task("custom_a", _ok_handler)
            p.register_task("custom_b", _ok_handler)
            p.enqueue(Job("custom_a", "1", payload={"k": "v"}))
            p.enqueue(Job("custom_b", "2", depends_on=["custom_a::1"]))
            with in_process_workers(p):
                p.run(install_signals=False)

        # 全新空 TaskLite（不注册任何 task）直接运维
        with TaskLite(backend="sqlite", state_path=db_path) as ops:
            with ops.edit(dry_run=False, note="cli requeue") as tx:
                assert "custom_a::1" in tx
                assert "custom_b::2" in tx
                tx.set_status("custom_a::1", "pending", recursive=True)
                diff_txt = tx.diff()
                assert "[↻ STATUS]" in diff_txt
                assert "PASS" in diff_txt

            with ops.edit(dry_run=False) as tx:
                assert tx.remove("custom_a::1", recursive=True) == 2

            # add / replace 仍强制要求已注册 task_type
            with pytest.raises(TaskLiteError, match="unknown task_type"):
                with ops.edit(dry_run=False) as tx:
                    tx.add(Job("custom_a", "new"), status="pending")


# ── 三、4 项硬 Error 单次全量攒错与 OSError 边界 ────────────────────────


class TestValidationFourHardErrors:
    """提交期单次全量攒错（4 项硬 Error 同步汇报），OSError 原样传播而 diff 永不崩。"""

    def test_single_pass_collects_all_four_hard_errors_together(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            with p.edit(dry_run=False) as tx:
                tx.add(Job("t", "keep_up"), status="succeeded")
                tx.add(Job("t", "surviving_child", depends_on=["t::keep_up"]), status="pending")
                tx.add(Job("t", "no_out_prod"), status="succeeded")

            # 在同一事务内故意同时制造全部 4 类硬错误：
            # 1) 删除 keep_up 导致 surviving_child 丧失既有上游（Error 1）
            # 2) 新增 cycle_a <-> cycle_b 依赖环 + dangling 悬空依赖（Error 2）
            # 3) bad_ref 引用已 succeeded 但未发布 output 的 no_out_prod（Error 3）
            # 4) manual_missing 声明不存在的物理 output 文件（Error 4）
            captured_diff = ""
            with pytest.raises(TaskLiteError) as exc_info:
                with p.edit(dry_run=True) as tx:
                    tx.remove("t::keep_up", recursive=False)
                    tx.add(Job("t", "cycle_a", depends_on=["t::cycle_b"]), status="pending")
                    tx.add(Job("t", "cycle_b", depends_on=["t::cycle_a"]), status="pending")
                    tx.add(Job("t", "dangling", depends_on=["t::ghost"]), status="pending")
                    tx.add(
                        Job(
                            "t",
                            "bad_ref",
                            depends_on=["t::no_out_prod"],
                            files={"x": FileSpec.from_output("t::no_out_prod", "missing_key", role="input")},
                        ),
                        status="pending",
                    )
                    tx.add(
                        Job(
                            "t",
                            "manual_missing",
                            files={"out": FileSpec(tmp_path / "absent.bin", role="output")},
                        ),
                        status="succeeded",
                    )
                    # diff() 永不因校验失败抛异常
                    captured_diff = tx.diff()

            err_msg = str(exc_info.value)
            assert "lost existing dependency" in err_msg
            assert "Dependency cycle detected" in err_msg
            assert "references missing dependency 't::ghost'" in err_msg
            assert "did not publish retained output 'missing_key'" in err_msg
            assert "does not exist on disk" in err_msg

            assert "[VALIDATION]" in captured_diff
            assert "FAIL" in captured_diff
            assert "lost existing dependency" in captured_diff
            assert "does not exist on disk" in captured_diff

    def test_historical_untouched_failed_dead_records_do_not_block_unrelated_edits(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            # 历史未触碰的 failed 死记录（含悬空依赖与历史死锁环）
            p._runtime.store.apply_graph_edit(
                GraphEditCommit(
                    failed_writes={
                        "t::dead_typo": (
                            {"job_snapshot": Job("t", "dead_typo", depends_on=["t::never_existed"]).to_dict()},
                            {},
                        ),
                        "t::dead_c1": (
                            {"job_snapshot": Job("t", "dead_c1", depends_on=["t::dead_c2"]).to_dict()},
                            {},
                        ),
                        "t::dead_c2": (
                            {"job_snapshot": Job("t", "dead_c2", depends_on=["t::dead_c1"]).to_dict()},
                            {},
                        ),
                    }
                )
            )

            # 无关分支正常增删改完全不被连坐
            with p.edit(dry_run=False) as tx:
                tx.add(Job("t", "healthy"), status="pending")
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("t::healthy")["status"] == "pending"  # type: ignore[index]

    def test_from_output_dynamic_output_passes_and_non_retained_rejected(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        dyn_file = tmp_path / "dyn.txt"
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("dyn", _dynamic_producer)
            p.register_task("cons", _consumer_handler)
            p.register_task("bad_prod", _ok_handler)

            # 运行期动态声明 output 成功发布后，edit 校验识别其外层 files/artifacts
            p.enqueue(Job("dyn", "1", payload={"out": str(dyn_file)}))
            with in_process_workers(p):
                p.run(install_signals=False)

            with p.edit(dry_run=False) as tx:
                tx.add(
                    Job(
                        "cons",
                        "1",
                        depends_on=["dyn::1"],
                        files={"src": FileSpec.from_output("dyn::1", "dyn_out", role="input")},
                    ),
                    status="pending",
                )
                assert "PASS" in tx.diff()

            # 非 succeeded 上游若静态声明为 delete_on_success=True 或 role="input"，强制报错
            with pytest.raises(TaskLiteError, match="non-retained or non-output"):
                with p.edit(dry_run=True) as tx:
                    tx.add(
                        Job(
                            "bad_prod",
                            "1",
                            files={"tmp": FileSpec(tmp_path / "tmp.txt", role="output", delete_on_success=True)},
                        ),
                        status="pending",
                    )
                    tx.add(
                        Job(
                            "cons",
                            "bad",
                            depends_on=["bad_prod::1"],
                            files={"src": FileSpec.from_output("bad_prod::1", "tmp", role="input")},
                        ),
                        status="pending",
                    )

    def test_manual_succeeded_strict_path_policy_and_oserror_propagation_vs_diff(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        state_path = tmp_path / "strict.db"
        real_file = tmp_path / "real.txt"
        real_file.write_text("ok", encoding="utf-8")
        link_file = tmp_path / "link.txt"
        link_file.symlink_to(real_file)

        with TaskLite(backend="sqlite", state_path=state_path) as p:
            p.register_task("t", _ok_handler)

            # 受保护 state_path、根路径、符号链接均被 PathPolicy(mode="strict") 拦截
            with pytest.raises(TaskLiteError) as exc_info:
                with p.edit(dry_run=True) as tx:
                    tx.add(Job("t", "prot", files={"o": FileSpec(state_path, role="output")}), status="succeeded")
                    tx.add(Job("t", "root", files={"o": FileSpec(Path(tmp_path.anchor), role="output")}), status="succeeded")
                    tx.add(Job("t", "sym", files={"o": FileSpec(link_file, role="output")}), status="succeeded")
            msg = str(exc_info.value)
            assert "protected root" in msg
            assert "filesystem root" in msg
            assert "symlink" in msg

            # 底层 OSError：diff() 渲染为 OS ERROR 不抛异常，__exit__ 原样向上传播
            def boom_inspect(path: Any, *, mode: str = "strict") -> Any:
                raise PermissionError("simulated EACCES")

            monkeypatch.setattr(p._runtime.store.path_policy, "inspect", boom_inspect)
            with pytest.raises(PermissionError, match="simulated EACCES"):
                with p.edit(dry_run=False) as tx:
                    tx.add(Job("t", "perm", files={"o": FileSpec(real_file, role="output")}), status="succeeded")
                    preview = tx.diff()
                    assert "[VALIDATION]" in preview
                    assert "OS ERROR" in preview
                    assert "simulated EACCES" in preview

    def test_deep_chain_cycle_detection_is_iterative_no_recursion_error(
        self, tmp_path: Path
    ) -> None:
        with TaskLite(backend="memory") as p:
            p.register_task("t", _ok_handler)
            depth = 1200
            with p.edit(dry_run=False) as tx:
                tx.add(Job("t", "n0"), status="pending")
                for i in range(1, depth):
                    tx.add(Job("t", f"n{i}", depends_on=[f"t::n{i - 1}"]), status="pending")
            with pytest.raises(TaskLiteError, match="Dependency cycle detected"):
                with p.edit(dry_run=True) as tx:
                    tx.replace("t::n0", Job("t", "n0", depends_on=[f"t::n{depth - 1}"]))


# ── 四、4 类只读 Warning（不阻断提交，同步 logger.warning）──────────────


class TestFourReadOnlyWarnings:
    """全部 4 类只读告警均写入 diff [WARNINGS] 且输出 logger.warning，绝不阻断提交。"""

    def test_all_four_warnings_emitted_and_commit_succeeds(
        self, tmp_path: Path, backend_kind: str, caplog: pytest.LogCaptureFixture
    ) -> None:
        caplog.set_level(logging.WARNING, logger="tasklite")
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("prod", _ok_handler)
            p.register_task("cons", _ok_handler)

            # 构造基础状态：
            # - prod::del_ok (succeeded) -> 触发 Warning #1（删除 succeeded）
            # - prod::dual_part (pending + successes 残留) -> 触发 Warning #2（单分区 remove）
            # - prod::rerun_cand (pending + successes 残留) -> 触发 Warning #3（replace 后 pending 仍带历史档案）
            # - prod::dyn_pending (pending 无静态 files) + cons::dyn_ref -> 触发 Warning #4
            with p.edit(dry_run=False) as tx:
                tx.add(Job("prod", "del_ok"), status="succeeded")
                tx.add(Job("prod", "dual_part"), status="succeeded")
                tx.add(Job("prod", "rerun_cand"), status="succeeded")
                tx.add(Job("prod", "dyn_pending"), status="pending")
            # 让 dual_part 和 rerun_cand 同时在 queue 与 successes 并存（合法重跑候选态）
            p.enqueue(Job("prod", "dual_part"))
            p.enqueue(Job("prod", "rerun_cand"))

            caplog.clear()
            with p.edit(dry_run=False) as tx:
                # Warning #1
                tx.remove("prod::del_ok", recursive=False)
                # Warning #2：仅从 queue 删 dual_part，successes 仍保留
                assert tx.remove("prod::dual_part", entries=("queue",), recursive=False) == 1
                # Warning #3：replace rerun_cand（pending），不隐式清其 successes 残留
                tx.replace("prod::rerun_cand", Job("prod", "rerun_cand", payload={"new": 1}))
                # Warning #4：引用非 succeeded 上游未静态声明的 output
                tx.add(
                    Job(
                        "cons",
                        "dyn_ref",
                        depends_on=["prod::dyn_pending"],
                        files={"x": FileSpec.from_output("prod::dyn_pending", "runtime_key", role="input")},
                    ),
                    status="pending",
                )
                diff_txt = tx.diff()

            # 1. diff 包含全部 4 类告警且校验通过
            assert "[VALIDATION]\n  PASS" in diff_txt
            assert "[- REMOVED]\n  - [succeeded] prod::del_ok" in diff_txt
            warnings_section = diff_txt.split("[WARNINGS]")[1].split("[VALIDATION]")[0]
            assert "Removing completed [succeeded] job 'prod::del_ok'" in warnings_section
            assert "prod::dual_part" in warnings_section and "still remains in ['successes']" in warnings_section
            assert "Pending job 'prod::rerun_cand' also has historical ['successes'] archive" in warnings_section
            assert "relies on runtime ctx.declare_file" in warnings_section

            # 2. logger.warning 同样收到 4 条告警
            logged = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
            assert len(logged) == 4


# ── 五、遗留 running 补写、单次去重与未触碰保护 ─────────────────────────


class TestLegacyRunningRecoveryInEdit:
    """触碰遗留 running 补写失败 Attempt（同事务去重），未触碰行零扰动。"""

    @pytest.mark.parametrize(
        ("op", "expected_status"),
        [
            ("set_pending", "pending"),
            ("set_failed", "failed"),
            ("replace_inherit", "failed"),
            ("replace_pending", "pending"),
            ("remove_queue", None),
        ],
    )
    def test_touching_running_recovers_once_and_preserves_untouched_running(
        self, tmp_path: Path, backend_kind: str, op: str, expected_status: str | None
    ) -> None:
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            p.enqueue(Job("t", "target"))
            p.enqueue(Job("t", "untouched"))
            seed_running_job(p, "t::target", execution_id="r_old.1", run_id="r_old", started_at="2026-01-01T00:00:00Z")
            seed_running_job(p, "t::untouched", execution_id="r_old.2", run_id="r_old", started_at="2026-01-01T00:00:01Z")

            with p.edit(dry_run=False, note="recover running") as tx:
                if op == "set_pending":
                    tx.set_status("t::target", "pending", recursive=False)
                elif op == "set_failed":
                    tx.set_status("t::target", "failed", recursive=False)
                elif op == "replace_inherit":
                    tx.replace("t::target", Job("t", "target", payload={"v": 2}))
                elif op == "replace_pending":
                    tx.replace("t::target", Job("t", "target", payload={"v": 2}), status="pending")
                elif op == "remove_queue":
                    assert tx.remove("t::target", entries=("queue",), recursive=False) == 1
                after_snap = tx.after

            with StateSnapshot.open(p) as reopened:
                # tx.after 与落盘后重开快照 100% 同形
                assert _snapshot_dump(after_snap) == _snapshot_dump(reopened)

                # target 补写了恰好 1 条失败 Attempt，状态符合预期
                target_rec = reopened.get_job("t::target")
                if expected_status is None:
                    assert target_rec is None
                else:
                    assert target_rec is not None and target_rec["status"] == expected_status
                target_atts = list(reopened.iter_attempts(job_uid="t::target"))
                assert len(target_atts) == 1
                assert target_atts[0]["outcome"] == "failure"
                assert target_atts[0]["attempt_no"] == 1
                assert target_atts[0]["run_id"] == "r_old"

                # 未触碰的 untouched 保持 running，零扰动、无 Attempt
                untouched = reopened.get_job("t::untouched")
                assert untouched is not None and untouched["status"] == "running"
                assert list(reopened.iter_attempts(job_uid="t::untouched")) == []

    def test_multiple_touches_on_same_running_uid_deduplicate_to_one_attempt(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            p.enqueue(Job("t", "multi"))
            seed_running_job(
                p, "t::multi", execution_id="r1.1", run_id="r1", started_at="2026-01-01T00:00:00Z"
            )

            with p.edit(dry_run=False) as tx:
                tx.set_status("t::multi", "pending", recursive=False)
                tx.replace("t::multi", Job("t", "multi", payload={"step": 2}), status="pending")
                tx.set_status("t::multi", "failed", recursive=False)
                assert len(list(tx.after.iter_attempts(job_uid="t::multi"))) == 1

            with StateSnapshot.open(p) as snap:
                atts = list(snap.iter_attempts(job_uid="t::multi"))
                assert len(atts) == 1 and atts[0]["outcome"] == "failure"
                assert snap.get_job("t::multi")["status"] == "failed"  # type: ignore[index]

    def test_corrupted_running_identity_rejected_without_partial_mutation(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "corrupt.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p:
            p.register_task("t", _ok_handler)
            p.enqueue(Job("t", "bad"))
            seed_running_job(p, "t::bad", execution_id="r1.1", run_id="r1", started_at="2026-01-01T00:00:00Z")
            with closing(sqlite3.connect(db_path)) as conn:
                with conn:
                    conn.execute("UPDATE job_sequences SET last_attempt_no = 99 WHERE job_uid = 't::bad'")

            with p.edit(dry_run=False) as tx:
                with pytest.raises(TaskLiteError, match="Invariant conflict"):
                    tx.set_status("t::bad", "failed", recursive=False)
                # 工作图未留半成品变更
                assert tx.after.get_job("t::bad")["status"] == "running"  # type: ignore[index]


# ── 六、before/after/diff 退出后可读性、dry_run、锁互斥与守卫 ───────────


class TestSnapshotsDiffLifecycleAndMutex:
    """tx.before/after/diff 契约、dry_run 零写入、异常回滚与单写者互斥。"""

    def test_before_after_diff_readable_after_exit_and_pipeline_close(
        self, tmp_path: Path
    ) -> None:
        db_path = tmp_path / "iso.db"
        p = TaskLite(backend="sqlite", state_path=db_path)
        p.register_task("t", _ok_handler)
        p.enqueue(Job("t", "a", payload={"unchanged": "X" * 500, "ver": 1}))
        p.enqueue(Job("t", "b"))
        with in_process_workers(p):
            p.run(install_signals=False)

        with p.edit(dry_run=False, note="update a") as tx:
            tx.replace("t::a", Job("t", "a", payload={"unchanged": "X" * 500, "ver": 2}))
            tx.remove("t::b", recursive=False)
            diff_in_ctx = tx.diff()

        with StateSnapshot.open(db_path, mode="copy") as reopened:
            assert _snapshot_dump(tx.after) == _snapshot_dump(reopened)

        # 关闭 pipeline 后，tx.before / tx.after / tx.diff() 仍永久可读且不持有连接
        p.close()
        assert tx.before.get_job("t::a")["job"]["payload"]["ver"] == 1  # type: ignore[index]
        assert tx.before.get_job("t::b")["status"] == "succeeded"  # type: ignore[index]
        assert tx.after.get_job("t::a")["job"]["payload"]["ver"] == 2  # type: ignore[index]
        assert tx.after.get_job("t::b") is None
        assert tx.diff() == diff_in_ctx
        # [⇄ REPLACED] 仅显示变化的 payload 键，不刷屏 500 个 X
        assert "payload keys changed: ['ver']" in diff_in_ctx
        assert ("X" * 100) not in diff_in_ctx

        # 退出后调用任何写原语立即抛 TaskLiteError
        with pytest.raises(TaskLiteError, match="after EditTransaction has exited"):
            tx.add(Job("t", "c"), status="pending")
        with pytest.raises(TaskLiteError, match="after EditTransaction has exited"):
            tx.replace("t::a", Job("t", "a"))
        with pytest.raises(TaskLiteError, match="after EditTransaction has exited"):
            tx.replace_many({"t::a": Job("t", "a")})
        with pytest.raises(TaskLiteError, match="after EditTransaction has exited"):
            tx.set_status("t::a", "pending", recursive=False)
        with pytest.raises(TaskLiteError, match="after EditTransaction has exited"):
            tx.remove("t::a", recursive=False)

    def test_dry_run_default_makes_zero_disk_writes_and_exception_aborts_cleanly(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            p.enqueue(Job("t", "a"))

            # 默认 dry_run=True：不落盘，退出后 before/after 可对比
            with p.edit() as tx:
                assert isinstance(tx, EditTransaction)
                tx.add(Job("t", "b"), status="pending")
                tx.set_status("t::a", "failed", recursive=False)
                assert tx.after.get_job("t::b") is not None
                assert tx.after.get_job("t::a")["status"] == "failed"  # type: ignore[index]

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("t::a")["status"] == "pending"  # type: ignore[index]
                assert snap.get_job("t::b") is None

            # with 块内未捕获异常：跳过校验与提交，释放锁并传播原异常
            with pytest.raises(RuntimeError, match="user script aborted"):
                with p.edit(dry_run=False) as tx:
                    tx.add(Job("t", "orphan_child", depends_on=["t::missing"]), status="pending")
                    raise RuntimeError("user script aborted")

            # 锁已释放，后续 edit 可正常进入
            with p.edit(dry_run=False) as tx:
                tx.add(Job("t", "after_abort"), status="pending")
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("t::after_abort") is not None
                assert snap.get_job("t::orphan_child") is None

    def test_primitive_prevalidation_failure_leaves_zero_partial_mutation(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            with p.edit(dry_run=False) as tx:
                tx.add(Job("t", "a"), status="pending")
                tx.add(Job("t", "b"), status="pending")

            with p.edit(dry_run=True) as tx:
                # replace_many 第二项存在未注册 task_type，第一项不得留下半成品改动
                with pytest.raises(TaskLiteError, match="unknown task_type"):
                    tx.replace_many({
                        "t::a": Job("t", "a_new"),
                        "t::b": Job("unregistered", "b_new"),
                    })
                assert "t::a" in tx and "t::a_new" not in tx
                assert "t::b" in tx

                # replace_many 重复目标 UID 拒绝且零改动
                with pytest.raises(TaskLiteError, match="Duplicate target"):
                    tx.replace_many({
                        "t::a": Job("t", "same"),
                        "t::b": Job("t", "same"),
                    })
                assert "t::a" in tx and "t::b" in tx and "t::same" not in tx

                # __contains__ 非法 UID 抛 TaskLiteError
                with pytest.raises(TaskLiteError, match="canonical"):
                    _ = "bad_uid" in tx

    def test_exclusive_lock_mutex_and_guards(self, tmp_path: Path) -> None:
        # Memory 单写者互斥（嵌套 edit 立即拒绝）
        with TaskLite(backend="memory") as mem:
            mem.register_task("t", _ok_handler)
            with mem.edit(dry_run=True):
                with pytest.raises(TaskLiteError, match="concurrent"):
                    with mem.edit(dry_run=True):
                        pass

            # Run 期拒绝
            with running(mem):
                with pytest.raises(TaskLiteError, match="outside run"):
                    mem.edit()

            # 不可重复进入同一事务对象
            tx = mem.edit(dry_run=True)
            with tx:
                pass
            with pytest.raises(TaskLiteError, match="more than once"):
                with tx:
                    pass

        # SQLite 跨实例文件锁互斥
        db_path = tmp_path / "lock.db"
        with TaskLite(backend="sqlite", state_path=db_path) as p1:
            fd = try_acquire_lock(db_path, timeout=0)
            assert fd is not None
            try:
                with pytest.raises(TaskLiteError, match="Another run"):
                    with p1.edit(dry_run=True):
                        pass
            finally:
                release_lock(fd)
            with p1.edit(dry_run=True) as tx:
                assert list(tx.before.iter_jobs()) == []

        # 基准库存在 successes ∩ failed 重叠时立即释放锁并抛 TaskLiteError
        overlap_db = tmp_path / "overlap.db"
        with TaskLite(backend="sqlite", state_path=overlap_db) as p2:
            p2.register_task("t", _ok_handler)
            with p2.edit(dry_run=False) as tx:
                tx.add(Job("t", "dup"), status="succeeded")
            with closing(sqlite3.connect(overlap_db)) as conn:
                with conn:
                    conn.execute("INSERT INTO failed (job_uid, metadata) VALUES ('t::dup', '{}')")
            with pytest.raises(TaskLiteError, match="Invariant conflict"):
                with p2.edit(dry_run=True):
                    pass
            # 确认锁已释放（可立即重新获取文件锁）
            fd2 = try_acquire_lock(overlap_db, timeout=0)
            assert fd2 is not None
            release_lock(fd2)


# ── 七、异名替换清三表、显式 pending 清档案、路径即时规范与重入互斥 ─────────


class TestReplacePartitionsCanonicalPathsAndReentrancy:
    """覆盖双分区并存节点替换、规范路径块内可见与同实例 edit() 写重入互斥。"""

    def test_rename_dual_partition_job_clears_all_three_tables_and_enforces_orphan_check(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        """queue + successes 并存节点异名 replace 后旧 UID 彻底消失，未迁下游必报孤儿错。"""
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("step", _ok_handler)
            p.enqueue(Job("step", "old"))
            p.enqueue(Job("step", "child", depends_on=["step::old"]))
            with in_process_workers(p):
                p.run(install_signals=False)
            # 再次入队 old，使其同时存在于 queue 与 successes
            p.enqueue(Job("step", "old"))

            # 未迁移 child 的依赖时，异名替换 old -> new 必报孤儿错误且不残留旧幽灵
            with pytest.raises(TaskLiteError, match="lost existing dependency"):
                with p.edit(dry_run=False) as tx:
                    tx.replace("step::old", Job("step", "new"))
                    assert "step::old" not in tx
                    assert "step::new" in tx
                    assert tx.after.get_job("step::old") is None

            # 同步迁移 child 依赖后可干净提交，重开快照中彻底不存在 step::old
            with p.edit(dry_run=False) as tx:
                tx.replace("step::old", Job("step", "new"))
                tx.replace("step::child", Job("step", "child", depends_on=["step::new"]))
                assert "step::old" not in tx
                assert "step::new" in tx

            with StateSnapshot.open(p) as snap:
                assert snap.get_job("step::old") is None
                new_rec = snap.get_job("step::new")
                assert new_rec is not None and new_rec["status"] == "pending"
                child_rec = snap.get_job("step::child")
                assert child_rec is not None and child_rec["job"]["depends_on"] == ["step::new"]

    def test_explicit_pending_status_clears_residual_archive_while_none_preserves_it(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        """queue + successes 并存节点显式 status='pending' 清档案且重跑生效；status=None 保留并告警。"""
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("step", _ok_handler, trigger="once")
            p.enqueue(Job("step", "item", payload={"ver": 1}))
            with in_process_workers(p):
                p.run(install_signals=False)
            # 再次入队使其同时在 queue 与 successes 并存
            p.enqueue(Job("step", "item", payload={"ver": 1}))

            # 1) status=None 同名替换：保留历史 successes 档案并触发历史档案告警
            with p.edit(dry_run=True) as tx:
                tx.replace("step::item", Job("step", "item", payload={"ver": 2}))
                diff_inherit = tx.diff()
                assert "historical ['successes'] archive" in diff_inherit

            # 2) 显式 status='pending'：清空历史 successes 档案、无告警、下次 run() 真实执行新配置
            with p.edit(dry_run=False) as tx:
                tx.replace("step::item", Job("step", "item", payload={"ver": 2}), status="pending")
                diff_explicit = tx.diff()
                assert "historical ['successes'] archive" not in diff_explicit
                assert "[WARNINGS]\n  (none)" in diff_explicit

            with in_process_workers(p):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snap:
                rec = snap.get_job("step::item")
                assert rec is not None and rec["status"] == "succeeded"
                assert rec["result"]["metadata"]["payload"] == {"ver": 2}
                assert len(list(snap.iter_attempts(job_uid="step::item"))) == 2

    def test_replace_many_swap_with_dual_partition_nodes_leaves_no_ghost(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        """replace_many 互换含 queue + successes 并存的节点时无残留历史污染。"""
        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("step", _ok_handler, trigger="once")
            p.enqueue(Job("step", "alpha", payload={"origin": "alpha"}))
            p.enqueue(Job("step", "beta", payload={"origin": "beta"}))
            with in_process_workers(p):
                p.run(install_signals=False)
            # 让 alpha 和 beta 均进入 queue + successes 并存态
            p.enqueue(Job("step", "alpha", payload={"origin": "alpha"}))
            p.enqueue(Job("step", "beta", payload={"origin": "beta"}))

            with p.edit(dry_run=False) as tx:
                tx.replace_many({
                    "step::alpha": Job("step", "beta", payload={"from": "alpha"}),
                    "step::beta": Job("step", "alpha", payload={"from": "beta"}),
                })
                diff_txt = tx.diff()
                # 异名互换已清空双方三表，不应残留对方历史档案告警
                assert "historical" not in diff_txt

            with in_process_workers(p):
                p.run(install_signals=False)
            with StateSnapshot.open(p) as snap:
                rec_a = snap.get_job("step::alpha")
                rec_b = snap.get_job("step::beta")
                assert rec_a is not None and rec_a["status"] == "succeeded"
                assert rec_b is not None and rec_b["status"] == "succeeded"
                assert rec_a["result"]["metadata"]["payload"] == {"from": "beta"}
                assert rec_b["result"]["metadata"]["payload"] == {"from": "alpha"}

    def test_manual_succeeded_canonical_path_visible_in_context_and_no_false_diff(
        self, tmp_path: Path, backend_kind: str, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """相对路径与父目录软链在块内 tx.after 即规范化，且等价路径替换不触发 files 假阳性 diff。"""
        real_dir = tmp_path / "real_out"
        real_dir.mkdir()
        out_file = real_dir / "result.dat"
        out_file.write_text("payload", encoding="utf-8")
        alias_dir = tmp_path / "alias_out"
        alias_dir.symlink_to(real_dir, target_is_directory=True)

        monkeypatch.chdir(tmp_path)
        expected_canonical = str(out_file.resolve())

        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("step", _ok_handler)

            # 相对路径 + 父目录软链路径 add(..., status="succeeded")
            with p.edit(dry_run=False) as tx:
                tx.add(
                    Job("step", "rel", files={"out": FileSpec("real_out/result.dat", role="output")}),
                    status="succeeded",
                )
                tx.add(
                    Job("step", "symparent", files={"out": FileSpec(alias_dir / "result.dat", role="output")}),
                    status="succeeded",
                )
                # 块内首次读取 tx.after 即为规范绝对路径，无退出后突变
                rel_in_ctx = tx.after.get_job("step::rel")
                sym_in_ctx = tx.after.get_job("step::symparent")
                assert rel_in_ctx is not None
                assert sym_in_ctx is not None
                assert rel_in_ctx["files"]["out"]["path"] == expected_canonical
                assert sym_in_ctx["files"]["out"]["path"] == expected_canonical

            assert tx.after.get_job("step::rel")["files"]["out"]["path"] == expected_canonical  # type: ignore[index]
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("step::rel")["files"]["out"]["path"] == expected_canonical  # type: ignore[index]
                assert snap.get_job("step::symparent")["files"]["out"]["path"] == expected_canonical  # type: ignore[index]

            # 基准为规范绝对路径，替换为指向同一物理文件的相对路径时 diff 不误报 files 变更
            with p.edit(dry_run=False) as tx:
                tx.add(
                    Job("step", "abs_base", files={"out": FileSpec(expected_canonical, role="output")}),
                    status="succeeded",
                )
            with p.edit(dry_run=True) as tx:
                tx.replace(
                    "step::abs_base",
                    Job("step", "abs_base", files={"out": FileSpec("real_out/result.dat", role="output")}),
                )
                # 块内 tx.after 已规范化
                assert tx.after.get_job("step::abs_base")["files"]["out"]["path"] == expected_canonical  # type: ignore[index]
                diff_txt = tx.diff()
                assert "files{" not in diff_txt
                assert "(no field changes)" in diff_txt

    def test_editing_guard_blocks_reentrant_pipeline_apis_and_recovers_after_exit(
        self, tmp_path: Path, backend_kind: str
    ) -> None:
        """with pipeline.edit() 块内调用任意同实例读写/注册/运行/关闭接口必抛 TaskLiteError 且零写入。"""
        from tasklite.resource import CapacityResource

        auth_target = tmp_path / "auth_dir"
        auth_target.mkdir()

        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            p.set_cursor("seed", "v0")
            p.grant_authorization(auth_target, target_type="directory")

            with p.edit(dry_run=True):
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.enqueue(Job("t", "blocked"))
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.set_cursor("seed", "v1")
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.get_cursor("seed")
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.grant_authorization(tmp_path / "other", target_type="file")
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.list_authorizations()
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.revoke_authorization(auth_target)
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.register_task("t_extra", _ok_handler)
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.register_resource(CapacityResource("gpu", max_capacity=1))
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.register_trigger("custom_trig", lambda ctx: True)
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.run(install_signals=False)
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.close()
                with pytest.raises(TaskLiteError, match="edit\\(\\) transaction is active"):
                    p.edit(dry_run=True)

            # 正常退出后恢复可用，且块内被拒调用均零写入
            assert p.get_cursor("seed") == "v0"
            assert len(p.list_authorizations()) == 1
            with StateSnapshot.open(p) as snap:
                assert snap.get_job("t::blocked") is None

            # 异常退出后同样复位 _editing，实例恢复可用
            with pytest.raises(RuntimeError, match="boom"):
                with p.edit(dry_run=True):
                    raise RuntimeError("boom")

            assert p.enqueue(Job("t", "after_exception")) is True
            p.set_cursor("seed", "v2")
            assert p.get_cursor("seed") == "v2"

    @pytest.mark.parametrize("backend_kind", ["sqlite", "memory"])
    def test_diff_is_pure_read_only_without_mutating_work_or_frozen_after(
        self, backend_kind, tmp_path
    ):
        """diff() 预览严格只读：块内与退出后调用均不得原地改写工作图或使冻结 tx.after 失效。"""
        out_file = tmp_path / "cached.txt"
        out_file.write_text("ok", encoding="utf-8")

        with _make_pipeline(backend_kind, tmp_path) as p:
            p.register_task("t", _ok_handler)
            with p.edit(dry_run=True) as tx:
                tx.add(
                    Job("t", "manual_ok", files={"out": FileSpec(out_file, role="output")}),
                    status="succeeded",
                )
                work_before_diff = copy.deepcopy(tx._work.successes)
                cached_after_before = tx.after
                _ = tx.diff()
                assert tx._work.successes == work_before_diff
                assert tx.after is cached_after_before

            # 退出后冻结的 tx.after 稳定可读，再次调用 diff() 仍不改写工作图或破坏冻结缓存
            frozen_after = tx.after
            work_after_exit = copy.deepcopy(tx._work.successes)
            version_before = tx._logged_warnings_version
            _ = tx.diff()
            assert tx.after is frozen_after
            assert tx._work.successes == work_after_exit
            assert tx._logged_warnings_version == version_before
