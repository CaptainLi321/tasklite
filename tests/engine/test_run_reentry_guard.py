"""run() 防重入守卫回归测试。

不变式：同一实例 run() 执行期间再次 run() 必须 fail-loud 拒绝
（RuntimeError），run() 结束后运行标志复位、同实例可正常复跑。
并发 run() 的防重入此前仅靠调用纪律，无回归护栏。
"""

import threading

import pytest

from tasklite.models.job import Job

from tests.helpers import make_pipeline, patch_multiprocessing_for_fakes


def _fast_handler(job, ctx):
    return True


def _make_gate_process_class(enter_event, release_event):
    """FakeProcess：start() 即返回并置位 enter_event；release_event 置位后
    写成功结果并退出——供测试精确控制「执行体活动期」窗口。"""

    from tests.helpers import _write_fake_result

    class GateProcess:
        def __init__(self, target=None, args=(), kwargs=None, **_kw):
            self.args = args
            self._alive = False
            self._written = False
            self.exitcode = 0

        def start(self):
            self._alive = True
            enter_event.set()

        def _complete_if_released(self):
            if not release_event.is_set() or self._written:
                return
            self._written = True
            if self.args:
                _spec = self.args[0]
                _write_fake_result(_spec.ipc_dir, _spec.job.uid, {
                    "status": "success",
                    "raw_result": True,
                    "new_jobs": [],
                    "resource_suspensions": [],
                    "cursor_updates": {},
                }, incarnation=_spec.incarnation, auth_token=getattr(_spec, "result_token", None))
            self._alive = False

        def is_alive(self):
            self._complete_if_released()
            return self._alive

        def join(self, timeout=None):
            self._complete_if_released()

        def kill(self):
            self._alive = False
            self.exitcode = -9

    return GateProcess


class TestRunReentryGuard:
    def test_reentrant_run_rejected_and_reset_after(self, tmp_path, monkeypatch):
        """执行期间二次 run() 必须 RuntimeError；结束后标志复位且可复跑。"""
        enter, release = threading.Event(), threading.Event()
        patch_multiprocessing_for_fakes(
            monkeypatch,
            fake_process_class=_make_gate_process_class(enter, release),
        )
        p = make_pipeline(tmp_path)
        p.register_handler("fast", _fast_handler)
        p.enqueue([Job("fast", "j1", payload={})])

        run_errors = []

        def run_once():
            try:
                p.run()
            except BaseException as e:  # 正常路径不应抛，兜底记录便于诊断
                run_errors.append(e)

        t = threading.Thread(target=run_once)
        t.start()
        try:
            assert enter.wait(10), "前置：run() 应已派发执行体进入活动期"
            assert p._runtime.is_running, "run() 执行期间 is_running 必须为真"

            # 执行期间同实例再次 run() 必须拒绝（重入守卫）
            with pytest.raises(RuntimeError, match=r"^Pipeline run\(\) already in progress"):
                p.run()
        finally:
            release.set()
        t.join(timeout=15)
        assert not t.is_alive(), "首次 run() 应在放行后正常结束"
        assert run_errors == [], f"首次 run() 不得异常: {run_errors}"
        assert not p._runtime.is_running, "run() 结束后运行标志必须复位"

        # 结束后恢复正常：同实例可再次 run() 并完成队列
        p.run()
        assert "fast::j1" in p.backend.load_wall(), "复跑必须正常完成作业"
