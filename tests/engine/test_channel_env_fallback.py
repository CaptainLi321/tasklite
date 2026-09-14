"""ExecutionChannel env 兜底 ipc_dir 模式回归测试。

ExecutionChannel 以 ipc_dir=None 构造时，spawn 按 spec → 实例属性 →
TASKLITE_IPC_DIR 环境变量顺序兜底解析执行目录。解析成功必须回写实例
属性：journal 构造、孤儿锁探测、信号排空等收割路径全部以实例属性为
事实源——若仅 handle 携带 env 目录，spawn 成功而 reap/probe/drain 全部
崩溃（实例属性与 handle 两套真相源）。
"""

from tasklite.engine.channel import ExecutionChannel, WorkerLaunchSpec
from tasklite.models.context import TaskContext
from tasklite.models.job import Job
from tasklite.utils.ipc import ArtifactJournal

_INCARNATION = "deadbeefdeadbeefdeadbeefdeadbeef.1"


class _ResultWritingProcess:
    """start() 时经 spec.task_ctx.ipc_dir 写 success 结果的进程桩。

    spawn 已把兜底解析出的执行目录写入 task_ctx，桩与真实子进程共享
    同一落盘代码路径。
    """

    def __init__(self, target=None, args=(), kwargs=None, **_kw):
        self.args = args
        self._alive = False
        self.exitcode = 0

    def start(self):
        spec = self.args[0]
        self._alive = True
        ArtifactJournal(spec.task_ctx.ipc_dir).write_result_atomic(
            spec.job.uid,
            {
                "status": "success",
                "raw_result": True,
                "new_jobs": [],
                "resource_suspensions": [],
                "cursor_updates": {},
            },
            incarnation=spec.incarnation,
        )

    def join(self, timeout=None):
        self._alive = False

    def is_alive(self):
        return self._alive

    def kill(self):
        self._alive = False


class TestEnvFallbackIpcDirCoherence:
    """env 兜底目录必须在 spawn 期回写实例属性，收割路径同源可用。"""

    def test_env_fallback_spawn_reap_probe_share_same_ipc_dir(
        self, tmp_path, monkeypatch
    ):
        monkeypatch.setenv("TASKLITE_IPC_DIR", str(tmp_path))
        fake_ctx = type("Ctx", (), {"Process": _ResultWritingProcess})()
        channel = ExecutionChannel(mp_ctx=fake_ctx)

        job = Job("t", "x")
        spec = WorkerLaunchSpec(
            handler=lambda j, c: (True, {}),
            job=job,
            task_ctx=TaskContext(job, set(), set(), {}),
            incarnation=_INCARNATION,
            ipc_dir=None,
            timeout=60.0,
        )
        handle = channel.spawn(spec)

        # 实例属性、journal 与 handle 派发路径同源
        assert channel.ipc_dir == str(tmp_path), (
            "env 兜底解析成功后必须回写实例属性（收割路径的事实源）"
        )
        assert handle.ipc_dir == str(tmp_path)
        assert channel.journal.ipc_dir == str(tmp_path)

        # reap/probe/drain 全链路可用（不再抛 ValueError/TypeError）
        completed = channel.reap_completed([handle])
        assert [(h.uid, r.success) for h, r in completed] == [("t::x", True)]
        assert channel.probe_orphan_lock("t::x") is True
        assert channel.drain_all_signals() == []
