"""单元测试显式使用的轻量进程替身。

替身仍在真实单向 Pipe 边界发送 WorkerLaunchSpec 结果；仅进程生命周期由
测试同步控制。真实 spawn、EOF、退出码与超时路径必须使用原生产上下文。
"""

from __future__ import annotations

import multiprocessing as mp
from typing import Any, Callable, Iterable

from tasklite.pipeline import TaskLite


def _send_result(spec: Any, payload: dict[str, Any]) -> None:
    sender = spec.pipe_sender
    if sender is None:
        raise AssertionError("fake worker 需要 WorkerLaunchSpec.pipe_sender")
    sender.send_result(payload)


def make_pipe_process_class(
    results: Iterable[dict[str, Any] | None] = (),
    *,
    exitcode: int = 0,
    stay_alive: bool = False,
    on_start: Callable[[Any], None] | None = None,
) -> type:
    """创建按 Pipe 结果序列驱动的进程替身。

    ``results`` 中 ``None`` 表示本次启动不发送结果；最后一项供后续启动重复
    使用。``stay_alive`` 用于固定“结果已到、进程尚未退出”的观察窗口。
    """
    sequence = list(results)
    calls = [0]

    class PipeProcess:
        call_count = calls

        def __init__(self, target=None, args=(), kwargs=None, **_kwargs):
            self.args = args
            self._alive = False
            self.exitcode = exitcode

        def start(self) -> None:
            self._alive = stay_alive
            if not self.args:
                return
            spec = self.args[0]
            if on_start is not None:
                on_start(spec)
            if not sequence:
                return
            index = min(calls[0], len(sequence) - 1)
            calls[0] += 1
            payload = sequence[index]
            if payload is not None:
                _send_result(spec, payload)

        def join(self, timeout=None) -> None:
            if not stay_alive:
                self._alive = False

        def is_alive(self) -> bool:
            return self._alive

        def kill(self) -> None:
            self._alive = False
            self.exitcode = -9

        def close(self) -> None:
            self._alive = False

    return PipeProcess


class FakeSpawnContext:
    """只替换 Process，Pipe 仍由真实 spawn 上下文创建。"""

    def __init__(self, process_class: type) -> None:
        self._context = mp.get_context("spawn")
        self.Process = process_class

    def Pipe(self, *args, **kwargs):
        return self._context.Pipe(*args, **kwargs)


def install_fake_process_context(pipeline: TaskLite, process_class: type) -> None:
    """为单个 TaskLite 实例显式安装进程替身。"""
    context = FakeSpawnContext(process_class)
    pipeline._process_context = context
    pipeline._channel._process_context = context
