"""单元测试显式使用的轻量进程替身。

替身仍在真实单向 Pipe 边界发送 WorkerLaunchSpec 结果；仅进程生命周期由
测试同步控制。真实 spawn、EOF、退出码与超时路径必须使用原生产上下文。
"""

from __future__ import annotations

import multiprocessing as mp

from tasklite.pipeline import TaskLite


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
