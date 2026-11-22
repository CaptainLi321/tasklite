"""隔离子进程两阶段引导入口与 Fork-Bomb 重入守卫。

由 ``SubprocessWorkerLauncher`` 以 ``python -m tasklite.engine._worker_bootstrap`` 启动，
仅导入纯 worker 侧模块，严格执行两阶段握手协议：
1. 立即清除私有 IPC 环境变量并屏蔽停止信号；
2. 依次向父进程 ``Listener`` 建立 ``uplink`` 与 ``downlink`` 双物理连接；
3. 阻塞接收预序列化的 ``prep_bytes`` 与 ``payload_bytes``（仅缓存字节，不导入用户代码）；
4. 阻塞等待父进程 ``b"GO"`` 放行门（此时进程树绑定已零竞态完成）；
5. 放行后置位 ``_IMPORTING_MP_MAIN`` 重入守卫，还原 ``sys.path`` / ``cwd`` / ``__mp_main__``，
   反序列化执行载荷并进入 ``run_worker_entry``；引导期任何故障经 ``uplink`` 回传
   结构化 ``failure`` 帧，绝不静默退出。
"""

from __future__ import annotations

import os
import pickle
import runpy
import sys
import types
from multiprocessing.connection import Client
from typing import Any

from ..utils.ipc import (
    HANDSHAKE_ROLE_DOWNLINK,
    HANDSHAKE_ROLE_UPLINK,
    WorkerPipeSender,
    WorkerReplyReceiver,
    decode_json_object_frame,
    encode_handshake_frame,
)
from ._worker_runtime import (
    WorkerLaunchSpec,
    build_exception_dict,
    build_failure_result_frame,
    ignore_worker_stop_signals,
    run_worker_entry,
)
from .types import (
    ENV_IPC_ADDR,
    ENV_IPC_AUTHKEY,
    ENV_IPC_FAMILY,
    LAUNCH_GO_TOKEN,
    _set_importing_mp_main,
)


def _import_mp_main_from_name(mod_name: str) -> None:
    """按模块名在 ``__mp_main__`` 命名空间中导入主模块并绑定到 ``__main__``。"""
    module_globals = runpy.run_module(mod_name, run_name="__mp_main__", alter_sys=False)
    mp_main = types.ModuleType("__mp_main__")
    mp_main.__dict__.update(module_globals)
    sys.modules["__mp_main__"] = mp_main
    sys.modules["__main__"] = mp_main


def _import_mp_main_from_path(main_path: str) -> None:
    """按脚本路径在 ``__mp_main__`` 命名空间中导入主模块并绑定到 ``__main__``。"""
    module_globals = runpy.run_path(main_path, run_name="__mp_main__")
    mp_main = types.ModuleType("__mp_main__")
    mp_main.__dict__.update(module_globals)
    sys.modules["__mp_main__"] = mp_main
    sys.modules["__main__"] = mp_main


def _restore_environment(prep_data: dict[str, Any]) -> None:
    """还原父进程 ``sys.path``、工作目录与 ``__mp_main__`` 模块环境。"""
    sys_path = prep_data.get("sys_path")
    if isinstance(sys_path, list):
        sys.path[:] = [str(entry) for entry in sys_path]

    cwd = prep_data.get("cwd")
    if isinstance(cwd, str) and cwd:
        os.chdir(cwd)

    init_from_name = prep_data.get("init_main_from_name")
    init_from_path = prep_data.get("init_main_from_path")
    if not init_from_name and not init_from_path:
        return

    _set_importing_mp_main(True)
    try:
        if isinstance(init_from_name, str) and init_from_name:
            _import_mp_main_from_name(init_from_name)
        elif isinstance(init_from_path, str) and init_from_path:
            _import_mp_main_from_path(init_from_path)
    finally:
        _set_importing_mp_main(False)


def _send_bootstrap_failure(
    pipe_sender: WorkerPipeSender,
    exc: BaseException,
) -> None:
    """将引导或反序列化失败封装为结构化 failure 帧回传主进程。"""
    hint = "Task handler 须定义在模块顶层，不得置于 `if __name__ == '__main__':` 内"
    message = f"Worker bootstrap failed ({type(exc).__name__}: {exc}). {hint}"
    payload = build_failure_result_frame(
        message,
        exception=build_exception_dict(exc),
    )
    try:
        pipe_sender.send_result(payload)
    except OSError:
        pass


def _bootstrap_main() -> None:
    """子进程引导主流程。"""
    addr = os.environ.pop(ENV_IPC_ADDR, None)
    family = os.environ.pop(ENV_IPC_FAMILY, None)
    authkey_hex = os.environ.pop(ENV_IPC_AUTHKEY, None)
    if not addr or not family or not authkey_hex:
        sys.stderr.write("tasklite worker bootstrap missing required IPC environment variables\n")
        sys.exit(2)

    ignore_worker_stop_signals()

    try:
        authkey = bytes.fromhex(authkey_hex)
    except ValueError:
        sys.stderr.write("tasklite worker bootstrap received invalid IPC authkey\n")
        sys.exit(2)

    uplink = None
    downlink = None
    try:
        uplink = Client(address=addr, family=family, authkey=authkey)
        uplink.send_bytes(
            encode_handshake_frame(HANDSHAKE_ROLE_UPLINK, pid=os.getpid())
        )

        downlink = Client(address=addr, family=family, authkey=authkey)
        downlink.send_bytes(
            encode_handshake_frame(HANDSHAKE_ROLE_DOWNLINK, pid=os.getpid())
        )

        # 第一阶段：仅缓存预序列化字节，等待父进程完成进程树绑定与放行门校验
        prep_bytes = downlink.recv_bytes()
        payload_bytes = downlink.recv_bytes()
        gate = downlink.recv_bytes()
    except (EOFError, OSError):
        for conn in (downlink, uplink):
            if conn is not None:
                try:
                    conn.close()
                except OSError:
                    pass
        sys.exit(0)

    if gate != LAUNCH_GO_TOKEN:
        for conn in (downlink, uplink):
            try:
                conn.close()
            except OSError:
                pass
        sys.exit(0)

    try:
        prep_data = decode_json_object_frame(prep_bytes, context="bootstrap prep_data")
    except Exception as exc:
        sys.stderr.write(f"tasklite worker bootstrap failed to decode prep_data: {exc}\n")
        for conn in (downlink, uplink):
            try:
                conn.close()
            except OSError:
                pass
        sys.exit(2)

    job_uid = str(prep_data.get("job_uid", ""))
    execution_id = str(prep_data.get("execution_id", ""))
    run_id = prep_data.get("run_id")
    if run_id is not None:
        run_id = str(run_id)
    attempt_no = prep_data.get("attempt_no")
    if not isinstance(attempt_no, int) or isinstance(attempt_no, bool):
        attempt_no = None

    pipe_sender = WorkerPipeSender(
        uplink,
        job_uid=job_uid,
        execution_id=execution_id,
        run_id=run_id,
        attempt_no=attempt_no,
    )
    reply_receiver = WorkerReplyReceiver(
        downlink,
        job_uid=job_uid,
        execution_id=execution_id,
        run_id=run_id,
        attempt_no=attempt_no,
    )

    try:
        _restore_environment(prep_data)
        spec_payload = pickle.loads(payload_bytes)
        launch_spec = WorkerLaunchSpec(
            handler=spec_payload["handler"],
            job=spec_payload["job"],
            job_context=spec_payload["job_context"],
            execution_id=spec_payload["execution_id"],
            timeout_seconds=spec_payload["timeout_seconds"],
            pipe_sender=pipe_sender,
            reply_receiver=reply_receiver,
            run_id=spec_payload["run_id"],
            attempt_no=spec_payload["attempt_no"],
        )
    except BaseException as exc:
        _send_bootstrap_failure(pipe_sender, exc)
        pipe_sender.close()
        reply_receiver.close()
        return

    run_worker_entry(launch_spec)


if __name__ == "__main__":
    # 通过正式包路径运行主流程，保证 _IMPORTING_MP_MAIN 与引擎侧引用同一模块实例。
    from tasklite.engine._worker_bootstrap import _bootstrap_main as _run_bootstrap

    _run_bootstrap()
