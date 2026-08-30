"""文件级 IPC 的写入侧：输入/输出/信号声明的路径构造与追加落盘。

三个 ``append_*`` 与 ``*_path`` 构造函数原属 engine/executor；
``TaskContext``（models 层）在声明 API 中调用它们落盘——为消除
models→engine 的上层引用，下沉到 utils（仅依赖 jsonutil/lockfile，
无反向依赖）。executor 侧的读取函数（``read_inputs``/
``read_outputs``/``read_signals``）经本模块的路径构造函数共享同一
文件名规则（``{safe_uid_filename(uid)}.{inputs|outputs|signals}.jsonl``）。
"""

from pathlib import Path

from .jsonutil import dumps
from .lockfile import safe_uid_filename

# 声明/信号文件的扩展名
_SIGNALS_SUFFIX = ".signals.jsonl"
_OUTPUTS_SUFFIX = ".outputs.jsonl"
_INPUTS_SUFFIX = ".inputs.jsonl"


def signals_path(ipc_dir, uid: str) -> Path:
    """某个 job 的 suspend 信号文件路径（追加 JSONL）。"""
    return Path(ipc_dir) / f"{safe_uid_filename(uid)}{_SIGNALS_SUFFIX}"


def outputs_path(ipc_dir, uid: str) -> Path:
    """某个 job 的已声明输出文件路径（追加 JSONL）。"""
    return Path(ipc_dir) / f"{safe_uid_filename(uid)}{_OUTPUTS_SUFFIX}"


def inputs_path(ipc_dir, uid: str) -> Path:
    """输入声明的落盘路径：``{uid}.inputs.jsonl``。"""
    return Path(ipc_dir) / f"{safe_uid_filename(uid)}{_INPUTS_SUFFIX}"


def append_input(ipc_dir, uid: str, entry: dict) -> None:
    """追加一条输入声明到落盘文件（handler 子进程内调用）。

    entry 形如 ``{"path": ..., "kind": "file", "size": ..., "mtime_ns": ...}``
    或 ``{"path": ..., "kind": "uri", "uri_fingerprint": ...}``。失败静默
    （输入声明只影响可追溯性与 on_input_change 比对，丢失不破坏执行）。
    """
    path = inputs_path(ipc_dir, uid)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(dumps(entry) + "\n")
            f.flush()
    except OSError:
        pass


def append_output(ipc_dir, uid: str, out_path: str, cleanup: bool, kind: str = "output") -> None:
    """追加一条输出声明到落盘文件（handler 子进程内调用）。

    失败静默（尽力而为）：声明丢失不仅影响失败清理（输出残留），还影响
    **成功路径的输出存在性校验**（executor 侧读 outputs.jsonl 校验
    handler 声明的产物）——写入失败通常伴随更严重的磁盘故障（结果文件
    同样写不成功 → NO_IPC_RESULT 可见失败），fail-loud 会改变 handler
    语义（声明失败 = 任务失败），故保持静默。

    ``kind`` 区分 ``"output"``（最终产物：成功校验存在、失败按 cleanup
    删除）与 ``"cache"``（临时文件：成功跳过存在性校验且尝试删除、
    失败无条件删除）。
    """
    path = outputs_path(ipc_dir, uid)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(dumps(
                {"path": out_path, "cleanup": bool(cleanup), "kind": kind},
            ) + "\n")
            f.flush()
    except OSError:
        pass


def append_signal(ipc_dir, uid: str, r_name: str, secs: float) -> None:
    """追加一条 suspend 信号到 signals 文件（进程死文件仍在，不丢）。"""
    path = signals_path(ipc_dir, uid)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as f:
            f.write(dumps({"suspend": [r_name, secs]}) + "\n")
            f.flush()
    except OSError:
        pass  # 尽力而为：信号丢失不致命（resource_suspensions 兜底通道）
