"""文件级 IPC 与产物清单管理：输入/输出/信号声明、沙盒校验、指纹采集与生命周期清理。

统一内敛文件级 IPC 与产物清单的完整生命周期管理：
- 路径与沙盒校验：跨盘多根沙盒匹配、空字节防御、相对路径规范重定位；
- 声明追加落盘：输入（文件 stat 指纹 / URI 声明）、输出（产物 / 临时 cache 声明）、挂起信号；
- 校验与排空读取：产物存在性校验（忽略 cache）、信号原子排空（truncate + unlink）；
- 产物生命周期清理：按 PRE_SUBMIT / SUCCESS / FAILURE_OR_RETRY 模式清理临时文件与声明。
"""

from __future__ import annotations

import enum
import json
import logging
import os
from pathlib import Path
import shutil
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

from .jsonutil import dumps, loads
from .lockfile import safe_uid_filename

logger = logging.getLogger("tasklite")

# 声明/信号文件的扩展名
_SIGNALS_SUFFIX = ".signals.jsonl"
_OUTPUTS_SUFFIX = ".outputs.jsonl"
_INPUTS_SUFFIX = ".inputs.jsonl"


class ArtifactCleanupMode(str, enum.Enum):
    """产物生命周期清理模式。"""
    PRE_SUBMIT = "pre_submit"
    SUCCESS = "success"
    FAILURE_OR_RETRY = "failure_retry"


class ArtifactJournal:
    """产物清单与文件级 IPC 深模块。

    对外提供极简高层操作，封装沙盒越界防御、stat 指纹采集、坏行容灾解析、
    原子清空与多模式文件清理逻辑。
    """

    def __init__(self, ipc_dir: Optional[Union[str, Path]] = None) -> None:
        self.ipc_dir: Optional[str] = str(ipc_dir) if ipc_dir is not None else None

    # ── 路径构造 ──────────────────────────────────────────────────

    def signals_path(self, uid: str) -> Path:
        """某个 job 的 suspend 信号文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build signals_path")
        return Path(self.ipc_dir) / f"{safe_uid_filename(uid)}{_SIGNALS_SUFFIX}"

    def outputs_path(self, uid: str) -> Path:
        """某个 job 的已声明输出文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build outputs_path")
        return Path(self.ipc_dir) / f"{safe_uid_filename(uid)}{_OUTPUTS_SUFFIX}"

    def inputs_path(self, uid: str) -> Path:
        """输入声明的落盘路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build inputs_path")
        return Path(self.ipc_dir) / f"{safe_uid_filename(uid)}{_INPUTS_SUFFIX}"

    # ── 路径沙盒与规范化 ──────────────────────────────────────────

    @staticmethod
    def resolve_and_validate_path(
        raw_path: Union[str, Path],
        output_roots: Optional[Union[Path, Sequence[Path]]] = None,
        sandbox: bool = True,
    ) -> str:
        """解析声明路径为规范绝对路径（沙盒校验 + 相对重定位共用逻辑）。"""
        raw = str(raw_path)
        if "\x00" in raw:
            raise ValueError(f"Output path contains a null byte: {raw!r}")

        if output_roots is not None and sandbox:
            p = Path(raw)
            roots = (
                output_roots
                if isinstance(output_roots, (list, tuple))
                else [output_roots]
            )
            if not p.is_absolute():
                # 相对路径按第一个根重定位
                p = roots[0] / p
            resolved_path = Path(os.path.abspath(str(p)))
            try:
                resolved_path = resolved_path.resolve()
            except (OSError, RuntimeError):
                logger.debug(f"Could not resolve output path '{raw}', using abspath fallback.")
            if not any(resolved_path.is_relative_to(r) for r in roots):
                raise ValueError(
                    f"Output path '{raw}' resolves outside output_root {roots!r}"
                )
            return str(resolved_path)

        p = Path(raw)
        try:
            if p.parent.exists():
                return str(p.resolve())
            return os.path.abspath(str(p))
        except (OSError, RuntimeError):
            return os.path.abspath(str(p))

    # ── 声明追加写入（子进程 Worker 侧）───────────────────────────

    def record_output(
        self, uid: str, out_path: str, cleanup: bool = True, kind: str = "output"
    ) -> None:
        """追加一条输出声明到落盘文件（失败静默，尽力而为）。"""
        if self.ipc_dir is None:
            return
        path = self.outputs_path(uid)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(
                    dumps(
                        {"path": out_path, "cleanup": bool(cleanup), "kind": kind}
                    )
                    + "\n"
                )
                f.flush()
        except OSError:
            pass

    def record_input_entry(self, uid: str, entry: dict) -> None:
        """追加一条输入声明条目到落盘文件。"""
        if self.ipc_dir is None:
            return
        path = self.inputs_path(uid)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(dumps(entry) + "\n")
                f.flush()
        except OSError:
            pass

    def record_input_file(self, uid: str, resolved_path: str) -> dict:
        """采集文件 stat 指纹并追加输入声明。"""
        entry: dict = {"path": resolved_path, "kind": "file"}
        try:
            st = os.stat(resolved_path)
            entry["size"] = st.st_size
            entry["mtime_ns"] = st.st_mtime_ns
        except OSError:
            pass
        self.record_input_entry(uid, entry)
        return entry

    def record_input_uri(
        self, uid: str, url: str, uri_fingerprint: Optional[str] = None
    ) -> dict:
        """记录 URI 输入声明。"""
        entry: dict = {"path": url, "kind": "uri"}
        if uri_fingerprint is not None:
            entry["uri_fingerprint"] = uri_fingerprint
        self.record_input_entry(uid, entry)
        return entry

    def record_signal(self, uid: str, r_name: str, secs: float) -> None:
        """追加一条 suspend 信号到 signals 文件。"""
        if self.ipc_dir is None:
            return
        path = self.signals_path(uid)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(dumps({"suspend": [r_name, secs]}) + "\n")
                f.flush()
        except OSError:
            pass

    # ── 读取与解析（父进程 Engine 侧）─────────────────────────────

    def read_inputs(self, uid: str) -> List[dict]:
        """读取一个 job 的全部输入声明。"""
        if self.ipc_dir is None:
            return []
        path = self.inputs_path(uid)
        entries: List[dict] = []
        try:
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = loads(line)
                            if isinstance(data, dict) and isinstance(data.get("path"), str):
                                entries.append(data)
                        except (json.JSONDecodeError, TypeError, ValueError):
                            continue
        except OSError:
            pass
        return entries

    def read_outputs(self, uid: str) -> List[Tuple[str, bool, str]]:
        """读取一个 job 的全部已声明输出 (path, cleanup, kind)。"""
        if self.ipc_dir is None:
            return []
        path = self.outputs_path(uid)
        outputs: List[Tuple[str, bool, str]] = []
        try:
            if path.exists():
                with open(path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = loads(line)
                            if isinstance(data, dict) and isinstance(data.get("path"), str):
                                kind = data.get("kind")
                                if not isinstance(kind, str):
                                    continue
                                outputs.append((
                                    data["path"],
                                    bool(data.get("cleanup", True)),
                                    kind,
                                ))
                        except (json.JSONDecodeError, TypeError, ValueError):
                            continue
        except OSError:
            pass
        return outputs

    def drain_signals(self, uid: str) -> List[Tuple[str, float]]:
        """读取并删除一个 job 的 suspend 信号文件（排空语义）。"""
        if self.ipc_dir is None:
            return []
        path = self.signals_path(uid)
        signals: List[Tuple[str, float]] = []
        try:
            if path.exists():
                with open(path, "r+", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            data = loads(line)
                            if isinstance(data, dict) and "suspend" in data:
                                r_name, secs = data["suspend"]
                                signals.append((r_name, float(secs)))
                        except (json.JSONDecodeError, TypeError, ValueError):
                            continue
                    try:
                        f.seek(0)
                        f.truncate(0)
                    except OSError:
                        pass
                try:
                    path.unlink()
                except (FileNotFoundError, OSError):
                    pass
        except OSError:
            pass
        return signals

    # ── 产物校验与生命周期清理 ────────────────────────────────────

    def verify_outputs(self, uid: str) -> Tuple[bool, Optional[str]]:
        """校验 job 的产物是否存在（忽略临时 cache）。

        Returns:
            (True, None) 表示校验通过；(False, error_msg) 表示产物缺失。
        """
        for out_path, _, kind in self.read_outputs(uid):
            if kind == "cache":
                continue
            if not Path(out_path).exists():
                return False, f"Missing output {out_path}"
        return True, None

    def cleanup(self, uid: str, mode: Union[str, ArtifactCleanupMode]) -> None:
        """根据清理模式对产物文件与声明文件进行生命周期清理。"""
        if self.ipc_dir is None:
            return

        mode_val = mode.value if hasattr(mode, "value") else str(mode)

        if mode_val == ArtifactCleanupMode.PRE_SUBMIT.value:
            for p in (self.inputs_path(uid), self.outputs_path(uid), self.signals_path(uid)):
                try:
                    if p.exists():
                        p.unlink()
                except OSError:
                    pass
            return

        if mode_val == ArtifactCleanupMode.SUCCESS.value:
            try:
                for out_path, _, kind in self.read_outputs(uid):
                    if kind == "cache":
                        out_obj = Path(out_path)
                        if out_obj.exists():
                            out_obj.unlink()
                            logger.info(f"Cleaned cache file: {out_obj}")
                op = self.outputs_path(uid)
                if op.exists():
                    op.unlink()
                ip = self.inputs_path(uid)
                if ip.exists():
                    ip.unlink()
            except OSError:
                pass
            return

        if mode_val == ArtifactCleanupMode.FAILURE_OR_RETRY.value:
            try:
                for out_path, cleanup, kind in self.read_outputs(uid):
                    if kind == "cache" or cleanup:
                        out_path_obj = Path(out_path)
                        if out_path_obj.exists():
                            if out_path_obj.is_dir():
                                shutil.rmtree(out_path_obj)
                            else:
                                out_path_obj.unlink()
                            logger.info(f"Cleaned broken output: {out_path_obj}")
            except Exception as e:
                logger.error(f"Could not remove outputs for {uid}: {e}")
            finally:
                try:
                    p = self.outputs_path(uid)
                    if p.exists():
                        p.unlink()
                    ip = self.inputs_path(uid)
                    if ip.exists():
                        ip.unlink()
                except OSError:
                    pass


# ── 模块级代理函数（保持向后兼容）─────────────────────────────

def signals_path(ipc_dir: Union[str, Path], uid: str) -> Path:
    return ArtifactJournal(ipc_dir).signals_path(uid)


def outputs_path(ipc_dir: Union[str, Path], uid: str) -> Path:
    return ArtifactJournal(ipc_dir).outputs_path(uid)


def inputs_path(ipc_dir: Union[str, Path], uid: str) -> Path:
    return ArtifactJournal(ipc_dir).inputs_path(uid)


def append_input(ipc_dir: Union[str, Path], uid: str, entry: dict) -> None:
    ArtifactJournal(ipc_dir).record_input_entry(uid, entry)


def append_output(
    ipc_dir: Union[str, Path], uid: str, out_path: str, cleanup: bool, kind: str = "output"
) -> None:
    ArtifactJournal(ipc_dir).record_output(uid, out_path, cleanup, kind=kind)


def append_signal(ipc_dir: Union[str, Path], uid: str, r_name: str, secs: float) -> None:
    ArtifactJournal(ipc_dir).record_signal(uid, r_name, secs)


def read_inputs(ipc_dir: Union[str, Path], uid: str) -> List[dict]:
    return ArtifactJournal(ipc_dir).read_inputs(uid)


def read_outputs(ipc_dir: Union[str, Path], uid: str) -> List[Tuple[str, bool, str]]:
    return ArtifactJournal(ipc_dir).read_outputs(uid)


def read_signals(ipc_dir: Union[str, Path], uid: str) -> List[Tuple[str, float]]:
    return ArtifactJournal(ipc_dir).drain_signals(uid)


__all__ = [
    "ArtifactCleanupMode",
    "ArtifactJournal",
    "signals_path",
    "outputs_path",
    "inputs_path",
    "append_input",
    "append_output",
    "append_signal",
    "read_inputs",
    "read_outputs",
    "read_signals",
]

