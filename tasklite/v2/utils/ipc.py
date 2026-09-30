"""v2 文件级 IPC 与产物清单：路径沙盒校验、声明追加落盘与声明读取。

本模块是模型层（JobContext）的 IPC 声明读写下沉点（分层红线：模型层
严禁 import 引擎层，IPC 声明读写一律经由本模块）。当前承载阶段所需
子集：

- 路径与沙盒校验：空字节防御、跨盘多根沙盒匹配、相对路径规范重定位；
- 声明追加落盘：输入（文件 stat 指纹 / URI 声明）、输出（产物 / 临时
  cache 声明）、挂起信号；
- 声明读取：输入/输出声明条目的坏行容灾解析。

结果落盘与降级、信号排空（摘除式原子协议）与产物生命周期清理随引擎
阶段扩展进本模块，接口形态以届时 ADR 实施单元为准。
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from typing import Any, Sequence

from .encoding import safe_uid_filename
from .jsonutil import dumps, loads

logger = logging.getLogger("tasklite.v2")

# 声明/信号文件的扩展名
_SIGNALS_SUFFIX = ".signals.jsonl"
_OUTPUTS_SUFFIX = ".outputs.jsonl"
_INPUTS_SUFFIX = ".inputs.jsonl"


class ArtifactJournal:
    """产物清单与文件级 IPC 声明深模块（模型层消费面子集）。

    封装沙盒越界防御、stat 指纹采集与坏行容灾解析；声明追加为尽力而
    为（OSError 静默——声明文件丢失只损失可追溯性，不得反噬 handler
    执行）。
    """

    def __init__(self, ipc_dir: str | Path | None = None) -> None:
        self.ipc_dir: str | None = str(ipc_dir) if ipc_dir is not None else None

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
        raw_path: str | Path,
        output_roots: Path | Sequence[Path] | None = None,
        *,
        sandbox: bool = True,
    ) -> str:
        """解析声明路径为规范绝对路径（沙盒校验 + 相对重定位共用逻辑）。"""
        raw = str(raw_path)
        if "\x00" in raw:
            raise ValueError(f"Output path contains a null byte: {raw!r}")

        if output_roots is not None and sandbox:
            p = Path(raw)
            if isinstance(output_roots, (list, tuple)):
                roots = [Path(r) for r in output_roots]
            else:
                roots = [Path(str(output_roots))]
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

    # ── 声明追加写入（Worker 侧）─────────────────────────────────

    def record_output(
        self, uid: str, out_path: str, *, cleanup: bool = True, kind: str = "output"
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
        self, uid: str, url: str, uri_fingerprint: str | None = None
    ) -> dict:
        """记录 URI 输入声明。"""
        entry: dict = {"path": url, "kind": "uri"}
        if uri_fingerprint is not None:
            entry["uri_fingerprint"] = uri_fingerprint
        self.record_input_entry(uid, entry)
        return entry

    def record_signal(self, uid: str, r_name: str, secs: float) -> None:
        """追加一条 suspend 信号到 signals 文件。

        信号立即落盘 flush——即使 handler 随后崩溃/超时，限流信息也不
        丢失（进程被 kill 后文件仍在，信号不丢）。
        """
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

    # ── 声明读取（引擎侧消费面）──────────────────────────────────

    def read_inputs(self, uid: str) -> list[dict]:
        """读取一个 job 的全部输入声明。"""
        if self.ipc_dir is None:
            return []
        path = self.inputs_path(uid)
        entries: list[dict] = []
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
                        except (json.JSONDecodeError, RecursionError, TypeError, ValueError):
                            continue
        except OSError:
            pass
        return entries

    def read_outputs(self, uid: str) -> list[tuple[str, bool, str]]:
        """读取一个 job 的全部已声明输出 (path, cleanup, kind)。"""
        if self.ipc_dir is None:
            return []
        path = self.outputs_path(uid)
        outputs: list[tuple[str, bool, str]] = []
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
                        except (json.JSONDecodeError, RecursionError, TypeError, ValueError):
                            continue
        except OSError:
            pass
        return outputs


__all__ = [
    "ArtifactJournal",
]
