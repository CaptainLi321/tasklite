"""文件级 IPC 与产物清单管理：输入/输出/信号/结果声明、沙盒校验、指纹采集与生命周期清理。

统一内敛文件级 IPC 与产物清单的完整生命周期管理：
- 路径与沙盒校验：跨盘多根沙盒匹配、空字节防御、相对路径规范重定位；
- 声明追加落盘：输入（文件 stat 指纹 / URI 声明）、输出（产物 / 临时 cache 声明）、挂起信号；
- 结果落盘与降级：原子写 (fsync + replace)、两级降级、坏行/损坏容灾解析；
- 校验与排空读取：产物存在性校验（忽略 cache）、信号原子排空（truncate + unlink）、残留结果认领；
- 产物与 IPC 生命周期清理：按 PRE_SUBMIT / SUCCESS / FAILURE_OR_RETRY 模式清理临时文件与声明。
"""

from __future__ import annotations

import enum
import json
import logging
import os
from pathlib import Path
import re
import shutil
import time
from typing import Any, Sequence
from urllib.parse import unquote

from .injective import safe_uid_filename
from .jsonutil import dump, dumps, load, loads

logger = logging.getLogger("tasklite")

# 声明/信号/结果文件的扩展名与模式
_SIGNALS_SUFFIX = ".signals.jsonl"
_OUTPUTS_SUFFIX = ".outputs.jsonl"
_INPUTS_SUFFIX = ".inputs.jsonl"
_RESULT_TMP_SUFFIX = ".result.json.tmp"
_RESULT_SUFFIX = ".result.json"
_INCARNATION_RE = re.compile(r"\.([0-9a-f]{32})\.(\d+)\.result\.json$")
_DRAINING_RE = re.compile(r"\.(\d+)\.(\d+)\.draining$")
_RAW_TUPLE_SENTINEL = "__tl_tuple_v1"


def encode_raw_result(raw_result: Any) -> Any:
    """编码 handler 返回值以便 JSON 落盘。"""
    if isinstance(raw_result, tuple):
        return [_RAW_TUPLE_SENTINEL, list(raw_result)]
    return raw_result


def decode_raw_result(encoded: Any) -> Any:
    """读取结果文件后还原 handler 返回值。"""
    if (
        isinstance(encoded, list)
        and len(encoded) == 2
        and encoded[0] == _RAW_TUPLE_SENTINEL
        and isinstance(encoded[1], (list, tuple))
    ):
        return tuple(encoded[1])
    return encoded


class ArtifactCleanupMode(str, enum.Enum):
    """产物生命周期清理模式。"""
    PRE_SUBMIT = "pre_submit"
    SUCCESS = "success"
    FAILURE_OR_RETRY = "failure_retry"


class ArtifactJournal:
    """产物清单与文件级 IPC 深模块。

    对外提供极简高层操作，封装沙盒越界防御、stat 指纹采集、坏行容灾解析、
    原子清空、结果原子落盘/降级与多模式文件生命周期清理逻辑。
    """

    def __init__(
        self,
        ipc_dir: str | Path | None = None,
        output_roots: str | Path | Sequence[str | Path] | None = None,
    ) -> None:
        self.ipc_dir: str | None = str(ipc_dir) if ipc_dir is not None else None
        if output_roots is None:
            self.output_roots: list[Path] | None = None
        elif isinstance(output_roots, (str, Path)):
            self.output_roots = [Path(output_roots).resolve()]
        else:
            self.output_roots = [Path(r).resolve() for r in output_roots]

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

    def result_path(self, uid: str, incarnation: str | None = None) -> Path:
        """某个 job 的最终结果文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build result_path")
        suffix = f".{incarnation}{_RESULT_SUFFIX}" if incarnation else _RESULT_SUFFIX
        return Path(self.ipc_dir) / f"{safe_uid_filename(uid)}{suffix}"

    def result_tmp_path(self, uid: str, incarnation: str | None = None) -> Path:
        """某个 job 的结果临时文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build result_tmp_path")
        suffix = f".{incarnation}{_RESULT_TMP_SUFFIX}" if incarnation else _RESULT_TMP_SUFFIX
        return Path(self.ipc_dir) / f"{safe_uid_filename(uid)}{suffix}"

    def iter_stale_result_paths(self, uid: str) -> list[Path]:
        """枚举某个 uid 的全部残留结果文件路径。"""
        if self.ipc_dir is None:
            return []
        d = Path(self.ipc_dir)
        base = f"{safe_uid_filename(uid)}"
        found: list[Path] = []
        try:
            for pat in (f"{base}.*{_RESULT_SUFFIX}", f"{base}.*{_RESULT_TMP_SUFFIX}"):
                for p in d.glob(pat):
                    if _INCARNATION_RE.match(p.name, pos=len(base)):
                        found.append(p)
        except OSError:
            pass
        return found

    # ── 路径沙盒与规范化 ──────────────────────────────────────────

    @staticmethod
    def resolve_and_validate_path(
        raw_path: str | Path,
        output_roots: Path | Sequence[Path] | None = None,
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
        self, uid: str, url: str, uri_fingerprint: str | None = None
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

    # ── 结果写入与降级 ──────────────────────────────────────────

    def write_result_atomic(
        self, uid: str, result_dict: dict, incarnation: str | None = None
    ) -> None:
        """原子写结果：先写 .tmp 再 os.replace。失败时清理 .tmp。"""
        tmp = self.result_tmp_path(uid, incarnation)
        final = self.result_path(uid, incarnation)
        tmp.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                dump(result_dict, f)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp, final)
            try:
                dir_fd = os.open(str(final.parent), os.O_RDONLY)
                try:
                    os.fsync(dir_fd)
                finally:
                    os.close(dir_fd)
            except OSError:
                pass
        except Exception:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            raise

    def write_result_with_degradation(
        self, uid: str, payload: dict[str, Any], incarnation: str | None = None
    ) -> None:
        """worker 结果落盘的唯一出口：完整写失败时两级降级，绝不裸抛 OSError。"""
        try:
            self.write_result_atomic(uid, payload, incarnation=incarnation)
            return
        except OSError as e:
            logger.warning(
                f"result write failed for {uid}: {e}; retrying full payload once after brief pause"
            )
        time.sleep(0.05)
        try:
            self.write_result_atomic(uid, payload, incarnation=incarnation)
            return
        except OSError as e:
            write_err = str(e)
            logger.warning(
                f"full result write retry also failed for {uid}: {e}; falling back to degraded result"
            )
        orig_status = payload.get("status")
        degraded_status = "retry" if orig_status in (None, "success") else orig_status
        degraded: dict[str, Any] = {
            "status": degraded_status,
            "error": f"IPC_RESULT_WRITE_DEGRADED: {write_err}",
        }
        # 认证令牌随降级继承：读取侧强校验下丢令牌的降级结果会被误拒，
        # transient_kind 的瞬态保真语义随之失效
        if "auth" in payload:
            degraded["auth"] = payload["auth"]
        if payload.get("transient_kind"):
            degraded["transient_kind"] = payload["transient_kind"]
        try:
            self.write_result_atomic(uid, degraded, incarnation=incarnation)
        except OSError as e2:
            logger.error(
                f"degraded result write also failed for {uid}: {e2}; worker exiting without IPC result"
            )

    # ── 读取与解析（父进程 Engine 侧）─────────────────────────────

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

    def drain_signals(self, uid: str) -> list[tuple[str, float]]:
        """读取并删除一个 job 的 suspend 信号文件（排空语义）。

        不变式：先以原子 rename 把信号文件摘出命名空间，再读摘除后的稳定
        inode——活跃 worker 的 O_APPEND 追加要么落在 rename 前的旧 inode
        （本次读得），要么经按名新建落入同名新文件（下轮排空读得）。
        「读后 truncate 抹写」与「unlink 后按名写孤儿 inode」两类丢失窗口
        由该摘除序消除；残余窗口仅剩「worker 持旧 inode 的未落盘写跨过
        rename 且在读取 EOF 之后才 flush」，已收窄至微秒级。
        不变式：读者自身在 rename 后、unlink 前死亡会把信号内容困在
        .draining 孤儿中，每次排空必须先回收同名孤儿（先读取分发再删除，
        直接丢弃会把崩溃窗口放大成永久丢信号），保证信号不丢不重。
        """
        if self.ipc_dir is None:
            return []
        signals = self._salvage_draining_files(uid)
        path = self.signals_path(uid)
        # 摘除名须与原文件同目录（同文件系统是 rename 原子性的前提）
        draining = path.with_name(
            f"{path.name}.{os.getpid()}.{time.monotonic_ns()}.draining"
        )
        try:
            os.rename(path, draining)
        except OSError:
            return signals
        signals.extend(self._read_suspend_lines(draining))
        try:
            draining.unlink()
        except OSError:
            pass
        return signals

    def drain_all_signals(self) -> list[tuple[str, str, float]]:
        """清扫 ipc_dir 全部 suspend 信号残留并排空（含 .draining 孤儿）。

        不变式：任何清理动作前先读取并按语义分发内容——本方法只负责
        排空（先读后删），应用由调用方完成。跨 run 崩溃可能遗留「已落盘
        信号却无任何再消费路径」的残留：后续派发预检清理与完成收尾清理
        都会未读删除信号文件，启动期全量清扫是该不变式的兜底回收点。
        每个文件名还原 uid 后复用 ``drain_signals`` 的摘除式排空与孤儿
        回收协议；文件名 → uid 取 ``safe_uid_filename`` 的百分号解码逆
        映射，往返校验失败（像集外形态）不触碰。
        """
        if self.ipc_dir is None:
            return []
        try:
            d = Path(self.ipc_dir)
            candidates = [p.name for p in d.glob(f"*{_SIGNALS_SUFFIX}")]
            candidates += [
                p.name for p in d.glob(f"*{_SIGNALS_SUFFIX}.*.draining")
            ]
        except OSError:
            return []
        out: list[tuple[str, str, float]] = []
        seen: set[str] = set()
        for name in candidates:
            uid = self._uid_from_signals_filename(name)
            if uid is None or uid in seen:
                continue
            seen.add(uid)
            for r_name, secs in self.drain_signals(uid):
                out.append((uid, r_name, secs))
        return out

    @staticmethod
    def _uid_from_signals_filename(name: str) -> str | None:
        """信号文件名（含 .draining 摘除名）→ uid；非协议域形态返回 None。

        摘除名形状为 {base}.signals.jsonl.{pid}.{ns}.draining，与
        ``_salvage_draining_files`` 的锚定正则同域。
        """
        if name.endswith(_SIGNALS_SUFFIX):
            base = name[: -len(_SIGNALS_SUFFIX)]
        elif name.endswith(".draining"):
            parts = name[: -len(".draining")].rsplit(".", 2)
            if len(parts) != 3 or not parts[1].isdigit() or not parts[2].isdigit():
                return None
            if not parts[0].endswith(_SIGNALS_SUFFIX):
                return None
            base = parts[0][: -len(_SIGNALS_SUFFIX)]
        else:
            return None
        uid = unquote(base)
        if safe_uid_filename(uid) != base:
            return None
        return uid

    @staticmethod
    def _read_suspend_lines(path: Path) -> list[tuple[str, float]]:
        """读取单个信号文件中的全部 suspend 记录（坏行容灾跳过）。"""
        signals: list[tuple[str, float]] = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        data = loads(line)
                        if isinstance(data, dict) and "suspend" in data:
                            r_name, secs = data["suspend"]
                            signals.append((r_name, float(secs)))
                    except (json.JSONDecodeError, RecursionError, TypeError, ValueError):
                        continue
        except OSError:
            pass
        return signals

    def _salvage_draining_files(self, uid: str) -> list[tuple[str, float]]:
        """回收 uid 名下排空中途读者死亡遗留的 .draining 孤儿（先读后删）。

        孤儿名形状为 {base}.signals.jsonl.{pid}.{monotonic_ns}.draining，
        glob 先按 uid 前缀 + 信号后缀收敛，再以正则锚定 {pid}.{ns} 段，
        防止前缀恰好重叠的其他 uid（如 a 与 a.signals.jsonl）互串。
        """
        if self.ipc_dir is None:
            return []
        base = safe_uid_filename(uid)
        anchor = len(base) + len(_SIGNALS_SUFFIX)
        signals: list[tuple[str, float]] = []
        try:
            orphans = [
                p
                for p in Path(self.ipc_dir).glob(f"{base}{_SIGNALS_SUFFIX}.*.draining")
                if _DRAINING_RE.match(p.name, pos=anchor)
            ]
        except OSError:
            return []
        for orphan in orphans:
            signals.extend(self._read_suspend_lines(orphan))
            try:
                orphan.unlink()
            except OSError:
                pass
        return signals

    def read_result(
        self, path_or_uid: str | Path, incarnation: str | None = None
    ) -> dict | None:
        """读取结果文件；损坏/不存在返回 None。"""
        if isinstance(path_or_uid, Path):
            path = path_or_uid
        elif "/" in str(path_or_uid) or "\\" in str(path_or_uid):
            path = Path(path_or_uid)
        else:
            path = self.result_path(str(path_or_uid), incarnation=incarnation)

        try:
            with open(path, encoding="utf-8") as f:
                data = load(f)
            if isinstance(data, dict):
                return data
            logger.warning(f"Corrupt result file {path}: not a dict, ignoring")
            return None
        except FileNotFoundError:
            return None
        except (json.JSONDecodeError, OSError, RecursionError, TypeError, ValueError) as e:
            logger.warning(f"Corrupt result file {path}: {e}, ignoring")
            return None

    def claim_stale_result(self, uid: str) -> dict | None:
        """认领并读取遗留的已落盘残留结果，清理其余过期结果。"""
        res_paths = self.iter_stale_result_paths(uid)
        res_paths = [p for p in res_paths if p.name.endswith(_RESULT_SUFFIX)]
        if not res_paths:
            return None

        def _freshness_key(p: Path) -> tuple[int, int]:
            try:
                mtime_ns = p.stat().st_mtime_ns
            except OSError:
                mtime_ns = -1
            try:
                seq = int(p.name.removesuffix(_RESULT_SUFFIX).rsplit(".", 1)[-1])
            except (ValueError, IndexError):
                seq = -1
            return (mtime_ns, seq)

        res_path = max(res_paths, key=_freshness_key)
        res = self.read_result(res_path)
        for p in res_paths:
            try:
                p.unlink()
            except OSError:
                pass

        if not isinstance(res, dict) or "status" not in res:
            logger.warning(
                f"Discarding stale result file for {uid}: not a valid result dict: {res!r}"
            )
            return None
        return res

    # ── 产物校验与生命周期清理 ────────────────────────────────────

    def _in_cleanup_sandbox(self, raw_path: str) -> bool:
        """删除前的消费侧沙盒归属复检（解析符号链接与 ``..`` 后判定）。

        不变式：``.outputs.jsonl`` 是 ipc_dir 上的不可信输入，声明侧校验
        可被伪造声明绕过——任何 unlink/rmtree 前必须复检路径归属，信任
        根为 output_roots 与 ipc_dir 的并集；无任何信任根或解析越界一律
        拒绝清理（fail-safe 方向：漏删可人工补救，误删不可逆）。
        """
        candidates: list[Path] = list(self.output_roots or [])
        if self.ipc_dir:
            candidates.append(Path(self.ipc_dir).resolve())
        if not candidates:
            return False
        try:
            self.resolve_and_validate_path(raw_path, candidates, sandbox=True)
        except ValueError:
            return False
        return True

    def _cleanup_sandbox_guard(self, uid: str, raw_path: str) -> bool:
        """清理卫兵：归属复检失败时告警一次并示意调用方跳过该路径。"""
        if self._in_cleanup_sandbox(raw_path):
            return True
        logger.error(
            f"Refusing to clean declared path outside sandbox for {uid}: {raw_path}"
        )
        return False

    def verify_outputs(self, uid: str) -> tuple[bool, str | None]:
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

    def cleanup_ipc_files(self, uid: str, incarnation: str | None = None) -> None:
        """删除某个 job 的结果/信号/临时文件（不含 outputs.jsonl）。"""
        if self.ipc_dir is None:
            return
        targets = [self.signals_path(uid)]
        if incarnation is not None:
            targets += [
                self.result_path(uid, incarnation),
                self.result_tmp_path(uid, incarnation),
            ]
        targets += self.iter_stale_result_paths(uid)
        for p in targets:
            try:
                p.unlink()
            except (FileNotFoundError, OSError):
                pass

    def cleanup(self, uid: str, mode: str | ArtifactCleanupMode) -> None:
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
            for sp in self.iter_stale_result_paths(uid):
                try:
                    if sp.exists():
                        sp.unlink()
                except OSError:
                    pass
            return

        if mode_val == ArtifactCleanupMode.SUCCESS.value:
            try:
                for out_path, _, kind in self.read_outputs(uid):
                    if kind == "cache":
                        if not self._cleanup_sandbox_guard(uid, out_path):
                            continue
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
            self.cleanup_ipc_files(uid)
            return

        if mode_val == ArtifactCleanupMode.FAILURE_OR_RETRY.value:
            try:
                for out_path, cleanup, kind in self.read_outputs(uid):
                    if kind == "cache" or cleanup:
                        if not self._cleanup_sandbox_guard(uid, out_path):
                            continue
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
                self.cleanup_ipc_files(uid)


__all__ = [
    "ArtifactCleanupMode",
    "ArtifactJournal",
    "encode_raw_result",
    "decode_raw_result",
]

