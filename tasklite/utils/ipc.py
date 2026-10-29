"""v2 文件级 IPC 与产物清单：声明读写、结果落盘降级、信号排空与生命周期清理。

统一内敛文件级 IPC 与产物清单的完整生命周期管理：

- 路径与沙盒校验：空字节防御、跨盘多根沙盒匹配、相对路径规范重定位；
- 声明追加落盘：输入（规范路径）、输出（产物 / 临时
  cache 声明）、挂起信号；
- 结果落盘与降级：原子写（fsync + replace）、两级降级、坏行/损坏容灾
  解析；瞬态信号（interrupted / lock_conflict / rate_limited）的降级
  保真——``transient_kind`` 结构化字段随降级继承，瞬态军规全链不失效；
- 校验与排空读取：产物存在性校验（忽略 cache）、信号原子排空（rename
  摘除 + 孤儿回收）、残留结果认领（``claim_stale_result_payload``，
  与 channel 层认领入口同名异构消歧）；
- 产物与 IPC 生命周期清理：按 PRE_SUBMIT / SUCCESS / FAILURE_OR_RETRY
  模式清理临时文件与声明，删除前消费侧沙盒归属复检。

分层红线：本模块是模型层（JobContext）的 IPC 声明读写下沉点；瞬态信号
落盘即回队——本层严禁出现任何重试节奏语义（节奏归 RequeuePolicy）。
"""

from __future__ import annotations

import enum
import json
import logging
import os
import re
import time
from pathlib import Path
from typing import Any, Sequence
from urllib.parse import unquote

from ..exceptions import _ArtifactIOError
from ..models.artifact import ArtifactDeclaration, append_artifact_declaration, validate_artifact_declarations
from .artifact import ArtifactPathPolicy
from .encoding import encode_job_uid_filename
from .jsonutil import dump, dumps, load, loads

logger = logging.getLogger("tasklite")

# 声明/信号/结果文件的扩展名与锚定模式
_SIGNALS_SUFFIX = ".signals.jsonl"
_OUTPUTS_SUFFIX = ".outputs.jsonl"
_INPUTS_SUFFIX = ".inputs.jsonl"
_RESULT_TMP_SUFFIX = ".result.json.tmp"
_RESULT_SUFFIX = ".result.json"
_EXECUTION_ID_RESULT_RE = re.compile(r"\.([0-9a-f]{32})\.(\d+)\.result\.json$")
_DRAINING_RE = re.compile(r"\.(\d+)\.(\d+)\.draining$")
_RAW_TUPLE_SENTINEL = "__tl_tuple_v1"


def encode_raw_result(raw_result: Any) -> Any:
    """编码 handler 返回值以便 JSON 落盘（tuple 经哨兵标记还原）。"""
    if isinstance(raw_result, tuple):
        return [_RAW_TUPLE_SENTINEL, list(raw_result)]
    return raw_result


def decode_raw_result(encoded: Any) -> Any:
    """读取结果文件后还原 handler 返回值。

    哨兵碰撞防御：用户返回值恰好形如 ``[哨兵, x]`` 且还原段非 list/tuple
    时原样返回——强行 ``tuple(x)`` 会在非序列还原段上抛 TypeError 穿透
    收割链（单任务损坏放大为整管崩溃）。
    """
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
    FAILURE_OR_RETRY = "failure_or_retry"


class ArtifactJournal:
    """产物清单与文件级 IPC 声明深模块。

    对外提供极简高层操作，封装沙盒越界防御、输入路径记录、坏行容灾
    解析、原子清空、结果原子落盘/降级与多模式文件生命周期清理逻辑。
    """

    def __init__(
        self,
        ipc_dir: str | Path | None = None,
        output_roots: str | Path | Sequence[str | Path] | None = None,
        protected_roots: Sequence[str | Path] = (),
    ) -> None:
        self.protected_roots = tuple(protected_roots)
        self.ipc_dir: str | None = str(ipc_dir) if ipc_dir is not None else None
        if output_roots is None:
            self.output_roots: list[Path] | None = None
        elif isinstance(output_roots, (str, Path)):
            self.output_roots = [Path(output_roots).resolve()]
        else:
            self.output_roots = [Path(r).resolve() for r in output_roots]

    # ── 路径构造 ──────────────────────────────────────────────────

    def signals_path(self, job_uid: str) -> Path:
        """某个 job 的 suspend 信号文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build signals_path")
        return Path(self.ipc_dir) / f"{encode_job_uid_filename(job_uid)}{_SIGNALS_SUFFIX}"

    def outputs_path(self, job_uid: str) -> Path:
        """某个 job 的已声明输出文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build outputs_path")
        return Path(self.ipc_dir) / f"{encode_job_uid_filename(job_uid)}{_OUTPUTS_SUFFIX}"

    def inputs_path(self, job_uid: str) -> Path:
        """输入声明的落盘路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build inputs_path")
        return Path(self.ipc_dir) / f"{encode_job_uid_filename(job_uid)}{_INPUTS_SUFFIX}"

    def result_path(self, job_uid: str, execution_id: str | None = None) -> Path:
        """某个 job 的最终结果文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build result_path")
        suffix = f".{execution_id}{_RESULT_SUFFIX}" if execution_id else _RESULT_SUFFIX
        return Path(self.ipc_dir) / f"{encode_job_uid_filename(job_uid)}{suffix}"

    def result_tmp_path(self, job_uid: str, execution_id: str | None = None) -> Path:
        """某个 job 的结果临时文件路径。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to build result_tmp_path")
        suffix = f".{execution_id}{_RESULT_TMP_SUFFIX}" if execution_id else _RESULT_TMP_SUFFIX
        return Path(self.ipc_dir) / f"{encode_job_uid_filename(job_uid)}{suffix}"

    def iter_stale_result_paths(self, job_uid: str) -> list[Path]:
        """枚举某个 uid 的全部残留结果文件路径（锚定 execution_id 形态）。

        不变式：job_id 可含点，glob ``{uid}.*`` 会命中点后缀兄弟 uid
        （如 ``h::a`` 与 ``h::a.b``）的文件——必须以 execution_id 正则
        在 base 之后锚定匹配，防误认/误删兄弟 uid 的结果。
        """
        if self.ipc_dir is None:
            return []
        d = Path(self.ipc_dir)
        base = f"{encode_job_uid_filename(job_uid)}"
        found: list[Path] = []
        try:
            for pat in (f"{base}.*{_RESULT_SUFFIX}", f"{base}.*{_RESULT_TMP_SUFFIX}"):
                for p in d.glob(pat):
                    if _EXECUTION_ID_RESULT_RE.match(p.name, pos=len(base)):
                        found.append(p)
        except OSError:
            pass
        return found

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
                    f"Output path '{raw}' resolves outside output_roots {roots!r}"
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

    def _append_artifact(self, job_uid: str, declaration: ArtifactDeclaration) -> None:
        if self.ipc_dir is None:
            return
        path = self.outputs_path(job_uid)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "a", encoding="utf-8") as stream:
            stream.write(dumps(declaration.to_dict()) + "\n")
            stream.flush()

    def record_artifact(
        self, job_uid: str, declaration: ArtifactDeclaration, *, prepare: bool = False,
    ) -> None:
        """校验同执行授权与路径范围后追加；持久写入错误向调用者传播。"""
        if self.ipc_dir is None:
            raise ValueError("ipc_dir is required to record an artifact")
        try:
            self._validate_artifact_path(declaration)
            previous = self._read_artifact_entries(job_uid)
        except OSError as error:
            raise _ArtifactIOError(str(error)) from error
        declarations = append_artifact_declaration(previous, declaration)
        if len(declarations) == len(previous):
            return
        if prepare:
            try:
                self.path_policy.remove_existing(declaration)
            except FileExistsError as error:
                if not declaration.delete_existing:
                    raise
                raise _ArtifactIOError(str(error)) from error
            except OSError as error:
                raise _ArtifactIOError(str(error)) from error
        try:
            if prepare:
                Path(declaration.path).parent.mkdir(parents=True, exist_ok=True)
            self._append_artifact(job_uid, declaration)
        except OSError as error:
            raise _ArtifactIOError(str(error)) from error

    @property
    def path_policy(self) -> ArtifactPathPolicy:
        """本执行配置对应的路径与保护范围。"""
        return ArtifactPathPolicy(
            output_roots=self.output_roots,
            protected_roots=(*self.protected_roots, *((self.ipc_dir,) if self.ipc_dir else ())),
        )

    def _validate_artifact_path(self, declaration: ArtifactDeclaration) -> None:
        resolved = self.path_policy.resolve(declaration.path, sandbox=declaration.sandbox)
        if resolved != declaration.path:
            raise ValueError("artifact declaration must use the canonical path")

    def record_input_entry(self, job_uid: str, entry: dict) -> None:
        """追加一条输入声明条目到落盘文件。"""
        if self.ipc_dir is None:
            return
        path = self.inputs_path(job_uid)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(dumps(entry) + "\n")
                f.flush()
        except OSError as error:
            raise _ArtifactIOError(str(error)) from error

    def record_input_file(self, job_uid: str, resolved_path: str) -> str:
        """追加一条规范输入路径声明并返回该路径字符串。"""
        self.record_input_entry(job_uid, {"path": resolved_path})
        return resolved_path

    def record_signal(
        self, job_uid: str, resource_name: str, seconds: float
    ) -> None:
        """追加一条 suspend 信号到 signals 文件。

        信号立即落盘 flush——即使 handler 随后崩溃/超时，限流信息也不
        丢失（进程被 kill 后文件仍在，信号不丢）。
        """
        if self.ipc_dir is None:
            return
        path = self.signals_path(job_uid)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                f.write(dumps({"suspend": [resource_name, seconds]}) + "\n")
                f.flush()
        except OSError:
            pass

    # ── 结果写入与降级 ──────────────────────────────────────────

    def write_result_atomic(
        self, job_uid: str, result_dict: dict, execution_id: str | None = None
    ) -> None:
        """原子写结果：先写 .tmp 再 os.replace。失败时清理 .tmp。"""
        tmp = self.result_tmp_path(job_uid, execution_id)
        final = self.result_path(job_uid, execution_id)
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
        self, job_uid: str, payload: dict[str, Any], execution_id: str | None = None
    ) -> None:
        """worker 结果落盘的唯一出口：完整写失败时两级降级，绝不裸抛 OSError。

        降级语义：完整写 → 短暂停顿后重试完整写 → 降级写最小结果
        （success 改写 retry 让 job 重跑，其他 status 保持原语义）→
        仍失败记 error 后放弃（worker 无结果退出，收割按崩溃语义处理）。
        """
        try:
            self.write_result_atomic(job_uid, payload, execution_id=execution_id)
            return
        except OSError as e:
            logger.warning(
                f"result write failed for {job_uid}: {e}; retrying full payload once after brief pause"
            )
        time.sleep(0.05)
        try:
            self.write_result_atomic(job_uid, payload, execution_id=execution_id)
            return
        except OSError as e:
            write_err = str(e)
            logger.warning(
                f"full result write retry also failed for {job_uid}: {e}; falling back to degraded result"
            )
        orig_status = payload.get("status")
        degraded_status = "retry" if orig_status in (None, "success") else orig_status
        degraded: dict[str, Any] = {
            "status": degraded_status,
            "error_message": f"IPC_RESULT_WRITE_DEGRADED: {write_err}",
        }
        # 认证令牌与瞬态种类随降级继承：读取侧强校验下丢令牌的降级结果
        # 会被误拒，transient_kind 的瞬态保真语义（不烧预算）随之失效
        if "auth" in payload:
            degraded["auth"] = payload["auth"]
        if payload.get("transient_kind"):
            degraded["transient_kind"] = payload["transient_kind"]
        try:
            self.write_result_atomic(job_uid, degraded, execution_id=execution_id)
        except OSError as e2:
            logger.error(
                f"degraded result write also failed for {job_uid}: {e2}; worker exiting without IPC result"
            )

    # ── 读取与解析（父进程 Engine 侧）─────────────────────────────

    def read_inputs(self, job_uid: str) -> list[str]:
        """严格读取输入路径；缺失日志为空，读取错误不得丢失依赖后发布成功。"""
        if self.ipc_dir is None:
            return []
        try:
            stream = open(self.inputs_path(job_uid), encoding="utf-8")
        except FileNotFoundError:
            return []
        paths: list[str] = []
        with stream:
            for line in stream:
                if not line.strip():
                    continue
                try:
                    data = loads(line)
                    if not isinstance(data, dict) or not isinstance(data.get("path"), str) or not data["path"]:
                        raise ValueError("input path is required")
                except (RecursionError, TypeError, ValueError) as error:
                    raise ValueError(f"invalid input declaration for {job_uid}") from error
                paths.append(data["path"])
        return paths

    def read_artifacts(self, job_uid: str) -> tuple[ArtifactDeclaration, ...]:
        """严格读取本执行授权；缺失日志为空，旧格式或损坏记录拒绝发布。"""
        return validate_artifact_declarations(self._read_artifact_entries(job_uid))

    def _read_artifact_entries(self, job_uid: str) -> tuple[ArtifactDeclaration, ...]:
        if self.ipc_dir is None:
            return ()
        try:
            stream = open(self.outputs_path(job_uid), encoding="utf-8")
        except FileNotFoundError:
            return ()
        declarations: list[ArtifactDeclaration] = []
        with stream:
            for line in stream:
                if not line.strip():
                    continue
                try:
                    declaration = ArtifactDeclaration.from_dict(loads(line))
                except (RecursionError, TypeError, ValueError) as error:
                    raise ValueError(f"invalid artifact declaration for {job_uid}") from error
                if declaration.name is not None:
                    self._validate_artifact_path(declaration)
                declarations.append(declaration)
        return tuple(declarations)

    def drain_signals(self, job_uid: str) -> list[tuple[str, float]]:
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
        signals = self._salvage_draining_files(job_uid)
        path = self.signals_path(job_uid)
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
        回收协议；文件名 → uid 取 ``encode_job_uid_filename`` 的百分号解码逆
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
            job_uid = self._uid_from_signals_filename(name)
            if job_uid is None or job_uid in seen:
                continue
            seen.add(job_uid)
            for r_name, secs in self.drain_signals(job_uid):
                out.append((job_uid, r_name, secs))
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
        job_uid = unquote(base)
        if encode_job_uid_filename(job_uid) != base:
            return None
        return job_uid

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

    def _salvage_draining_files(self, job_uid: str) -> list[tuple[str, float]]:
        """回收 uid 名下排空中途读者死亡遗留的 .draining 孤儿（先读后删）。

        孤儿名形状为 {base}.signals.jsonl.{pid}.{monotonic_ns}.draining，
        glob 先按 uid 前缀 + 信号后缀收敛，再以正则锚定 {pid}.{ns} 段，
        防止前缀恰好重叠的其他 uid（如 a 与 a.signals.jsonl）互串。
        """
        if self.ipc_dir is None:
            return []
        base = encode_job_uid_filename(job_uid)
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
        self, path_or_uid: str | Path, execution_id: str | None = None
    ) -> dict | None:
        """读取结果文件；损坏/不存在返回 None（宁可重跑，不可崩）。"""
        if isinstance(path_or_uid, Path):
            path = path_or_uid
        elif "/" in str(path_or_uid) or "\\" in str(path_or_uid):
            path = Path(path_or_uid)
        else:
            path = self.result_path(str(path_or_uid), execution_id=execution_id)

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

    def claim_stale_result_payload(self, job_uid: str) -> dict | None:
        """认领并读取遗留的已落盘残留结果，清理其余过期结果。

        只消费 final 结果文件——.tmp 残留可能是孤儿 worker 正在写的
        半成品，读部分 JSON 返回 None 后 unlink 正在写的文件会让孤儿
        的 os.replace 抛 FileNotFoundError。残留取舍以
        (mtime_ns, dispatch_no) 决胜：秒级 mtime 同刻不稳定，最新
        执行代的派发号 是第二决胜键。
        """
        res_paths = self.iter_stale_result_paths(job_uid)
        res_paths = [p for p in res_paths if p.name.endswith(_RESULT_SUFFIX)]
        if not res_paths:
            return None

        res_path = max(res_paths, key=self._freshness_key)
        res = self.read_result(res_path)
        for p in res_paths:
            try:
                p.unlink()
            except OSError:
                pass

        if not isinstance(res, dict) or "status" not in res:
            logger.warning(
                f"Discarding stale result file for {job_uid}: not a valid result dict: {res!r}"
            )
            return None
        return res

    @staticmethod
    def _freshness_key(p: Path) -> tuple[int, int]:
        """残留结果新鲜度键 (mtime_ns, dispatch_no)。"""

        def _dispatch_no_of(path: Path) -> int:
            try:
                return int(path.name.removesuffix(_RESULT_SUFFIX).rsplit(".", 1)[-1])
            except (ValueError, IndexError):
                return -1

        try:
            mtime_ns = p.stat().st_mtime_ns
        except OSError:
            mtime_ns = -1
        return (mtime_ns, _dispatch_no_of(p))

    # ── 产物校验与生命周期清理 ────────────────────────────────────

    def verify_artifacts(self, declarations: Sequence[ArtifactDeclaration]) -> str | None:
        """仅验证需保留的产物；临时路径允许已被 rename 移走。"""
        for declaration in declarations:
            self._validate_artifact_path(declaration)
            if not declaration.delete_on_success:
                try:
                    os.stat(declaration.path)
                except FileNotFoundError:
                    return f"Missing output {declaration.path}"
        return None

    def cleanup_declared_artifacts(
        self, declarations: Sequence[ArtifactDeclaration], *, success: bool,
    ) -> None:
        """只执行该结局的删除授权，错误不得降级为清理完成。"""
        policy = self.path_policy
        for declaration in declarations:
            policy.cleanup(declaration, success=success)

    def clear_declarations(self, job_uid: str) -> None:
        """业务转移已完成后删除声明；失败不改写已经提交的事实。"""
        if self.ipc_dir is not None:
            self.outputs_path(job_uid).unlink(missing_ok=True)
            self.inputs_path(job_uid).unlink(missing_ok=True)
        self.cleanup_ipc_files(job_uid)

    def cleanup_ipc_files(self, job_uid: str, execution_id: str | None = None) -> None:
        """删除某个 job 的结果/信号/临时文件（不含 outputs.jsonl）。"""
        if self.ipc_dir is None:
            return
        targets = [self.signals_path(job_uid)]
        if execution_id is not None:
            targets += [
                self.result_path(job_uid, execution_id),
                self.result_tmp_path(job_uid, execution_id),
            ]
        targets += self.iter_stale_result_paths(job_uid)
        for p in targets:
            try:
                p.unlink()
            except (FileNotFoundError, OSError):
                pass

    def cleanup(self, job_uid: str, mode: str | ArtifactCleanupMode) -> None:
        """清理本执行授权产物；派发前仅丢弃残留声明，不重放删除。"""
        mode = ArtifactCleanupMode(mode)
        if mode is not ArtifactCleanupMode.PRE_SUBMIT:
            self.cleanup_declared_artifacts(
                self.read_artifacts(job_uid), success=mode is ArtifactCleanupMode.SUCCESS,
            )
        self.clear_declarations(job_uid)


__all__ = [
    "ArtifactCleanupMode",
    "ArtifactJournal",
    "encode_raw_result",
    "decode_raw_result",
]
