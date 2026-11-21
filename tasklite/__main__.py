"""TaskLite 极简运维命令行入口（``python -m tasklite``，ADR-0012 §3.4）。

严格定位于只读观测与纯 ``uid`` 驱动的轻量状态运维：
- ``status``：薄封装 ``StateSnapshot.open`` 与 ``StateSnapshot.report``；
- ``set-status`` / ``remove``：100% 委托 ``TaskLite.edit()``，默认预览不落盘，
  显式传入 ``--apply`` 才原子提交；
- 所有子命令强制要求显式传入 ``<state_path>``，修改类命令前置校验路径存在且非目录，
  绝不静默新建空数据库文件。
"""
from __future__ import annotations

import argparse
from collections.abc import Sequence
from pathlib import Path
import sqlite3
import sys
from typing import Literal, TextIO, cast

from .backend.sqlite_backend import verify_existing_sqlite_database
from .exceptions import TaskLiteError
from .pipeline import TaskLite
from .snapshot import StateSnapshot


def _safe_print(text: str, *, file: TextIO) -> None:
    """在窄编码终端管道（如 Windows GBK）下安全输出文本，避免渲染符号触发编码异常。"""
    try:
        print(text, file=file)
    except UnicodeEncodeError:
        encoding = getattr(file, "encoding", None) or "utf-8"
        safe_text = text.encode(encoding, errors="replace").decode(encoding)
        print(safe_text, file=file)


def _validate_existing_state_path(raw_path: str) -> Path:
    """修改类子命令前置校验：只读探活已存在的 Schema 6 状态库，杜绝静默建空库或写入 WAL/SHM。"""
    if not raw_path or not raw_path.strip():
        raise TaskLiteError("state_path must be a non-empty file path")
    return verify_existing_sqlite_database(raw_path)


def _parse_entries(raw_entries: str) -> list[Literal["queue", "successes", "failures"]]:
    """按逗号拆分并去除空白；空值或非法分区名保留交由 EditTransaction.remove 统一校验。"""
    if not raw_entries.strip():
        return []
    parts = [part.strip() for part in raw_entries.split(",")]
    return cast(list[Literal["queue", "successes", "failures"]], parts)


def _add_recursive_group(subparser: argparse.ArgumentParser) -> None:
    """要求显式二选一传入 --recursive 或 --no-recursive。"""
    group = subparser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "--recursive",
        dest="recursive",
        action="store_true",
        help="沿正向 depends_on 闭包级联作用于全部下游作业",
    )
    group.add_argument(
        "--no-recursive",
        dest="recursive",
        action="store_false",
        help="仅作用于指定的单个作业，不级联下游",
    )


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tasklite",
        description="TaskLite 运维命令行工具（只读观测与纯 UID 级状态修整）。",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # 1. status
    status_parser = subparsers.add_parser(
        "status",
        help="输出状态库只读状态与耗时报告",
    )
    status_parser.add_argument(
        "state_path",
        help="SQLite 状态库文件路径（必填）",
    )

    # 2. set-status
    set_status_parser = subparsers.add_parser(
        "set-status",
        help="将指定作业（可选级联下游）流转为 pending 或 failed",
    )
    set_status_parser.add_argument(
        "state_path",
        help="SQLite 状态库文件路径（必填）",
    )
    set_status_parser.add_argument(
        "uid",
        help="目标作业规范 UID（task_type::job_id）",
    )
    set_status_parser.add_argument(
        "status",
        choices=("pending", "failed"),
        help="目标状态（仅允许 pending 或 failed）",
    )
    _add_recursive_group(set_status_parser)
    set_status_parser.add_argument(
        "--note",
        default=None,
        help="可选运维备注，写入事务变更记录",
    )
    set_status_parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="显式提交落盘（默认仅 dry-run 预览不写入数据库）",
    )

    # 3. remove
    remove_parser = subparsers.add_parser(
        "remove",
        help="从指定分区移除作业条目（永不删除 attempts 或磁盘文件）",
    )
    remove_parser.add_argument(
        "state_path",
        help="SQLite 状态库文件路径（必填）",
    )
    remove_parser.add_argument(
        "uid",
        help="目标作业规范 UID（task_type::job_id）",
    )
    remove_parser.add_argument(
        "--entries",
        default="queue,successes,failures",
        help="逗号分隔的目标分区列表（默认: queue,successes,failures）",
    )
    _add_recursive_group(remove_parser)
    remove_parser.add_argument(
        "--note",
        default=None,
        help="可选运维备注",
    )
    remove_parser.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="显式提交落盘（默认仅 dry-run 预览不写入数据库）",
    )

    return parser


def _cmd_status(args: argparse.Namespace) -> int:
    with StateSnapshot.open(args.state_path) as snap:
        _safe_print(StateSnapshot.report(snap), file=sys.stdout)
    return 0


def _cmd_set_status(args: argparse.Namespace) -> int:
    db_path = _validate_existing_state_path(args.state_path)
    target_status: Literal["pending", "failed"] = (
        "pending" if args.status == "pending" else "failed"
    )
    with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
        with pipeline.edit(dry_run=not args.apply, note=args.note) as tx:
            tx.set_status(args.uid, target_status, recursive=bool(args.recursive))
            _safe_print(tx.diff(), file=sys.stdout)
    if not args.apply:
        _safe_print(
            "Dry-run preview only; no changes written. Re-run with --apply to commit.",
            file=sys.stdout,
        )
    return 0


def _cmd_remove(args: argparse.Namespace) -> int:
    db_path = _validate_existing_state_path(args.state_path)
    entries = _parse_entries(args.entries)
    with TaskLite(backend="sqlite", state_path=db_path) as pipeline:
        with pipeline.edit(dry_run=not args.apply, note=args.note) as tx:
            tx.remove(args.uid, entries=entries, recursive=bool(args.recursive))
            _safe_print(tx.diff(), file=sys.stdout)
    if not args.apply:
        _safe_print(
            "Dry-run preview only; no changes written. Re-run with --apply to commit.",
            file=sys.stdout,
        )
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    """CLI 主入口，返回标准进程退出码（0 成功，1 运行/校验失败，2 参数错误）。"""
    parser = _build_parser()
    args = parser.parse_args(argv)
    try:
        if args.command == "status":
            return _cmd_status(args)
        if args.command == "set-status":
            return _cmd_set_status(args)
        if args.command == "remove":
            return _cmd_remove(args)
        raise TaskLiteError(f"Unknown command: {args.command!r}")
    except (TaskLiteError, OSError, sqlite3.Error) as exc:
        _safe_print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
