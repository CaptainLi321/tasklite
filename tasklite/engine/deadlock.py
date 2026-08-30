"""死锁细粒度归因的纯逻辑。

``split_deadlock`` 是 `_handle_deadlock` 细粒度归因分支共用的拆分原语——
各分支的唯一差异是 uid 提取方式（malformed 无法 Job.from_dict，用
`uid_from_job_dict` 的 hash fallback）与筛选条件（索引 vs uid 集合）。
输入 queue + 筛选谓词，输出「进 DLQ 的肇事者」与「保留的剩余队列」。
"""

from typing import Callable, List, Tuple


def split_deadlock(
    queue: List[dict],
    error: str,
    *,
    extract_uid: Callable[[dict], str],
    include: Callable[[int, str], bool],
) -> Tuple[List[Tuple[str, dict]], List[dict]]:
    """把队列拆分为「进 DLQ 的肇事者」与「保留的剩余队列」。

    各分支的唯一差异是 uid 提取方式（malformed 无法
    Job.from_dict，用 uid_from_job_dict 的 hash fallback）与筛选条件
    （索引 vs uid 集合）。
    """
    uids_metas = []
    remaining_queue = []
    for idx, jd in enumerate(queue):
        uid = extract_uid(jd)
        if include(idx, uid):
            uids_metas.append((uid, {"error": error, "root_cause": True}))
        else:
            remaining_queue.append(jd)
    return uids_metas, remaining_queue
