"""无状态纯依赖图原语：反向依赖索引、下游闭包遍历与迭代式 Kosaraju 环检测。

分层约束：本模块仅依赖 Python 标准库，供 ``PipelineState`` 与 ``EditTransaction``
同源复用，零递归深度限制。
"""
from __future__ import annotations

from collections import deque
from collections.abc import Collection, Iterator, Mapping, Set as AbstractSet


def build_dependents_index(edges: Mapping[str, Collection[str]]) -> dict[str, set[str]]:
    """由正向依赖边 ``node -> depends_on`` 构建反向依赖索引 ``dep -> {node...}``。"""
    dependents: dict[str, set[str]] = {}
    for node, deps in edges.items():
        for dep in deps:
            if isinstance(dep, str):
                dependents.setdefault(dep, set()).add(node)
    return dependents


def collect_downstream(
    root: str,
    dependents: Mapping[str, Collection[str]],
    *,
    stop_at: AbstractSet[str] = frozenset(),
) -> list[str]:
    """沿反向依赖索引迭代收集 ``root`` 的全部直接与间接下游（确定性字典序 BFS）。

    ``stop_at`` 中的节点为传播终止点：既不收入结果，也不向其下游继续扩散。
    """
    downstream: list[str] = []
    visited = {root}
    frontier: deque[str] = deque([root])
    while frontier:
        cur = frontier.popleft()
        for child in sorted(dependents.get(cur, ())):
            if child in visited or child in stop_at:
                continue
            visited.add(child)
            downstream.append(child)
            frontier.append(child)
    return downstream


def find_cycle_components(
    nodes: Collection[str],
    edges: Mapping[str, Collection[str]],
) -> list[list[str]]:
    """迭代式 Kosaraju 强连通分量检测，返回所有含环节点分量（零递归深度限制）。"""
    node_set = set(nodes)
    adj: dict[str, list[str]] = {
        u: [v for v in edges.get(u, ()) if v in node_set]
        for u in node_set
    }
    rev: dict[str, list[str]] = {u: [] for u in node_set}
    for u, nbrs in adj.items():
        for v in nbrs:
            rev[v].append(u)

    visited: set[str] = set()
    order: list[str] = []
    for root in sorted(node_set):
        if root in visited:
            continue
        visited.add(root)
        stack: list[tuple[str, Iterator[str]]] = [(root, iter(adj[root]))]
        while stack:
            cur, it = stack[-1]
            try:
                nxt = next(it)
            except StopIteration:
                stack.pop()
                order.append(cur)
                continue
            if nxt not in visited:
                visited.add(nxt)
                stack.append((nxt, iter(adj[nxt])))

    assigned: set[str] = set()
    cycles: list[list[str]] = []
    for root in reversed(order):
        if root in assigned:
            continue
        comp: list[str] = []
        dfs_stack = [root]
        assigned.add(root)
        while dfs_stack:
            cur = dfs_stack.pop()
            comp.append(cur)
            for nxt in rev[cur]:
                if nxt not in assigned:
                    assigned.add(nxt)
                    dfs_stack.append(nxt)
        if len(comp) > 1 or (len(comp) == 1 and comp[0] in adj[comp[0]]):
            cycles.append(sorted(comp))
    cycles.sort()
    return cycles


def find_cycle_members(
    nodes: Collection[str],
    edges: Mapping[str, Collection[str]],
) -> list[str]:
    """返回参与任意依赖环的全部节点列表（按分量有序去重）。"""
    return [uid for comp in find_cycle_components(nodes, edges) for uid in comp]


__all__ = [
    "build_dependents_index",
    "collect_downstream",
    "find_cycle_components",
    "find_cycle_members",
]
