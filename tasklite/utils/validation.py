"""Payload validation utilities."""
import math
import types
import typing

_UnionType = getattr(types, "UnionType", None)


def validate_resource_amounts(resources: dict, where: str) -> None:
    """校验资源 amount 数值（Job.__init__ 与 pipeline 注册路径共用单点）。

    拒绝三态：非数值（含 bool——bool 是 int 子类）/ NaN·Inf（毒化
    CapacityResource used 账目 → livelock）/ 负值（acquire 时抛 ValueError
    → try 之外资源永久泄漏）。``where`` 用于错误消息定位调用来源。
    """
    for res_name, amount in resources.items():
        if not isinstance(amount, (int, float)) or isinstance(amount, bool):
            raise TypeError(
                f"resource '{res_name}' amount in {where} must be a number, "
                f"got {type(amount).__name__} ({amount!r})"
            )
        try:
            finite = math.isfinite(amount)
        except OverflowError:
            # 超大 int（>1.8e308，如 10**400）让 math.isfinite
            # 抛 OverflowError 而非返回 False——用户拿到无定位的原始异常，
            # 且逃逸调度器 malformed 兜底（捕获元组不含 OverflowError）
            # → 磁盘脏数据可崩整轮扫描。转为标准 ValueError（与有限性
            # 校验同语义：超出 float 表示范围的量不可用）。
            raise ValueError(
                f"resource '{res_name}' amount in {where} is too large "
                f"(exceeds float range), got {amount!r}"
            )
        if not finite:
            raise ValueError(
                f"resource '{res_name}' amount in {where} must be finite, got {amount!r}"
            )
        if amount < 0:
            raise ValueError(
                f"resource '{res_name}' amount in {where} must be non-negative, got {amount!r}"
            )


def validate_payload(payload: dict, schema: type) -> list:
    """Validate payload against a TypedDict schema using runtime type hints.

    Returns a list of error strings. Empty list means valid.
    Uses typing.get_type_hints() for runtime type resolution (Appendix D.1).
    Handles parameterized generics (e.g. list[str]) via typing.get_origin(),
    and Union/Optional types (e.g. str | None) by checking against member types.

    Note: Validation is intentionally shallow — nested TypedDict fields are
    checked only at the top level (isinstance dict), not recursively. This
    keeps validation fast and avoids deep-type-resolution complexity.

    （never-raise 契约）：本函数**永不抛异常**——schema 畸形（Union 含
    Any/TypeVar 等不可 isinstance 的成员）或 payload 形状异常时，返回
    ``["schema error: ..."]`` 而非上抛。调用链（_dispatch_job → except
    Exception → requeue + raise）会让校验系统自身的故障打崩整个 run 并
    形成无限崩溃循环；校验故障应视为校验失败（job 进 DLQ），绝不崩 run。
    """
    try:
        return _validate_payload_impl(payload, schema)
    except Exception as e:
        # 兜底：任何未预期异常（含未来 schema 类型演化）→ 校验失败
        return [f"schema error: {type(e).__name__}: {e}"]


def _validate_payload_impl(payload: dict, schema: type) -> list:
    errors = []
    # payload 必须为 dict——非 dict（str/int/None）会在下方
    # `key not in payload`/`payload[key]` 处抛 TypeError（str 索引 int、
    # int 不可迭代），上抛到 _dispatch_job → 整个 run 崩溃。入口返回错误串。
    if not isinstance(payload, dict):
        return [f"payload must be a dict, got {type(payload).__name__}"]
    try:
        hints = typing.get_type_hints(schema)
    except Exception as e:
        return [f"schema resolution failed: {e}"]
    # TypedDict(total=False) 的可选字段允许缺失；无 __required_keys__
    # 属性时视为全必填。
    required_keys = getattr(schema, "__required_keys__", None)
    for key, expected_type in hints.items():
        if key not in payload:
            if required_keys is not None and key not in required_keys:
                continue
            errors.append(f"missing required field '{key}'")
        else:
            value = payload[key]
            origin = typing.get_origin(expected_type)
            if origin is typing.Literal:
                # Literal[...] 不能传给 isinstance，直接比较取值范围。
                # True == 1 会让 bool 值穿过 Literal[1, 2]；
                # 判定用「值相等且类型一致」——True 拒绝、1 接受。
                if not any(
                    value == allowed and type(value) is type(allowed)
                    for allowed in typing.get_args(expected_type)
                ):
                    type_name = str(expected_type)
                    errors.append(
                        f"field '{key}' expected {type_name}, "
                        f"got {type(value).__name__}"
                    )
                continue
            # bool 是 int 的子类：int 字段显式拒绝 True/False（防止载荷类型静默错位）
            if origin is None and expected_type is int and isinstance(value, bool):
                errors.append(
                    f"field '{key}' expected int, got bool"
                )
                continue
            # Unwrap parameterized generics (e.g. list[str] -> list).
            # Union types (str | None / Optional[str]) must be checked against
            # their member types so None is accepted for Optional fields.
            is_union = (origin is typing.Union or (_UnionType is not None and origin is _UnionType))
            if is_union:
                check_type = tuple(
                    (typing.get_origin(a) or a) for a in typing.get_args(expected_type)
                )
            else:
                check_type = origin or expected_type
            try:
                valid = isinstance(value, check_type)
            except TypeError:
                # union 含 Literal 成员时 isinstance(value, Literal[...])
                # 抛 TypeError 会被误吞（valid=True → 校验形同虚设）——对
                # Literal 成员做值比较（与顶层 Literal 分支同款），
                # 其余成员仍走 isinstance。
                literal_members = [
                    a for a in typing.get_args(expected_type)
                    if typing.get_origin(a) is typing.Literal
                ]
                if literal_members:
                    non_literal = [
                        (typing.get_origin(a) or a) for a in typing.get_args(expected_type)
                        if typing.get_origin(a) is not typing.Literal
                    ]
                    literal_ok = any(
                        value == allowed and type(value) is type(allowed)
                        for m in literal_members
                        for allowed in typing.get_args(m)
                    )
                    # non_literal 可能含 Any/TypeVar 等不可 isinstance 的
                    # 成员——过滤掉（isinstance 会再抛 TypeError）。
                    # 不可 isinstance 的成员按「放行」处理（无法静态校验）。
                    isinst_ok = True
                    for t in non_literal:
                        if t is typing.Any or isinstance(t, typing.TypeVar):
                            continue  # 不可 isinstance，放行
                        try:
                            if not isinstance(value, t):
                                isinst_ok = False
                                break
                        except TypeError:
                            continue  # 仍不可 isinstance，放行
                    valid = literal_ok or isinst_ok
                else:
                    valid = True
            if valid and is_union and isinstance(value, bool):
                # isinstance(True, int) 为 True，Union 含 int
                # 成员时 Optional[int] 会放行 True/False。与 int 字段的
                # bool 拒绝语义对齐：成员含 int 但无 bool 时拒绝 bool 值
                # （Optional/None 的接受不受影响）。
                member_types = tuple(
                    typing.get_origin(a) or a for a in typing.get_args(expected_type)
                )
                if int in member_types and bool not in member_types:
                    valid = False
            if not valid:
                type_name = getattr(expected_type, '__name__', str(expected_type))
                errors.append(
                    f"field '{key}' expected {type_name}, "
                    f"got {type(value).__name__}"
                )
    for key in payload:
        if key not in hints:
            errors.append(f"unexpected field '{key}'")
    return errors
