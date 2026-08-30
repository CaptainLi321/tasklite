"""Exception types for tasklite.

错误分类模型：

三类异常在 handler 中的处理待遇完全不同：

| 类别 | 包含 | 处理 |
|------|------|------|
| **Fatal** | `FatalError` + `FATAL_EXCEPTIONS` | 直接 DLQ，不消耗重试次数（确定性 bug） |
| **Transient** | `RetryError` + `TRANSIENT_EXCEPTIONS` + 用户注册的瞬态类 | 自动重试（退避等待），达上限进 DLQ |
| **Unknown** | 其余 `Exception` | 永久失败进 DLQ（与 Fatal 同路径，但元数据标记未知） |

设计决策：
- `RetryError` 是 Transient 的显式入口（业务主动声明「这次是瞬态」）。
- `TRANSIENT_EXCEPTIONS` 覆盖常见瞬时故障（网络连接/超时/远端错误）——
  业务无需手包 RetryError 即可让这些异常自动重试。
- `pipeline.register_transient_exception(cls)`（per-pipeline 注册表）允许
  业务把自有库的异常（如 requests.exceptions.ConnectionError）注册为瞬态。
- 三分类 + 可配置白名单使网络抖动/超时这类天然瞬态错误自动重试而非
  直接判死；`ValueError` 不入 FATAL_EXCEPTIONS——它含 JSONDecodeError
  等可能瞬态的子类，直接判死会误杀可重试失败。
"""

from typing import Optional


class PipelineError(Exception):
    """Base class for all tasklite framework exceptions."""
    pass


class RetryError(PipelineError):
    """Raise this in a handler to signal a transient failure. The job will be pushed back to the queue."""
    pass


class FatalError(PipelineError):
    """Non-retryable error. Goes directly to DLQ without consuming retries.

    Raise this for code-level bugs (invalid config, missing files that will
    never appear, logic errors).
    """
    pass


class RateLimitHit(RetryError):
    """HTTP 429 信号。

    ``RateLimitHit`` 是 ``RetryError`` 子类：裸抛即按瞬态退避重试。
    裸抛降级语义——handler 忘记在重试前挂起资源时，不会因「未匹配任何
    分类」按 Unknown 判死进 DLQ + 级联下游，仍走退避重试（代价是限流
    资源在重试间隔内未被挂起）。需要在重试前挂起资源时，由调用方自行
    调用资源挂起逻辑后抛 ``RetryError``。
    """
    pass


# 确定性 bug 类：直接 DLQ，不重试。不含 ValueError——它包含 JSONDecodeError
# 等可能瞬态的子类（由 TRANSIENT_EXCEPTIONS / register_transient_exception
# 另行归类）。
FATAL_EXCEPTIONS = (TypeError, KeyError, AttributeError,
                    IndexError, StopIteration, ArithmeticError,
                    ImportError, NotImplementedError, RecursionError)


# 常见瞬态故障：自动重试（与 RetryError 同等待遇）。
# ConnectionError/TimeoutError 是 OSError 子类；RemoteDisconnected 等
# 网络断开异常在此被自动归类为可重试，业务无需手包 RetryError。
TRANSIENT_EXCEPTIONS = (
    ConnectionError,
    TimeoutError,
    ConnectionRefusedError,
    ConnectionResetError,
    ConnectionAbortedError,
    BrokenPipeError,
)

def _validate_transient_class(exception_cls: type) -> None:
    """注册入口校验（fail-loud）——per-pipeline 注册表与外部直调共用。"""
    if not isinstance(exception_cls, type) or not issubclass(exception_cls, Exception):
        raise TypeError(
            f"register_transient_exception requires an Exception subclass, got {exception_cls!r}"
        )
    if issubclass(exception_cls, (RetryError, FatalError)):
        # 专用 except 分支优先于注册表，注册必然静默无效，入口直接拒绝
        raise TypeError(
            f"register_transient_exception cannot register a "
            f"{'RetryError' if issubclass(exception_cls, RetryError) else 'FatalError'} "
            f"subclass ({exception_cls!r}) — these have dedicated except branches "
            f"that bypass the registry; registration would silently no-op."
        )
    try:
        import pickle
        pickle.dumps(exception_cls)
    except (pickle.PicklingError, AttributeError, TypeError) as e:
        # 函数作用域定义的类无法被 spawn 子进程 import → 注册必然失效。
        # 在此 fail-loud，把「子进程静默判死」提前为「注册时显式报错」。
        raise TypeError(
            f"register_transient_exception requires a module-level (picklable) "
            f"Exception class for spawn-subprocess propagation, got {exception_cls!r}: {e}"
        ) from e


class TransientRegistry:
    """**per-pipeline** 瞬态异常注册表。

    注册在父进程、分类在子进程：注册表归 ``TaskLite`` 实例所有，
    子进程分类只消费 ``TaskContext`` 携带的 ``snapshot()``（不可变
    tuple）——无模块级可变注册表参与子进程决策，杜绝跨 pipeline
    累积与跨 run 泄漏。
    """

    def __init__(self) -> None:
        self._classes: list = []

    def register(self, exception_cls: type) -> None:
        """把业务自有异常类注册为瞬态（自动重试），幂等。

        用于第三方库的异常（如 ``requests.exceptions.ConnectionError``）。
        模块级类才可通过 pickle 预检（spawn 子进程分类依赖 ctx 快照）。
        """
        _validate_transient_class(exception_cls)
        if exception_cls not in self._classes:
            self._classes.append(exception_cls)

    def snapshot(self) -> tuple:
        """返回不可变注册表快照（随 ctx 显式下发子进程）。"""
        return tuple(self._classes)

    def matches(self, exc: BaseException) -> bool:
        """exc 是否命中本注册表（父进程侧测试/诊断用）。"""
        return any(isinstance(exc, cls) for cls in self._classes)


def _matches_registry(exc: BaseException, registry) -> bool:
    """纯判定：exc 是否命中给定注册表快照（tuple/TransientRegistry 皆可）。"""
    classes = registry.snapshot() if isinstance(registry, TransientRegistry) else tuple(registry or ())
    return any(isinstance(exc, cls) for cls in classes)


def classify_exception(
    exc: BaseException,
    registry=(),
    *,
    fatal_exceptions: Optional[tuple] = None,
    transient_exceptions: Optional[tuple] = None,
) -> str:
    """异常三分类的**唯一生产语义**，返回 retry/fatal/error。

    分类顺序：RetryError → FatalError → 注册表（用户显式声明）→
    FATAL_EXCEPTIONS（内置启发式）→ TRANSIENT_EXCEPTIONS → Unknown。
    注册表判定必须在 FATAL_EXCEPTIONS 之前——用户显式声明永远优先于内置
    启发式（否则注册 FATAL 子类会被 FATAL 分支短路）。

    ``registry`` 是 ``ctx.transient_registry`` 下发的快照（不可变 tuple）；
    子进程调用本函数**不读任何模块级可变状态**。

    ``fatal_exceptions``/``transient_exceptions``：per-pipeline 覆盖——
    与瞬态注册表同纪律，确定性/瞬态内置启发式的成员集合也可按 pipeline
    定制（None=用模块默认元组）。快照随 ctx 下发子进程（可 pickle 的
    tuple），分类决策不读模块级可变全局。
    """
    if isinstance(exc, RetryError):
        return "retry"
    if isinstance(exc, FatalError):
        return "fatal"
    if _matches_registry(exc, registry):
        return "retry"
    # is not None 判定（非真值判定）：空元组 = 显式「本 pipeline 无内置
    # 判死/瞬态成员」，必须与 None（用模块默认）区分。
    _fatal = FATAL_EXCEPTIONS if fatal_exceptions is None else tuple(fatal_exceptions)
    _transient = TRANSIENT_EXCEPTIONS if transient_exceptions is None else tuple(transient_exceptions)
    if isinstance(exc, _fatal):
        return "fatal"
    if isinstance(exc, _transient):
        return "retry"
    return "error"


def is_transient_exception(exc: BaseException, registry=()) -> bool:
    """判断异常是否属于瞬态（应自动重试）——分类语义的公开只读助手。

    ``registry`` 缺省为空快照；测试/文档调用方如需包含注册表项，应传入
    ``pipeline.transient_registry.snapshot()`` 或 ``TransientRegistry``。
    生产侧分类一律走 ``classify_exception``（executor 子进程入口）。
    """
    return classify_exception(exc, registry) == "retry"


class _CommitCrashSignal(BaseException):
    """内部信号：commit 失败后已 requeue，需立即崩溃。

    继承 ``BaseException``（而非 Exception）——从调用纪律变成结构保证：
    任何 ``except Exception`` 兜底在类型系统层面捕不到它，无需依赖「except 顺序」约定。
    它穿透一切 ``except Exception`` 直到 ``_run_loop`` 的显式分支（commit 失败需崩溃语义），
    中途不会被误吞。不导出为公共 API（内部信号）。
    """
    pass


class _JobTerminated(BaseException):
    """内部信号：当前 job 已终结（DLQ 阈值命中），需立即停止处理。

    继承 ``BaseException``——理由同 ``_CommitCrashSignal``：``except
    Exception`` 在类型系统层面捕不到它，漏捕调用点不会被静默 requeue
    （那是复发）。捕获点**必须**在 ``_dispatch_job``/``_complete_job``/
    ``_restore_stale_result`` 内层（语义是「当前 job 停止处理」→ 返回 None），
    **绝不应逃逸到 _run_loop**——若逃逸说明有调用点漏捕，应上抛暴露
    而非静默吞掉（否则未来类漏洞被掩盖）。

    与 _CommitCrashSignal 的区别：前者表达「commit 失败、系统需崩溃重启」
    （on-disk 队列保留，at-least-once 重跑）；本异常表达「job 已被判定为
    确定性坏输入、正常终结」（不崩溃、不重跑）。语义不同，分开捕获。
    """
    pass
