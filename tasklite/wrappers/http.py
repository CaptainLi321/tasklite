"""可选 HTTP 请求、响应与快照工具，本版不承诺稳定兼容。

请求返回原始响应，传输异常原样传播。handler 可显式返回
response.to_task_result()，响应状态本身不会改变引擎重试预算。
"""
from __future__ import annotations

import email.utils
import hashlib
import http.client
import math
import os
import random
import sqlite3
import time
from collections.abc import Callable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from pathlib import Path
from typing import Any
import urllib.error
import urllib.parse
import urllib.request

from ..models.result import TaskResult
from ..utils.encoding import encode_job_component
from ..utils.jsonutil import dumps as _json_dumps, loads as _json_loads


# 本地就地重试的单次等待封顶——大 max_retries 下无封顶的指数递增等待会
# 累计出巨量不可中断睡眠（wrapper 层自防；核心引擎不含等待计算，重试
# 节奏唯一出口是 RequeuePolicy seam）。
_LOCAL_RETRY_WAIT_MAX_SECONDS = 300.0


# ==============================================================================
# 1. 基础容器与 Cookie 转换工具
# ==============================================================================

def _read_cookie_text(file_or_content: str | Path) -> str:
    """读取 cookies.txt 文件内容；输入判定为文本内容时原样返回。

    路径形态启发式：无换行、无制表符、且是已存在路径或 .txt 结尾——
    其余一律按文本内容处理。文件不存在或读取失败返回空串（空文档语义，
    解析结果为空字典）。
    """
    if isinstance(file_or_content, Path):
        if not file_or_content.exists():
            return ""
        try:
            return file_or_content.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
    text_str = str(file_or_content).strip()
    if (
        "\n" not in text_str
        and ("\t" not in text_str)
        and (os.path.exists(text_str) or text_str.endswith(".txt"))
    ):
        path = Path(text_str)
        if not path.exists():
            return ""
        try:
            return path.read_text(encoding="utf-8", errors="replace")
        except OSError:
            return ""
    return text_str


def _parse_cookie_lines(content: str) -> dict[str, str]:
    """逐行解析 Netscape/MozillaCookieJar 文档为 Cookie 字典。

    - 自动跳过空行与 '#' 注释行；
    - 兼容包含 '#HttpOnly_' 前缀的行；
    - 同名 Cookie 后出现的覆盖先出现的（符合浏览器覆盖规则）；
    - 标准 7 列（含域列）取第 6/7 列，宽容 6 列形态取第 5/6 列。
    """
    cookies: dict[str, str] = {}
    for line in content.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("#HttpOnly_"):
            line = line[len("#HttpOnly_"):]
        elif line.startswith("#"):
            continue

        parts = line.split("\t")
        if len(parts) >= 7:
            name = parts[5].strip()
            value = parts[6].strip()
            if name:
                cookies[name] = value
        elif len(parts) == 6:
            name = parts[4].strip()
            value = parts[5].strip()
            if name:
                cookies[name] = value
    return cookies


def parse_netscape_cookies(file_or_content: str | Path) -> dict[str, str]:
    """解析 Netscape / MozillaCookieJar 格式 cookies.txt 为字典。

    - 自动跳过空行与 '#' 注释行；
    - 兼容包含 '#HttpOnly_' 前缀的行；
    - 同名 Cookie 后出现的覆盖先出现的（符合浏览器覆盖规则）；
    - 若文件不存在或为空，返回空字典。

    Args:
        file_or_content: cookies.txt 文件路径（Path 或 str）或直接传入的文本内容。

    Returns:
        解析提取的 Cookie 字典 ``{cookie_name: cookie_value}``。
    """
    return _parse_cookie_lines(_read_cookie_text(file_or_content))


def format_cookie_header(cookies: Mapping[str, str]) -> str:
    """将 Cookie 字典格式化为 'Cookie: k=v; k2=v2' 请求头字符串。

    Args:
        cookies: Cookie 键值对映射。

    Returns:
        格式化后的 Cookie 请求头字符串。
    """
    return "; ".join(f"{k}={v}" for k, v in cookies.items())


class HttpResponse:
    """不可变 HTTP 响应容器。

    为不同底层请求库（urllib、requests、httpx 等）提供统一的只读响应数据视图。
    """

    __slots__ = ("_status_code", "_headers", "_body", "_url", "_text_cache")

    def __init__(
        self,
        status_code: int,
        headers: Mapping[str, str],
        body: bytes | str,
        url: str = "",
    ) -> None:
        """初始化 HttpResponse。

        Args:
            status_code: HTTP 状态码。
            headers: 响应头字典（内部自动归一化为小写键名）。
            body: 响应体内容（字节或 UTF-8 字符串）。
            url: 产生该响应的请求 URL。
        """
        self._status_code = int(status_code)
        # 归一化 headers 为全小写键名
        self._headers = {str(k).lower(): str(v) for k, v in headers.items()}
        if isinstance(body, str):
            self._body = body.encode("utf-8")
        else:
            self._body = bytes(body)
        self._url = str(url)
        self._text_cache: str | None = None

    @property
    def status_code(self) -> int:
        """HTTP 响应状态码。"""
        return self._status_code

    @property
    def headers(self) -> dict[str, str]:
        """HTTP 响应头字典（小写键名）。"""
        return dict(self._headers)

    @property
    def body(self) -> bytes:
        """原始响应体字节。"""
        return self._body

    @property
    def url(self) -> str:
        """请求 URL。"""
        return self._url

    @property
    def ok(self) -> bool:
        """是否为成功响应（200 <= status_code < 300）。"""
        return 200 <= self._status_code < 300

    @property
    def text(self) -> str:
        """UTF-8 解码后的文本内容（带替换容错）。"""
        if self._text_cache is None:
            self._text_cache = self._body.decode("utf-8", errors="replace")
        return self._text_cache

    def json(self) -> Any:
        """解析响应文本为 JSON 对象。"""
        return _json_loads(self.text)

    def header(self, name: str, default: str | None = None) -> str | None:
        """大小写不敏感获取指定响应头。"""
        return self._headers.get(name.lower(), default)

    def to_task_result(self) -> TaskResult:
        """显式将响应转为任务结果；不执行睡眠、派发或预算修改。"""
        metadata: dict[str, Any] = {"status_code": self.status_code}
        message = f"HTTP {self.status_code}"
        if self.status_code == 429:
            metadata["retry_after"] = _retry_after(self.headers)
        if self.status_code == 429 or self.status_code >= 500:
            return TaskResult.retry(message=message, metadata=metadata)
        if 200 <= self.status_code < 400:
            return TaskResult.success(message=message, metadata=metadata)
        return TaskResult.failure(message=message, metadata=metadata)

    def __repr__(self) -> str:
        return f"<HttpResponse [{self._status_code}] len={len(self._body)}>"


# ==============================================================================
# HTTP 响应结果转换与快照准入
# ==============================================================================

_TRANSIENT_NO_SNAPSHOT_STATUSES = frozenset({408, 425, 429})


def _retry_after(headers: Mapping[str, str]) -> float | None:
    """解析 Retry-After 秒数或 HTTP 日期，未知或无效时返回 None。"""
    value = next((v for k, v in headers.items() if k.lower() == "retry-after"), None)
    if not value:
        return None
    try:
        seconds = float(value)
    except ValueError:
        try:
            parsed = email.utils.parsedate_tz(value)
            seconds = email.utils.mktime_tz(parsed) - time.time() if parsed else 0.0
        except (ValueError, OverflowError, TypeError):
            return None
    return seconds if math.isfinite(seconds) and seconds > 0 else None


# ==============================================================================
# 3. 快照去重与离线重放（SnapshotStore）
# ==============================================================================

class SnapshotStore:
    """快照存储基类协议。

    内容寻址的原始 HTTP 响应缓存，支持断点重试与离线重放。
    """

    @staticmethod
    def make_key(
        url: str,
        method: str = "GET",
        params: Mapping[str, Any] | None = None,
        body: bytes | str | Mapping[str, Any] | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> str:
        """规范化生成请求唯一键（URL 参数排序 + Method 大写 + Body/身份头哈希）。

        Args:
            url: 目标 URL。
            method: HTTP 方法（GET、POST 等）。
            params: 查询参数字典。
            body: 请求体内容（bytes、str 或 dict）。
            headers: 请求头 Mapping（仅身份头白名单参与指纹）。

        Returns:
            规范化的请求键。params 非空时附加类型前缀指纹段；
            None 值参数与 requests 语义对齐全链路丢弃（线上请求与省略该
            参数等价，key 亦相同），norm_query 以 str(v) 镜像线上 URL 形态。
        """
        parsed = urllib.parse.urlparse(url.strip())
        query_items = SnapshotStore._canonical_query_items(parsed, params)
        norm_query = urllib.parse.urlencode(query_items)
        norm_url = urllib.parse.urlunparse(
            (parsed.scheme.lower(), parsed.netloc.lower(), parsed.path, parsed.params, norm_query, "")
        )

        norm_method = method.strip().upper()

        body_hash = ""
        if body:
            raw_bytes = SnapshotStore._body_bytes(body)
            body_hash = f"_{hashlib.sha256(raw_bytes).hexdigest()[:16]}"

        url_component = encode_job_component(norm_url)
        params_hash = SnapshotStore._params_fingerprint(params)
        header_hash = SnapshotStore._identity_headers_fingerprint(headers)
        return f"{norm_method}::{url_component}{body_hash}{params_hash}{header_hash}"

    @staticmethod
    def _canonical_query_items(
        parsed: urllib.parse.ParseResult,
        params: Mapping[str, Any] | None,
    ) -> list[tuple[str, str]]:
        """合并 URL 内嵌 query 与显式 params 并排序（同语义请求同键）。"""
        query_items: list[tuple[str, str]] = []
        if parsed.query:
            query_items.extend(urllib.parse.parse_qsl(parsed.query, keep_blank_values=True))
        if params:
            for k, v in params.items():
                if v is None:
                    continue
                query_items.append((str(k), str(v)))
        query_items.sort()
        return query_items

    # 纳入快照 key 的身份头白名单：仅取真正改变响应语义的凭证/协商头。
    # 取舍：不纳入全量 headers——User-Agent/Accept-Encoding/Date 等易变头会把
    # 同语义请求碎片化为不同 key，摧毁命中率且无正确性收益；漏纳凭证头则会在
    # 换 Cookie/Authorization 后串号返回旧身份的响应。
    _IDENTITY_KEY_HEADERS: tuple[str, ...] = (
        "authorization",
        "proxy-authorization",
        "cookie",
        "x-api-key",
        "api-key",
        "content-type",
    )

    @classmethod
    def _identity_headers_fingerprint(cls, headers: Any) -> str:
        """身份头指纹段（单射）：白名单内非空头归一化小写排序后哈希。

        ``::h`` 标记处于像集安全位——url_component 经单射转义后不含字面 `:`
        （forbidden 集 ``/\\%`` 全部 %XX 转义），key 中 `::` 仅出现于 METHOD
        分隔与本标记，段结构无歧义，与 url/body 段组合不引入碰撞。
        """
        if not headers:
            return ""
        try:
            items = {str(k).lower(): str(v) for k, v in dict(headers).items()}
        except (TypeError, ValueError):
            return ""
        present = sorted((name, items[name]) for name in cls._IDENTITY_KEY_HEADERS if items.get(name))
        if not present:
            return ""
        return "::h" + hashlib.sha256(_json_dumps(present).encode("utf-8")).hexdigest()[:16]

    @classmethod
    def _params_fingerprint(cls, params: Any) -> str:
        """params 指纹段（单射）：值统一经 _value_fingerprint 类型前缀出口。

        不变式：同一 params 映射的不同序列化形态不得共享快照 key。None 值
        参数在指纹、norm_query 与两侧 fetch 的 wire 上一致丢弃（requests
        语义），丢弃后与省略该参数同 key 同 wire；字面 "None" 值仍保留参与
        指纹，与 None 不碰撞。排序后序列化以消除 dict 插入顺序差异
        （_json_dumps 不做键排序，直接序列化整个映射会使同语义映射因顺序
        不同而 key 漂移）。
        """
        if not params:
            return ""
        try:
            items = sorted(
                (str(k), cls._value_fingerprint(v))
                for k, v in dict(params).items()
                if v is not None
            )
        except (TypeError, ValueError):
            return ""
        if not items:
            return ""
        return "::p" + hashlib.sha256(_json_dumps(items).encode("utf-8")).hexdigest()[:16]

    # 参与 key 语义指纹的 body 类参数名（requests: data/json；本仓库
    # requests_fetch: json_data；urllib/httpx 风格: body/content）。
    # files 等文件对象参数无法稳定序列化，不参与指纹。
    _BODY_KEY_PARAMS: tuple[str, ...] = ("body", "content", "data", "json", "json_data")

    # 请求身份类关键字实参（requests 风格 cookies=/auth=）：与身份头白名单同
    # 机制进入指纹，换身份必须换 key，否则换身份方会零网络消耗地静默命中
    # 旧身份的快照响应。取舍：Session 级身份（session.headers / session.cookies）
    # 不参与 key——会话对象跨请求可变（cookie jar 随响应演化）会使 key 漂移，
    # 且异构 session 对象无统一内省接口；需要身份隔离的调用方应经
    # headers/cookies 关键字实参显式传递身份。
    _IDENTITY_KEY_KWARGS: tuple[str, ...] = ("cookies", "auth")

    @staticmethod
    def _value_fingerprint(value: Any) -> str:
        """单值指纹（单射）：类型前缀隔离不同形态，bytes 与同内容 str 零碰撞；
        不可 JSON 化对象（含 NaN）退化 repr 指纹（进程内确定性），保底不同请求
        不得静默共享快照。"""
        if isinstance(value, (bytes, bytearray)):
            return "bytes:" + bytes(value).hex()
        try:
            return "json:" + _json_dumps(value)
        except (TypeError, ValueError):
            return "repr:" + repr(value)

    @classmethod
    def _request_fingerprint(
        cls,
        args: tuple[Any, ...],
        kwargs: Mapping[str, Any],
    ) -> dict[str, Any] | None:
        """聚合位置实参与 body 类/身份类关键字实参，生成参与快照 key 的语义指纹。

        不变式（单射）：不同实参组合必须映射到不同指纹——位置实参按序号进入
        arg{i} 命名空间（与 body/身份类参数名结构性不相交），body 类参数
        按参数名聚合全部已提供项（不能只取第一个非空项）。
        """
        parts: dict[str, Any] = {}
        for idx, value in enumerate(args):
            parts[f"arg{idx}"] = cls._value_fingerprint(value)
        for name in cls._BODY_KEY_PARAMS:
            value = kwargs.get(name)
            if value is None:
                continue
            parts[name] = cls._value_fingerprint(value)
        for name in cls._IDENTITY_KEY_KWARGS:
            value = kwargs.get(name)
            # 身份语义与身份头白名单同机制：空值等价于未携带身份，不参与指纹
            if not value:
                continue
            parts[name] = cls._value_fingerprint(value)
        return parts or None

    @staticmethod
    def _body_bytes(body: bytes | str | Mapping[str, Any]) -> bytes:
        """请求体统一序列化：dict/list 走 JSON、str 编码 UTF-8、其余按字节。"""
        if isinstance(body, (dict, list)):
            return _json_dumps(body).encode("utf-8")
        if isinstance(body, str):
            return body.encode("utf-8")
        return bytes(body)  # type: ignore[arg-type]

    @staticmethod
    def _extract_response_fields(res: Any) -> tuple[int, dict[str, str], bytes]:
        """从任意响应形态提取 (status_code, headers, body)。

        兼容 HttpResponse、requests/httpx 响应（status_code/content）、
        urllib 原生响应（getcode/read）、dict/list 数据体与任意可 str 化值。
        状态默认 None 使 getcode 回退可达（urllib 原生 HTTPResponse 只有
        getcode()）：默认 200 会把真实状态恒记为 200 并写入快照；无状态
        语义的返回值（dict/list 数据体等）按成功 200 记录。
        """
        if isinstance(res, HttpResponse):
            return res.status_code, res.headers, res.body
        status_code: int | None = getattr(res, "status_code", None)
        if status_code is None and hasattr(res, "getcode"):
            status_code = res.getcode()
        if status_code is None:
            status_code = 200
        headers = dict(getattr(res, "headers", {}))
        if hasattr(res, "content"):
            raw_body = res.content
        elif hasattr(res, "read"):
            raw_body = res.read()
        elif isinstance(res, (dict, list)):
            raw_body = _json_dumps(res).encode("utf-8")
        else:
            raw_body = str(res).encode("utf-8")
        return int(status_code), headers, bytes(raw_body)

    def has(self, key: str) -> bool:
        """检查是否存在有效快照。"""
        raise NotImplementedError

    def get(self, key: str) -> HttpResponse | None:
        """获取快照响应，不存在返回 None。"""
        raise NotImplementedError

    def put(
        self,
        key: str,
        url: str,
        status_code: int,
        headers: Mapping[str, str],
        body: bytes | str | Mapping[str, Any],
        method: str = "GET",
    ) -> None:
        """保存快照。"""
        raise NotImplementedError

    def cached(
        self,
        fetch_fn: Callable[..., Any],
        key_func: Callable[..., str] | None = None,
        ignore_statuses: Sequence[int] = (429, 500, 502, 503, 504, 520, 521, 522, 524),
    ) -> Callable[..., HttpResponse]:
        """包装请求函数，提供透明的快照缓存拦截。

        Args:
            fetch_fn: 底层网络请求函数。
            key_func: 自定义 Key 生成函数（默认使用 make_key）。
            ignore_statuses: 额外不写入快照的状态码（408/425/429 与全部 5xx 始终保底跳过）。

        Returns:
            包装后的缓存函数。
        """
        def wrapped(url: str, *args: Any, **kwargs: Any) -> HttpResponse:
            method = kwargs.get("method", "GET")
            params = kwargs.get("params", None)
            body = self._request_fingerprint(args, kwargs)

            if key_func is not None:
                key = key_func(url, *args, **kwargs)
            else:
                key = self.make_key(url, method=method, params=params, body=body, headers=kwargs.get("headers"))

            cached_resp = self.get(key)
            if cached_resp is not None:
                return cached_resp

            res = fetch_fn(url, *args, **kwargs)
            status_code, headers, raw_body = self._extract_response_fields(res)

            # 不变式：瞬态状态（408/425/429 与全部 5xx）保底不写入快照
            # （防离线重放污染）；ignore_statuses 仅可在此基础上追加豁免
            # 状态码，不可收缩保底谓词。
            if not (
                status_code in _TRANSIENT_NO_SNAPSHOT_STATUSES
                or status_code >= 500
                or status_code in ignore_statuses
            ):
                self.put(
                    key=key,
                    url=url,
                    status_code=status_code,
                    headers=headers,
                    body=raw_body,
                    method=method,
                )

            return HttpResponse(status_code=status_code, headers=headers, body=raw_body, url=url)

        return wrapped


class SQLiteSnapshotStore(SnapshotStore):
    """基于 SQLite WAL 模式的持久化快照存储。

    使用独立的 SQLite 数据库文件保存原始 HTTP 响应，支持断电保护与并发读取。
    """

    def __init__(self, db_path: str | Path) -> None:
        """初始化 SQLite 快照库。

        Args:
            db_path: 数据库文件路径（如 './snapshots.db'）。
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def _get_conn(self) -> Iterator[sqlite3.Connection]:
        conn = sqlite3.connect(str(self.db_path), timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self._get_conn() as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS http_snapshots (
                    request_key TEXT PRIMARY KEY,
                    url TEXT NOT NULL,
                    method TEXT NOT NULL,
                    status_code INTEGER NOT NULL,
                    headers_json TEXT NOT NULL,
                    body BLOB NOT NULL,
                    created_at REAL NOT NULL
                )
                """
            )
            conn.commit()

    def has(self, key: str) -> bool:
        with self._get_conn() as conn:
            cur = conn.execute("SELECT 1 FROM http_snapshots WHERE request_key = ?", (key,))
            return cur.fetchone() is not None

    def get(self, key: str) -> HttpResponse | None:
        with self._get_conn() as conn:
            cur = conn.execute(
                "SELECT url, status_code, headers_json, body FROM http_snapshots WHERE request_key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row is None:
                return None
            url, status_code, headers_json, body = row
            try:
                headers = _json_loads(headers_json)
            except Exception:
                headers = {}
            return HttpResponse(status_code=status_code, headers=headers, body=body, url=url)

    def put(
        self,
        key: str,
        url: str,
        status_code: int,
        headers: Mapping[str, str],
        body: bytes | str | Mapping[str, Any],
        method: str = "GET",
    ) -> None:
        body_bytes = self._body_bytes(body)
        headers_dict = {str(k): str(v) for k, v in headers.items()}
        headers_json = _json_dumps(headers_dict)
        now = time.time()

        with self._get_conn() as conn:
            conn.execute(
                """
                INSERT INTO http_snapshots (request_key, url, method, status_code, headers_json, body, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(request_key) DO UPDATE SET
                    url=excluded.url,
                    method=excluded.method,
                    status_code=excluded.status_code,
                    headers_json=excluded.headers_json,
                    body=excluded.body,
                    created_at=excluded.created_at
                """,
                (key, url, method.upper(), int(status_code), headers_json, body_bytes, now),
            )
            conn.commit()


class MemorySnapshotStore(SnapshotStore):
    """纯内存快照存储（适合测试与临时会话）。"""

    def __init__(self) -> None:
        self._data: dict[str, HttpResponse] = {}

    def has(self, key: str) -> bool:
        return key in self._data

    def get(self, key: str) -> HttpResponse | None:
        return self._data.get(key)

    def put(
        self,
        key: str,
        url: str,
        status_code: int,
        headers: Mapping[str, str],
        body: bytes | str | Mapping[str, Any],
        method: str = "GET",
    ) -> None:
        self._data[key] = HttpResponse(
            status_code=status_code,
            headers={str(k): str(v) for k, v in headers.items()},
            body=self._body_bytes(body),
            url=url,
        )


# ==============================================================================
# 4. 组合式执行器（HttpExecutor 深模块）
# ==============================================================================

class HttpExecutor:
    """组合快照缓存与有限的本地请求重试，独立于 Job 重试预算。"""

    def __init__(
        self,
        snapshot_store: SnapshotStore | None = None,
        max_retries: int = 0,
        retry_delay_base: float = 1.0,
    ) -> None:
        self.snapshot_store = snapshot_store
        self.max_retries = max_retries
        self.retry_delay_base = retry_delay_base

    @staticmethod
    def _retry_wait(attempt: int, base: float) -> float:
        """本地重试第 attempt 次的等待秒数（指数递增封顶 + 抖动）。

        形状：min(base, 封顶) × 2^(attempt-1) 再封顶，±25% 抖动——
        先封顶基数再乘幂，杜绝大 base 大幂次的浮点溢出。
        """
        if not math.isfinite(base) or base <= 0:
            return 0.0
        exp = min(attempt - 1, 60)
        delay = min(min(base, _LOCAL_RETRY_WAIT_MAX_SECONDS) * (2 ** exp), _LOCAL_RETRY_WAIT_MAX_SECONDS)
        jitter = random.uniform(-0.25, 0.25) * delay
        return max(0.0, delay + jitter)

    def execute(
        self,
        fetch_fn: Callable[..., Any],
        *args: Any,
        **kwargs: Any,
    ) -> Any:
        """执行请求；仅在本地重试自然网络异常与 5xx，429 立即返回。"""
        target_fn = fetch_fn
        if self.snapshot_store is not None:
            target_fn = self.snapshot_store.cached(target_fn)
        for attempt in range(max(0, self.max_retries) + 1):
            if attempt > 0:
                time.sleep(self._retry_wait(attempt, self.retry_delay_base))
            try:
                response = target_fn(*args, **kwargs)
            except (urllib.error.URLError, TimeoutError, ConnectionError, http.client.HTTPException) as exc:
                if isinstance(exc, urllib.error.HTTPError):
                    raise
                if attempt >= self.max_retries:
                    raise
                continue
            status = getattr(response, "status_code", None)
            if status is None and hasattr(response, "getcode"):
                status = response.getcode()
            if status is None or status < 500 or attempt >= self.max_retries:
                return response
        raise AssertionError("request loop must return or raise")

    def wrap(self, fetch_fn: Callable[..., Any]) -> Callable[..., Any]:
        """将请求函数包装为绑定当前执行器配置的函数。"""
        def _wrapped(*args: Any, **kwargs: Any) -> Any:
            return self.execute(fetch_fn, *args, **kwargs)
        return _wrapped


# ==============================================================================
# 5. 内置标准请求辅助函数（urllib_fetch & requests_fetch）
# ==============================================================================

def _merge_params_into_url(url: str, params: Mapping[str, Any] | None) -> str:
    """把显式 params 合并进 URL query（None 值丢弃，与 requests 语义对齐）。

    None 值参数若上链（str(v) 拼接），同一 params 映射在两后端线上请求
    不同却共享同一快照 key，静默串快照。
    """
    if not params:
        return url
    parsed = urllib.parse.urlparse(url)
    q = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    for k, v in params.items():
        if v is None:
            continue
        q.append((str(k), str(v)))
    new_query = urllib.parse.urlencode(q)
    return urllib.parse.urlunparse(
        (parsed.scheme, parsed.netloc, parsed.path, parsed.params, new_query, parsed.fragment)
    )


def _build_urllib_opener(final_url: str, proxies: Mapping[str, str] | None):
    """构造 opener：显式代理优先；localhost/127.0.0.1 默认不走环境代理。"""
    if proxies is not None:
        return urllib.request.build_opener(urllib.request.ProxyHandler(dict(proxies)))
    if "127.0.0.1" in final_url or "localhost" in final_url:
        return urllib.request.build_opener(urllib.request.ProxyHandler({}))
    return urllib.request.build_opener()


def urllib_fetch(
    url: str,
    *,
    method: str = "GET",
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    data: bytes | str | Mapping[str, Any] | None = None,
    timeout: float = 30.0,
    proxies: Mapping[str, str] | None = None,
) -> HttpResponse:
    """基于 Python 标准库 urllib 的 HTTP 请求实现（零外部依赖）。

    Args:
        url: 目标 URL。
        method: HTTP 方法（GET、POST 等）。
        headers: 请求头。
        params: 查询参数。
        data: 请求体（支持 dict/list 自动序列化为 JSON、字符串或 bytes）。
        timeout: 超时秒数（默认 30.0s）。
        proxies: 显式代理配置（访问 localhost/127.0.0.1 时默认不走环境变量代理）。

    Returns:
        响应容器对象（HTTPError 形态的 4xx/5xx 响应同样以容器返回而非抛出）。
    """
    final_url = _merge_params_into_url(url, params)

    req_body: bytes | None = None
    req_headers = dict(headers) if headers else {}

    if data is not None:
        if isinstance(data, (dict, list)):
            req_body = _json_dumps(data).encode("utf-8")
            if "content-type" not in {k.lower(): v for k, v in req_headers.items()}:
                req_headers["Content-Type"] = "application/json"
        elif isinstance(data, str):
            req_body = data.encode("utf-8")
        else:
            req_body = bytes(data)  # type: ignore[arg-type]

    req = urllib.request.Request(
        url=final_url,
        data=req_body,
        headers=req_headers,
        method=method.upper(),
    )

    opener = _build_urllib_opener(final_url, proxies)

    try:
        with opener.open(req, timeout=timeout) as resp:
            status_code = resp.getcode()
            resp_headers = dict(resp.headers)
            body = resp.read()
            http_resp = HttpResponse(status_code=status_code, headers=resp_headers, body=body, url=final_url)
            return http_resp
    except urllib.error.HTTPError as e:
        err_headers = dict(e.headers) if hasattr(e, "headers") else {}
        err_body = e.read() if hasattr(e, "read") else b""
        err_resp = HttpResponse(status_code=e.code, headers=err_headers, body=err_body, url=final_url)
        return err_resp


def _prepare_requests_session(
    requests_mod: Any,
    url: str,
    session: Any,
    proxies: Mapping[str, str] | None,
    trust_env: bool | None,
) -> tuple[Any, bool]:
    """构造/复用 requests Session；返回 (session, 是否本次创建需关闭)。"""
    if session is not None:
        return session, False
    sess = requests_mod.Session()
    if trust_env is not None:
        sess.trust_env = trust_env
    elif "127.0.0.1" in url or "localhost" in url:
        sess.trust_env = False
    if proxies is not None:
        sess.proxies = dict(proxies)
    return sess, True


def requests_fetch(
    url: str,
    *,
    session: Any | None = None,
    method: str = "GET",
    headers: Mapping[str, str] | None = None,
    params: Mapping[str, Any] | None = None,
    data: Any = None,
    json_data: Any = None,
    timeout: float = 30.0,
    proxies: Mapping[str, str] | None = None,
    trust_env: bool | None = None,
    **kwargs: Any,
) -> HttpResponse:
    """基于 requests 库的 HTTP 请求实现（当环境中安装了 requests 时可用）。

    Args:
        url: 目标 URL。
        session: requests.Session 实例（必须在当前子进程内创建）。
        method: HTTP 方法。
        headers: 请求头。
        params: 查询参数。
        data: 表单或原始数据。
        json_data: JSON 数据（对应 requests 的 json 参数）。
        timeout: 超时秒数。
        proxies: 显式代理配置。
        trust_env: 是否使用环境变量中的代理设置。
        **kwargs: 透传给 requests 的其它参数。

    Returns:
        响应容器对象。
    """
    try:
        import requests
    except ImportError as e:
        raise RuntimeError("requests_fetch requires 'requests' package to be installed") from e

    req_kwargs: dict[str, Any] = {
        "method": method.upper(),
        "url": url,
        "headers": headers,
        "params": params,
        "data": data,
        "timeout": timeout,
        **kwargs,
    }
    if json_data is not None:
        req_kwargs["json"] = json_data

    sess, created_session = _prepare_requests_session(requests, url, session, proxies, trust_env)

    try:
        resp = sess.request(**req_kwargs)
        http_resp = HttpResponse(
            status_code=resp.status_code,
            headers=dict(resp.headers),
            body=resp.content,
            url=resp.url,
        )

        return http_resp
    finally:
        if created_session:
            sess.close()



__all__ = [
    "parse_netscape_cookies",
    "format_cookie_header",
    "HttpResponse",
    "HttpExecutor",
    "SnapshotStore",
    "SQLiteSnapshotStore",
    "MemorySnapshotStore",
    "urllib_fetch",
    "requests_fetch",
]
