"""utils/clock 单点测试：UTC ISO 格式与源码级裸时钟引用门禁。"""

from __future__ import annotations

import pathlib
import re

from tasklite.utils.clock import mtime_ns_to_us, parse_utc_iso_to_us, utc_now_iso

V2_ROOT = (
    pathlib.Path(__file__).resolve().parents[3] / "tasklite" / "v2"
)

# 裸时钟表达式形态：datetime.now(...) / timezone.utc —— UTC ISO 时间戳
# 的生成收敛于 utils/clock.py 单点，其余模块出现即为绕过单点的新写点。
_BARE_CLOCK_PATTERN = re.compile(r"datetime\.now|timezone\.utc")


class TestUtcNowIso:
    """utc_now_iso 格式契约。"""

    def test_returns_utc_aware_iso_string(self):
        ts = utc_now_iso()
        assert ts.endswith("+00:00")
        assert "T" in ts

    def test_monotonic_non_decreasing(self):
        assert utc_now_iso() <= utc_now_iso()


class TestParseUtcIsoToUs:
    """parse_utc_iso_to_us 整数微秒转换与时区校验。"""

    def test_epoch_conversion_exact(self):
        assert parse_utc_iso_to_us("1970-01-01T00:00:00+00:00") == 0
        assert parse_utc_iso_to_us("1970-01-01T00:00:01.123456+00:00") == 1_123_456

    def test_handles_z_suffix(self):
        assert parse_utc_iso_to_us("1970-01-01T00:00:00Z") == 0
        assert parse_utc_iso_to_us("1970-01-01T00:00:00z") == 0

    def test_arbitrary_timezone_normalizes_to_utc(self):
        # +08:00 北京时间 08:00:00 等同于 UTC 00:00:00
        assert parse_utc_iso_to_us("1970-01-01T08:00:00+08:00") == 0

    def test_naive_timestamp_rejected(self):
        import pytest

        with pytest.raises(ValueError, match="timezone-aware"):
            parse_utc_iso_to_us("1970-01-01T00:00:00")

    def test_invalid_types_and_empty_strings_rejected(self):
        import pytest

        with pytest.raises(TypeError):
            parse_utc_iso_to_us(None)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            parse_utc_iso_to_us(12345)  # type: ignore[arg-type]
        with pytest.raises(ValueError):
            parse_utc_iso_to_us("")
        with pytest.raises(Exception):
            parse_utc_iso_to_us("not-a-timestamp")


class TestMtimeNsToUs:
    """mtime_ns_to_us 纳秒向微秒安全截断转换。"""

    def test_integer_division_and_truncation(self):
        assert mtime_ns_to_us(0) == 0
        assert mtime_ns_to_us(999) == 0
        assert mtime_ns_to_us(1_000) == 1
        assert mtime_ns_to_us(1_999) == 1
        assert mtime_ns_to_us(1_700_000_000_123_456_789) == 1_700_000_000_123_456

    def test_type_guards(self):
        import pytest

        with pytest.raises(TypeError):
            mtime_ns_to_us("1000")  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            mtime_ns_to_us(True)  # type: ignore[arg-type]
        with pytest.raises(TypeError):
            mtime_ns_to_us(None)  # type: ignore[arg-type]


def test_no_bare_clock_references_outside_clock_module():
    """v2 源码不得在 utils/clock.py 之外出现裸时钟表达式（单点门禁）。"""
    offenders: list[str] = []
    for path in sorted(V2_ROOT.rglob("*.py")):
        if path.name == "clock.py":
            continue
        content = path.read_text(encoding="utf-8")
        for lineno, line in enumerate(content.splitlines(), start=1):
            if _BARE_CLOCK_PATTERN.search(line):
                offenders.append(f"{path.relative_to(V2_ROOT)}:{lineno}")
    assert not offenders, (
        f"发现绕过 utils/clock 单点的裸时钟引用（应改调 utc_now_iso）: "
        f"{offenders}"
    )
