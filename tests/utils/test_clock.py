"""utils/clock 单点测试：UTC ISO 格式与源码级裸时钟引用门禁。"""

from __future__ import annotations

import pathlib
import re

from tasklite.utils.clock import utc_now_iso

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
