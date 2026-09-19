"""发布声明契约测试：锁定 pyproject 元数据与支持面承诺的一致性。"""

from pathlib import Path

import pytest

tomllib = pytest.importorskip("tomllib")

SUPPORTED_PYTHONS = ["3.10", "3.11", "3.12", "3.13", "3.14"]

PYPROJECT = Path(__file__).resolve().parents[1] / "pyproject.toml"


def _load_pyproject():
    with PYPROJECT.open("rb") as f:
        return tomllib.load(f)


def test_classifiers_declare_full_interpreter_support():
    classifiers = _load_pyproject()["project"]["classifiers"]
    declared = {
        c.rsplit(" ", 1)[-1]
        for c in classifiers
        if c.startswith("Programming Language :: Python :: 3.")
    }
    assert set(SUPPORTED_PYTHONS) <= declared


def test_license_expression_excludes_license_classifier():
    project = _load_pyproject()["project"]
    assert project["license"] == "MIT"
    license_classifiers = [c for c in project["classifiers"] if c.startswith("License ::")]
    assert license_classifiers == []
