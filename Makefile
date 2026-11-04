# TaskLite 开发 Makefile
# 用 `make help` 查看所有命令

.DEFAULT_GOAL := help

# Python 解释器（优先用 python3，回退 python）
PYTHON ?= python3
PYTEST := $(PYTHON) -m pytest
MYPY := $(PYTHON) -m mypy

.PHONY: help typecheck test test-fast test-hypothesis test-matrix coverage install-hooks clean

help:  ## 显示所有可用命令
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

typecheck:  ## 运行 mypy 静态类型检查（配置见 pyproject.toml [tool.mypy]）
	$(MYPY)

test:  ## 运行全部测试（含 hypothesis 属性测试；数量以 pytest --collect-only 实测为准）
	$(PYTEST) tests/ -q -p no:cacheprovider --timeout=60

test-fast:  ## 跳过 hypothesis，跑快速测试（约 8s）
	$(PYTEST) tests/ -q -p no:cacheprovider -m "not hypothesis" --timeout=60

test-hypothesis:  ## 仅跑 hypothesis 属性测试
	$(PYTEST) tests/ -q -p no:cacheprovider -m "hypothesis" --timeout=120

test-matrix:  ## 多解释器矩阵冒烟 + mypy 静态类型检查（遍历本地 3.10..3.14 执行快速测试与类型提示求值冒烟）
	@echo "=== Running mypy static type check ==="
	$(MYPY)
	@for py in python3.10 python3.11 python3.12 python3.13 python3.14; do \
		if command -v $$py >/dev/null 2>&1; then \
			echo "=== Testing import, typing & multi-process E2E with $$py ==="; \
			PYTHONPATH=. $$py scripts/verify_matrix.py || exit 1; \
			if $$py -c 'import pytest' >/dev/null 2>&1; then \
				echo "=== Running pytest with $$py ==="; \
				$$py -m pytest tests/ -q -p no:cacheprovider -m "not hypothesis" || exit 1; \
			fi \
		fi \
	done
	@echo "✓ Multi-interpreter matrix checks passed."

coverage:  ## 生成覆盖率报告（输出到终端 + 缺失行号）
	$(PYTEST) tests/ --cov=tasklite --cov-report=term-missing --timeout=60

install-hooks:  ## 安装 git pre-push 钩子（push 前自动跑 mypy 与快速测试）
	@./scripts/install-git-hooks.sh

clean:  ## 清理测试与构建产物
	rm -rf .pytest_cache .mypy_cache .coverage htmlcov *.egg-info .mutmut-cache build dist
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "✓ Cleaned build artifacts"
