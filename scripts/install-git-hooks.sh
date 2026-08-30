#!/bin/bash
# 安装 git pre-push hook 到 .git/hooks/pre-push
# 由 `make install-hooks` 调用

set -e

# 找到仓库根目录（.git 所在）
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

if [ ! -d "$REPO_ROOT/.git" ]; then
    echo "ERROR: $REPO_ROOT is not a git repository (no .git directory)."
    echo "Run 'git init' first, or use this script inside a git repo."
    exit 1
fi

HOOK_DEST="$REPO_ROOT/.git/hooks/pre-push"
SOURCE="$SCRIPT_DIR/pre-push"

# 备份已存在的 hook（如果不是本脚本安装的）
if [ -f "$HOOK_DEST" ] && ! grep -q "tasklite_pre-push" "$HOOK_DEST" 2>/dev/null; then
    BACKUP="$HOOK_DEST.backup.$(date +%s)"
    mv "$HOOK_DEST" "$BACKUP"
    echo "Backed up existing pre-push hook to: $BACKUP"
fi

cp "$SOURCE" "$HOOK_DEST"
chmod +x "$HOOK_DEST"

echo "Installed pre-push hook to: $HOOK_DEST"
echo ""
echo "Behavior on push:"
echo "  Runs:   pytest tests/ -q -m 'not hypothesis' --timeout=60"
echo "  Log:    /tmp/tasklite_pre-push.log"
echo "  Bypass: git push --no-verify"
echo ""
echo "To uninstall: rm $HOOK_DEST"
