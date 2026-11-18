#!/usr/bin/env bash
# Tạo môi trường Python cục bộ (.venv) để VS Code nhận thư viện (hết báo đỏ, có gợi ý code).
# Chỉ phục vụ soạn code. Pipeline vẫn chạy trong Docker (scripts/run_*.sh) để đúng phiên bản của đề.
# Dùng: scripts/setup_local_env.sh
set -euo pipefail
cd "$(dirname "$0")/.."
if ! command -v uv >/dev/null 2>&1; then
  echo "Chưa có uv. Cài bằng: curl -LsSf https://astral.sh/uv/install.sh | sh  (hoặc brew install uv)" >&2
  exit 1
fi
uv venv .venv --python 3.12          # uv tự tải Python 3.12 nếu máy chưa có
uv pip install --python .venv/bin/python -r requirements.txt
.venv/bin/python -c "import pyspark, yaml, pymongo, sklearn; print('OK: Python', __import__('sys').version.split()[0], '| pyspark', pyspark.__version__)"
echo "Xong. Trong VS Code: Cmd+Shift+P > 'Python: Select Interpreter' > chọn .venv nếu chưa tự nhận."
