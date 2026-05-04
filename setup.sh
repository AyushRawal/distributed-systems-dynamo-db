#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BACKEND_DIR="$ROOT_DIR/backend"
DASHBOARD_DIR="$ROOT_DIR/admin-dashboard"
VENV_DIR="$ROOT_DIR/.venv"
REQ_FILE="$BACKEND_DIR/requirements.txt"
INSTALL_BENCHMARK_DEPS=false

info() {
  printf '[setup] %s\n' "$1"
}

fail() {
  printf '[setup] ERROR: %s\n' "$1" >&2
  exit 1
}

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || fail "Missing required command: $1"
}

usage() {
  cat <<EOF
Usage: bash setup.sh [--with-benchmark]

Options:
  --with-benchmark   Install optional Python dependencies for backend/benchmark.py
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-benchmark)
      INSTALL_BENCHMARK_DEPS=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      fail "Unknown option: $1"
      ;;
  esac
done

info "Checking required tools"
require_cmd go
require_cmd bash
require_cmd curl
require_cmd lsof
require_cmd pkill

info "Downloading backend Go modules"
(cd "$BACKEND_DIR" && go mod download)

info "Downloading admin dashboard Go modules"
(cd "$DASHBOARD_DIR" && go mod download)

info "Building backend"
(cd "$BACKEND_DIR" && go build ./...)

info "Building admin dashboard"
(cd "$DASHBOARD_DIR" && go build ./...)

info "Running backend unit tests"
(cd "$BACKEND_DIR" && go test ./...)

if [[ "$INSTALL_BENCHMARK_DEPS" == true ]]; then
  require_cmd python3

  if python3 -m venv --help >/dev/null 2>&1; then
    info "Creating Python virtual environment at .venv"
    python3 -m venv "$VENV_DIR"
    "$VENV_DIR/bin/python" -m pip install --upgrade pip
    "$VENV_DIR/bin/pip" install -r "$REQ_FILE"
  else
    info "python3 venv unavailable, installing Python packages with --user"
    python3 -m pip install --user -r "$REQ_FILE"
  fi
fi

chmod +x "$BACKEND_DIR/run_cluster.sh" "$BACKEND_DIR/test_dynamo.sh"

cat <<EOF

Setup completed successfully.

Next steps:
1. Start the backend cluster:
   cd "$BACKEND_DIR" && ./run_cluster.sh
2. In a second terminal, start the admin dashboard:
   cd "$DASHBOARD_DIR" && go run main.go
3. Run the end-to-end test suite:
   cd "$BACKEND_DIR" && bash test_dynamo.sh
EOF

if [[ "$INSTALL_BENCHMARK_DEPS" == true ]]; then
  cat <<EOF

Optional benchmark environment:
- Python packages were installed using $REQ_FILE
- If a virtual environment was created, activate it with:
  source "$VENV_DIR/bin/activate"
EOF
else
  cat <<EOF

Benchmark dependencies were not installed.
To install them later, run:
  bash setup.sh --with-benchmark
EOF
fi
