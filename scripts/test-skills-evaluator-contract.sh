#!/usr/bin/env bash
# Runs the real skills-cassette <-> skills-evaluator HTTP contracts.
#
# Two production applications are started as separate processes: this
# cassette's server binary (in-memory store, generation disabled) and the
# skills-evaluator FastAPI app behind its deterministic test-only judge. The
# evaluator resolves every durable revision read through the live cassette, so
# both the stateless candidate contract and the exact-revision contract are
# exercised without a mock of either service.
set -euo pipefail

skills_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
evaluator_root=${SKILLS_EVALUATOR_DIR:-"$skills_root/../skills-evaluator"}

if [[ ! -f "$evaluator_root/skills_evaluator/server.py" ]]; then
  echo "skills-evaluator checkout not found at $evaluator_root" >&2
  exit 1
fi

free_port() {
  python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
}

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/skills-evaluator-contract.XXXXXX")
cassette_log="$work_dir/skills-cassette.log"
evaluator_log="$work_dir/skills-evaluator.log"
cassette_pid=""
evaluator_pid=""
cleanup() {
  for pid in "$evaluator_pid" "$cassette_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" 2>/dev/null || true
      wait "$pid" 2>/dev/null || true
    fi
  done
  rm -rf "$work_dir"
}
trap cleanup EXIT

wait_ready() {
  local name=$1 url=$2 pid=$3 log=$4
  for _ in $(seq 1 100); do
    if curl --fail --silent "$url" >/dev/null; then
      return 0
    fi
    if ! kill -0 "$pid" 2>/dev/null; then
      cat "$log" >&2
      echo "$name exited before becoming ready" >&2
      exit 1
    fi
    sleep 0.1
  done
  cat "$log" >&2
  echo "$name did not become ready" >&2
  exit 1
}

cassette_port=$(free_port)
evaluator_port=$(free_port)
cassette_url="http://127.0.0.1:$cassette_port"
evaluator_url="http://127.0.0.1:$evaluator_port"

(cd "$skills_root" && go build -o "$work_dir/skills-cassette" ./cli/skills-cassette)
(
  cd "$work_dir"
  exec env -u TAPES_DATABASE_URL -u CASSETTE_CORE_URL -u CASSETTE_FILTERS \
    CASSETTE_NAME=skills \
    "$work_dir/skills-cassette" serve --listen "127.0.0.1:$cassette_port"
) >"$cassette_log" 2>&1 &
cassette_pid=$!
wait_ready skills-cassette "$cassette_url/ping" "$cassette_pid" "$cassette_log"

(
  cd "$evaluator_root"
  exec direnv exec . env \
    SKILLS_EVALUATOR_CONTRACT_PORT="$evaluator_port" \
    SKILLS_CASSETTE_CONTRACT_URL="$cassette_url" \
    uv run python tests/contract_server.py
) >"$evaluator_log" 2>&1 &
evaluator_pid=$!
wait_ready skills-evaluator "$evaluator_url/ping" "$evaluator_pid" "$evaluator_log"

cd "$skills_root"
SKILLS_EVALUATOR_CONTRACT_URL="$evaluator_url/api/skills-evaluator/candidate-evaluations" \
SKILLS_EVALUATOR_CONTRACT_BASE_URL="$evaluator_url/api/skills-evaluator" \
SKILLS_CASSETTE_CONTRACT_URL="$cassette_url" \
  go test ./internal/evaluator -ginkgo.focus='real cassette contract'
