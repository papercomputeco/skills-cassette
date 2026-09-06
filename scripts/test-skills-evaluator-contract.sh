#!/usr/bin/env bash
set -euo pipefail

skills_root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
evaluator_root=${SKILLS_EVALUATOR_DIR:-"$skills_root/../skills-evaluator"}

if [[ ! -f "$evaluator_root/skills_evaluator/server.py" ]]; then
  echo "skills-evaluator checkout not found at $evaluator_root" >&2
  exit 1
fi

port=$(python3 - <<'PY'
import socket
with socket.socket() as listener:
    listener.bind(("127.0.0.1", 0))
    print(listener.getsockname()[1])
PY
)
log_file=$(mktemp "${TMPDIR:-/tmp}/skills-evaluator-contract.XXXXXX.log")
server_pid=""
cleanup() {
  if [[ -n "$server_pid" ]]; then
    kill "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
  fi
  rm -f "$log_file"
}
trap cleanup EXIT

(
  cd "$evaluator_root"
  exec direnv exec . env SKILLS_EVALUATOR_CONTRACT_PORT="$port" \
    uv run python tests/contract_server.py
) >"$log_file" 2>&1 &
server_pid=$!

for _ in $(seq 1 100); do
  if curl --fail --silent "http://127.0.0.1:$port/ping" >/dev/null; then
    break
  fi
  if ! kill -0 "$server_pid" 2>/dev/null; then
    cat "$log_file" >&2
    exit 1
  fi
  sleep 0.1
done

if ! curl --fail --silent "http://127.0.0.1:$port/ping" >/dev/null; then
  cat "$log_file" >&2
  echo "skills-evaluator contract server did not become ready" >&2
  exit 1
fi

cd "$skills_root"
SKILLS_EVALUATOR_CONTRACT_URL="http://127.0.0.1:$port/api/skills-evaluator/candidate-evaluations" \
  go test ./internal/evaluator \
  -ginkgo.focus=real_cassettes_exchange_candidate_evaluation_contract
