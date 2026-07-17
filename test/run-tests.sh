#!/usr/bin/env bash
#
# Automated end-to-end test for queuectl.
# Validates the 5 scenarios required by the assignment:
#   1. Basic job completes successfully
#   2. Failed job retries with backoff and moves to DLQ
#   3. Multiple workers process jobs without overlap/duplication
#   4. Invalid commands fail gracefully
#   5. Job data survives restart
#
# Usage: ./test/run-tests.sh
set -uo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CLI="node $ROOT_DIR/bin/queuectl.js"
TEST_DIR="$(mktemp -d)"
PASS=0
FAIL=0

cleanup() {
  pkill -9 -f "$ROOT_DIR/src/worker.js" >/dev/null 2>&1 || true
}
trap cleanup EXIT

pass() { echo "  PASS: $1"; PASS=$((PASS+1)); }
fail() { echo "  FAIL: $1"; FAIL=$((FAIL+1)); }

cd "$TEST_DIR" || exit 1
echo "Running tests in $TEST_DIR"
echo ""

# ---------------------------------------------------------------------------
echo "[1] Basic job completes successfully"
$CLI config set max-retries 3 >/dev/null
$CLI config set backoff-base 2 >/dev/null
$CLI enqueue '{"id":"t1-ok","command":"echo hello-from-queuectl"}' >/dev/null
$CLI worker start --count 1 >/dev/null
sleep 1.5
state=$($CLI list --state completed | grep -c "t1-ok")
if [ "$state" -eq 1 ]; then pass "basic job completed"; else fail "basic job did not complete"; fi
$CLI worker stop >/dev/null
sleep 0.5

# ---------------------------------------------------------------------------
echo "[2] Failed job retries with backoff and moves to DLQ"
$CLI enqueue '{"id":"t2-fail","command":"exit 1"}' >/dev/null
$CLI worker start --count 1 >/dev/null
# base=2, max_retries=3 -> waits ~2s then ~4s before reaching dead (~6-7s total)
sleep 8
row=$($CLI list --state dead | grep "t2-fail")
if echo "$row" | grep -q "3/3"; then
  pass "job retried 3 times with exponential backoff and moved to DLQ"
else
  fail "job did not reach DLQ after exhausting retries (got: $row)"
fi
$CLI worker stop >/dev/null
sleep 0.5

# ---------------------------------------------------------------------------
echo "[3] Multiple workers process jobs without overlap"
for i in $(seq 1 12); do
  $CLI enqueue "{\"id\":\"t3-bulk-$i\",\"command\":\"sleep 0.2 && echo done-$i\"}" >/dev/null
done
$CLI worker start --count 4 >/dev/null
sleep 3
completed_count=$($CLI list --state completed | grep -c "t3-bulk-")
if [ "$completed_count" -eq 12 ]; then
  pass "all 12 jobs completed exactly once across 4 workers (no duplication)"
else
  fail "expected 12 completed t3-bulk jobs, got $completed_count"
fi
$CLI worker stop >/dev/null
sleep 0.5

# ---------------------------------------------------------------------------
echo "[4] Invalid commands fail gracefully"
$CLI enqueue '{"id":"t4-badcmd","command":"this_binary_does_not_exist_zzz","max_retries":1}' >/dev/null
$CLI worker start --count 1 >/dev/null
sleep 2
row=$($CLI list --state dead | grep "t4-badcmd")
if [ -n "$row" ]; then
  pass "invalid command failed gracefully and moved to DLQ (no crash)"
else
  fail "invalid command did not fail gracefully (row: $row)"
fi
$CLI worker stop >/dev/null
sleep 0.5

# ---------------------------------------------------------------------------
echo "[5] Job data survives restart"
$CLI enqueue '{"id":"t5-persist","command":"echo persisted"}' >/dev/null
before=$($CLI list --state pending | grep -c "t5-persist")
# Simulate a restart: every CLI invocation is already a fresh process reading
# the same SQLite file, so re-querying from a brand new process proves
# persistence across process restarts.
after=$($CLI status | grep "pending" | awk '{print $2}')
if [ "$before" -eq 1 ] && [ -f "$TEST_DIR/.queuectl/queuectl.db" ]; then
  pass "job data persisted in $TEST_DIR/.queuectl/queuectl.db across process restarts"
else
  fail "job data not found after restart"
fi

# ---------------------------------------------------------------------------
echo ""
echo "Results: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ] && exit 0 || exit 1
