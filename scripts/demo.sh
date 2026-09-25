#!/bin/sh
# End-to-end check. Requires Linux, g++, sha256sum, and wc.
# The workspace is under /tmp, which is often tmpfs. Compare with a
# directory on ext4 by passing another path as $1.
set -eu

ROOT=$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)
WS=${1:-/tmp/forge-demo-ws}
FORGE="$ROOT/forge"
SAMPLE="$WS/sample.bin"

rm -rf "$WS"
mkdir -p "$WS"
chmod +x "$ROOT/scripts/sleep60.sh"

"$FORGE" self-test

dd if=/dev/urandom of="$SAMPLE" bs=1048576 count=4 status=none

"$FORGE" run --workspace "$WS" --slots 2 >"$WS/controller.out" 2>&1 &
PID=$!
trap 'kill -TERM "$PID" 2>/dev/null || true; wait "$PID" 2>/dev/null || true' EXIT

i=0
while [ "$i" -lt 50 ]; do
  if "$FORGE" status --workspace "$WS" | grep -q "controller: running"; then
    break
  fi
  i=$((i + 1))
  sleep 0.1
done
"$FORGE" status --workspace "$WS" | grep -q "controller: running"

"$FORGE" submit --workspace "$WS" --input "$SAMPLE" --operation sha256 --io mmap --wait
ID=$(ls "$WS/jobs" | sort -n | tail -1)
GOT=$(tr -d '\n' < "$WS/jobs/$ID/output")
EXP=$(sha256sum "$SAMPLE" | awk '{print $1}')
[ "$GOT" = "$EXP" ]
echo "sha256 matches sha256sum"

"$FORGE" submit --workspace "$WS" --input "$SAMPLE" --operation count --workers 4 --io read --wait
ID=$(ls "$WS/jobs" | sort -n | tail -1)
GOT=$(tr -d '\n' < "$WS/jobs/$ID/output")
EXP=$(wc -c < "$SAMPLE" | tr -d ' ')
[ "$GOT" = "$EXP" ]
echo "count matches wc -c"

"$FORGE" submit --workspace "$WS" --input "$SAMPLE" --operation copy --io mmap --wait
ID=$(ls "$WS/jobs" | sort -n | tail -1)
cmp "$SAMPLE" "$WS/jobs/$ID/output"
echo "copy matches cmp"

"$FORGE" submit --workspace "$WS" --operation exec --exec "$ROOT/scripts/sleep60.sh" \
  --input "$SAMPLE" --deadline 2 --wait && FAIL=1 || FAIL=0
[ "$FAIL" = 0 ]
echo "deadline fired"

# A second controller must fail while the lock is held, including during exec.
"$FORGE" submit --workspace "$WS" --operation exec --exec "$ROOT/scripts/sleep60.sh" \
  --input "$SAMPLE" >/dev/null
if "$FORGE" run --workspace "$WS" --slots 1; then
  echo "second controller was allowed to start" >&2
  exit 1
fi
echo "second controller refused (flock held)"

"$FORGE" storage stats --workspace "$WS" | head -n 20
"$FORGE" device list | head -n 5

kill -TERM "$PID"
wait "$PID" || true
trap - EXIT
echo "demo ok"
