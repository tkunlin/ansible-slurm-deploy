#!/usr/bin/env bash
set -euo pipefail

PARTITION="debug"
WORKDIR="/tmp"
SECONDS_TO_RUN=60
NODES=3
NTASKS_PER_NODE=1
CPUS_PER_TASK=1
NODELIST=""
MODE="help"

usage() {
  cat <<'USAGE'
Usage:
  slurm_value_tests_fixed.sh pi [options]
  slurm_value_tests_fixed.sh prime [options]
  slurm_value_tests_fixed.sh stress-pi [options]

Modes:
  pi         Multi-task numerical integration of PI. Outputs computed PI and error.
  prime      Multi-task prime counting in a range. Outputs total prime count.
  stress-pi  CPU burn for a duration, then compute PI and report elapsed time.

Options:
  -p, --partition NAME      Slurm partition (default: debug)
  -w, --workdir PATH        Working directory on compute nodes (default: /tmp)
  -t, --seconds N           Duration in seconds for stress-pi (default: 60)
  -n, --nodes N             Number of nodes (default: 3)
  --ntasks-per-node N       Tasks per node (default: 1)
  -c, --cpus-per-task N     CPUs per task (default: 1)
  -l, --nodelist LIST       Explicit node list, e.g. slurm-c01,slurm-c02
  -h, --help                Show this help

Examples:
  ./slurm_value_tests_fixed.sh pi -n 3
  ./slurm_value_tests_fixed.sh prime -n 3 --ntasks-per-node 2
  ./slurm_value_tests_fixed.sh stress-pi -t 120 -n 3 -c 2
USAGE
}

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "[ERROR] missing command: $1" >&2; exit 1; }
}

parse_args() {
  [[ $# -ge 1 ]] || { usage; exit 1; }
  MODE="$1"; shift
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -p|--partition) PARTITION="$2"; shift 2 ;;
      -w|--workdir) WORKDIR="$2"; shift 2 ;;
      -t|--seconds) SECONDS_TO_RUN="$2"; shift 2 ;;
      -n|--nodes) NODES="$2"; shift 2 ;;
      --ntasks-per-node) NTASKS_PER_NODE="$2"; shift 2 ;;
      -c|--cpus-per-task) CPUS_PER_TASK="$2"; shift 2 ;;
      -l|--nodelist) NODELIST="$2"; shift 2 ;;
      -h|--help) usage; exit 0 ;;
      *) echo "[ERROR] unknown option: $1" >&2; usage; exit 1 ;;
    esac
  done
}

build_srun_args() {
  SRUN_ARGS=(srun --chdir="$WORKDIR" -p "$PARTITION" -N "$NODES" --ntasks-per-node="$NTASKS_PER_NODE" -c "$CPUS_PER_TASK")
  if [[ -n "$NODELIST" ]]; then
    SRUN_ARGS+=(-w "$NODELIST")
  fi
}

b64() {
  base64 -w0
}

run_pi() {
  need_cmd srun
  local total_tasks=$(( NODES * NTASKS_PER_NODE ))
  echo "[INFO] PI numerical integration"
  echo "[INFO] partition=$PARTITION nodes=$NODES ntasks_per_node=$NTASKS_PER_NODE total_tasks=$total_tasks cpus_per_task=$CPUS_PER_TASK workdir=$WORKDIR"
  [[ -n "$NODELIST" ]] && echo "[INFO] nodelist=$NODELIST"

  build_srun_args

  local py_b64
  py_b64=$(cat <<'PY' | b64
import os
rank = int(os.environ.get("SLURM_PROCID", "0"))
size = int(os.environ.get("SLURM_NTASKS", "1"))
steps = 4000000
local = 0.0
for i in range(rank, steps, size):
    x = (i + 0.5) / steps
    local += 4.0 / (1.0 + x*x)
print(f"PARTIAL {rank} {local}")
PY
)

  "${SRUN_ARGS[@]}" bash -lc "printf '%s' '$py_b64' | base64 -d | python3 -" | awk '
    /^PARTIAL/ {sum += $3}
    END {
      steps = 4000000;
      pi = sum / steps;
      err = pi - 3.141592653589793;
      if (err < 0) err = -err;
      printf("PI_ESTIMATE=%.12f\nABS_ERROR=%.12e\n", pi, err);
    }'
}

run_prime() {
  need_cmd srun
  local total_tasks=$(( NODES * NTASKS_PER_NODE ))
  echo "[INFO] Prime counting"
  echo "[INFO] partition=$PARTITION nodes=$NODES ntasks_per_node=$NTASKS_PER_NODE total_tasks=$total_tasks cpus_per_task=$CPUS_PER_TASK workdir=$WORKDIR"
  [[ -n "$NODELIST" ]] && echo "[INFO] nodelist=$NODELIST"

  build_srun_args

  local py_b64
  py_b64=$(cat <<'PY' | b64
import math, os
rank = int(os.environ.get("SLURM_PROCID", "0"))
size = int(os.environ.get("SLURM_NTASKS", "1"))
start = 1
end = 300000
chunk = (end - start + 1 + size - 1) // size
lo = start + rank * chunk
hi = min(end, lo + chunk - 1)

def is_prime(n):
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False
    r = int(math.isqrt(n))
    f = 3
    while f <= r:
        if n % f == 0:
            return False
        f += 2
    return True

cnt = 0
for n in range(lo, hi + 1):
    if is_prime(n):
        cnt += 1
print(f"PRIME_PARTIAL {rank} {lo} {hi} {cnt}")
PY
)

  "${SRUN_ARGS[@]}" bash -lc "printf '%s' '$py_b64' | base64 -d | python3 -" | awk '
    /^PRIME_PARTIAL/ {sum += $5}
    END {
      printf("PRIME_COUNT_1_TO_300000=%d\n", sum);
    }'
}

run_stress_pi() {
  need_cmd srun
  local total_tasks=$(( NODES * NTASKS_PER_NODE ))
  echo "[INFO] CPU stress + PI verification"
  echo "[INFO] partition=$PARTITION nodes=$NODES ntasks_per_node=$NTASKS_PER_NODE total_tasks=$total_tasks cpus_per_task=$CPUS_PER_TASK seconds=$SECONDS_TO_RUN workdir=$WORKDIR"
  [[ -n "$NODELIST" ]] && echo "[INFO] nodelist=$NODELIST"

  build_srun_args
  export SECONDS_TO_RUN CPUS_PER_TASK

  local py_b64
  py_b64=$(cat <<'PY' | b64
steps = 1000000
s = 0.0
for i in range(steps):
    x = (i + 0.5) / steps
    s += 4.0 / (1.0 + x*x)
pi = s / steps
err = abs(pi - 3.141592653589793)
print(f"POST_STRESS_PI {pi:.12f} {err:.12e}")
PY
)

  "${SRUN_ARGS[@]}" bash -lc '
start=$(date +%s)
if command -v stress-ng >/dev/null 2>&1; then
  stress-ng --cpu "$CPUS_PER_TASK" --timeout "${SECONDS_TO_RUN}s" --metrics-brief >/tmp/stress-ng-${SLURM_PROCID}.log 2>&1
else
  pids=""
  for i in $(seq 1 "$CPUS_PER_TASK"); do
    ( while :; do :; done ) &
    pids="$pids $!"
  done
  sleep "$SECONDS_TO_RUN"
  kill $pids >/dev/null 2>&1 || true
  wait $pids >/dev/null 2>&1 || true
fi
printf "%s" '"$py_b64"' | base64 -d | python3 -
end=$(date +%s)
echo "ELAPSED $((end-start))"
' | awk '
    /^POST_STRESS_PI/ {pi=$2; err=$3; count++}
    /^ELAPSED/ {sum += $2; nodes++}
    END {
      avg = (nodes > 0 ? sum / nodes : 0);
      printf("POST_STRESS_PI_LAST=%s\nPOST_STRESS_ABS_ERROR_LAST=%s\nAVG_ELAPSED_SECONDS=%.2f\nTASK_REPORTS=%d\n", pi, err, avg, count);
    }'
}

main() {
  parse_args "$@"
  case "$MODE" in
    pi) run_pi ;;
    prime) run_prime ;;
    stress-pi) run_stress_pi ;;
    *) usage; exit 1 ;;
  esac
}

main "$@"
