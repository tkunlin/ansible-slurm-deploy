#!/usr/bin/env bash
set -euo pipefail

PARTITION="debug"
NODES=1
NTASKS_PER_NODE=1
CPUS_PER_TASK=1
SECONDS=300
WORKDIR="/tmp"
NODELIST=""
MEM_MB=512
IO_MB=256
IO_BLOCK_KB=1024
PI_STEPS=1200000

usage() {
  cat <<'USAGE'
Usage:
  slurm_stage2_validation.sh <command> [options]

Commands:
  mem       Memory allocation + page touch + checksum test
  io        Local filesystem write/read + digest + throughput test
  burnin    Long-duration mixed burn-in (CPU+MEM+IO) then final PI check

Common options:
  -p, --partition <name>         Slurm partition (default: debug)
  -n, --nodes <N>                Number of nodes (default: 1)
      --ntasks-per-node <N>      Tasks per node (default: 1)
  -c, --cpus-per-task <N>        CPUs per task (default: 1)
  -t, --seconds <N>              Duration in seconds (default: 300)
  -l, --nodelist <a,b,c>         Explicit nodelist
      --workdir <path>           Work directory on compute nodes (default: /tmp)
      --mem-mb <MB>              Memory per task in MB (default: 512)
      --io-mb <MB>               I/O file size per task in MB (default: 256)
      --io-block-kb <KB>         I/O block size in KB (default: 1024)
      --pi-steps <N>             Final PI integration steps per task (default: 1200000)
  -h, --help                     Show help

Examples:
  ./slurm_stage2_validation.sh mem -n 3 --ntasks-per-node 1 --mem-mb 1024 -t 60
  ./slurm_stage2_validation.sh io -n 3 --ntasks-per-node 1 --io-mb 512 --io-block-kb 1024
  ./slurm_stage2_validation.sh burnin -n 3 -c 2 -t 1800 --mem-mb 1024 --io-mb 128
USAGE
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

COMMAND="$1"
shift

while [[ $# -gt 0 ]]; do
  case "$1" in
    -p|--partition) PARTITION="$2"; shift 2 ;;
    -n|--nodes) NODES="$2"; shift 2 ;;
    --ntasks-per-node) NTASKS_PER_NODE="$2"; shift 2 ;;
    -c|--cpus-per-task) CPUS_PER_TASK="$2"; shift 2 ;;
    -t|--seconds) SECONDS="$2"; shift 2 ;;
    -l|--nodelist) NODELIST="$2"; shift 2 ;;
    --workdir) WORKDIR="$2"; shift 2 ;;
    --mem-mb) MEM_MB="$2"; shift 2 ;;
    --io-mb) IO_MB="$2"; shift 2 ;;
    --io-block-kb) IO_BLOCK_KB="$2"; shift 2 ;;
    --pi-steps) PI_STEPS="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "[ERROR] Unknown argument: $1" >&2; usage; exit 1 ;;
  esac
done

TOTAL_TASKS=$(( NODES * NTASKS_PER_NODE ))

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { echo "[ERROR] Missing command: $1" >&2; exit 1; }
}

require_cmd srun
require_cmd python3
require_cmd base64
require_cmd awk

build_srun_cmd() {
  SRUN_CMD=(
    srun
    --chdir="$WORKDIR"
    -p "$PARTITION"
    -N "$NODES"
    --ntasks-per-node "$NTASKS_PER_NODE"
    -c "$CPUS_PER_TASK"
  )
  if [[ -n "$NODELIST" ]]; then
    SRUN_CMD+=( -w "$NODELIST" )
  fi
}

run_python_srun() {
  local code="$1"
  local b64
  b64=$(printf '%s' "$code" | base64 -w0)
  build_srun_cmd
  PYCODE_B64="$b64" "${SRUN_CMD[@]}" --export=ALL,PYCODE_B64 bash -lc '
python3 - <<"PY"
import os, base64
code = base64.b64decode(os.environ["PYCODE_B64"]).decode("utf-8")
exec(compile(code, "<remote>", "exec"))
PY'
}

pi_code() {
cat <<'PY'
import math, os, socket
rank = int(os.getenv("SLURM_PROCID", "0"))
ntasks = int(os.getenv("SLURM_NTASKS", "1"))
steps = int(os.environ["TEST_PI_STEPS"])
host = socket.gethostname()
local = 0.0
for i in range(rank, steps, ntasks):
    x = (i + 0.5) / steps
    local += 4.0 / (1.0 + x * x)
local /= steps
print(f"PITASK host={host} rank={rank} partial={local:.15f}")
PY
}

mem_code() {
cat <<'PY'
import os, time, socket
rank = int(os.getenv("SLURM_PROCID", "0"))
host = socket.gethostname()
mem_mb = int(os.environ["TEST_MEM_MB"])
seconds = int(os.environ["TEST_SECONDS"])
size = mem_mb * 1024 * 1024
page = 4096
buf = bytearray(size)
start = time.time()
for idx in range(0, size, page):
    buf[idx] = ((idx // page) + rank) & 0xFF
checksum = sum(buf[::page]) % 1000000007
left = seconds - (time.time() - start)
if left > 0:
    time.sleep(left)
elapsed = time.time() - start
pages = size // page
print(f"MEMTASK host={host} rank={rank} bytes={size} pages={pages} checksum={checksum} elapsed={elapsed:.2f}")
PY
}

io_code() {
cat <<'PY'
import hashlib, os, socket, time
rank = int(os.getenv("SLURM_PROCID", "0"))
host = socket.gethostname()
workdir = os.environ["TEST_WORKDIR"]
size_mb = int(os.environ["TEST_IO_MB"])
block_kb = int(os.environ["TEST_IO_BLOCK_KB"])
block = block_kb * 1024
total = size_mb * 1024 * 1024
pattern = bytes(((rank + i) & 0xFF) for i in range(block))
fname = os.path.join(workdir, f"slurm_io_rank{rank}_{os.getpid()}.dat")

written = 0
start_w = time.time()
with open(fname, "wb", buffering=0) as f:
    while written < total:
        chunk = min(block, total - written)
        f.write(pattern[:chunk])
        written += chunk
    f.flush()
    os.fsync(f.fileno())
write_sec = time.time() - start_w

md5 = hashlib.md5()
read_bytes = 0
start_r = time.time()
with open(fname, "rb", buffering=0) as f:
    while True:
        data = f.read(block)
        if not data:
            break
        md5.update(data)
        read_bytes += len(data)
read_sec = time.time() - start_r
os.remove(fname)
print(f"IOTASK host={host} rank={rank} bytes={total} write_sec={write_sec:.6f} read_sec={read_sec:.6f} md5={md5.hexdigest()}")
PY
}

burn_code() {
cat <<'PY'
import hashlib, os, socket, time
rank = int(os.getenv("SLURM_PROCID", "0"))
host = socket.gethostname()
seconds = int(os.environ["TEST_SECONDS"])
mem_mb = int(os.environ["TEST_MEM_MB"])
io_mb = int(os.environ["TEST_IO_MB"])
workdir = os.environ["TEST_WORKDIR"]
size = mem_mb * 1024 * 1024
page = 4096
buf = bytearray(size)
for idx in range(0, size, page):
    buf[idx] = ((rank + idx // page) & 0xFF)

io_total = io_mb * 1024 * 1024
io_chunk = 1024 * 1024
pattern = bytes(((rank + i) & 0xFF) for i in range(io_chunk))
fname = os.path.join(workdir, f"slurm_burn_rank{rank}_{os.getpid()}.dat")
deadline = time.time() + seconds
iters = 0
rolling = 0
io_cycles = 0
start = time.time()

while time.time() < deadline:
    x = 0
    for i in range(250000):
        x = (x * 1664525 + 1013904223 + rank + i) & 0xFFFFFFFF

    partial = 0
    for idx in range(0, size, page):
        val = (buf[idx] + 1) & 0xFF
        buf[idx] = val
        partial += val
    rolling = (rolling + partial + x) % 1000000007

    with open(fname, "wb", buffering=0) as f:
        written = 0
        while written < io_total:
            chunk = min(io_chunk, io_total - written)
            f.write(pattern[:chunk])
            written += chunk
        f.flush()
        os.fsync(f.fileno())

    md5 = hashlib.md5()
    with open(fname, "rb", buffering=0) as f:
        while True:
            data = f.read(io_chunk)
            if not data:
                break
            md5.update(data)
    os.remove(fname)
    io_cycles += 1
    rolling = (rolling + int(md5.hexdigest()[:8], 16)) % 1000000007
    iters += 1

elapsed = time.time() - start
print(f"BURNTASK host={host} rank={rank} iters={iters} io_cycles={io_cycles} rolling={rolling} elapsed={elapsed:.2f}")
PY
}

run_mem() {
  echo "[INFO] Memory validation"
  echo "[INFO] partition=$PARTITION nodes=$NODES ntasks_per_node=$NTASKS_PER_NODE total_tasks=$TOTAL_TASKS cpus_per_task=$CPUS_PER_TASK seconds=$SECONDS mem_mb=$MEM_MB workdir=$WORKDIR"
  local output
  output=$(TEST_MEM_MB="$MEM_MB" TEST_SECONDS="$SECONDS" run_python_srun "$(mem_code)")
  printf '%s\n' "$output"
  printf '%s\n' "$output" | awk '
    BEGIN { tasks=0; bytes=0; checksum=0; elapsed=0 }
    /^MEMTASK / {
      tasks++
      for (i=1; i<=NF; i++) {
        split($i, a, "=")
        if (a[1] == "bytes") bytes += a[2]
        if (a[1] == "checksum") checksum += a[2]
        if (a[1] == "elapsed") elapsed += a[2]
      }
    }
    END {
      if (tasks > 0) {
        printf("MEM_TASK_REPORTS=%d\n", tasks)
        printf("MEM_TOTAL_BYTES=%d\n", bytes)
        printf("MEM_TOTAL_GiB=%.2f\n", bytes/1024/1024/1024)
        printf("MEM_CHECKSUM_SUM=%.0f\n", checksum)
        printf("MEM_AVG_ELAPSED_SECONDS=%.2f\n", elapsed/tasks)
      } else {
        print "MEM_TASK_REPORTS=0"
        exit 1
      }
    }'
}

run_io() {
  echo "[INFO] I/O validation"
  echo "[INFO] partition=$PARTITION nodes=$NODES ntasks_per_node=$NTASKS_PER_NODE total_tasks=$TOTAL_TASKS cpus_per_task=$CPUS_PER_TASK io_mb=$IO_MB io_block_kb=$IO_BLOCK_KB workdir=$WORKDIR"
  local output
  output=$(TEST_IO_MB="$IO_MB" TEST_IO_BLOCK_KB="$IO_BLOCK_KB" TEST_WORKDIR="$WORKDIR" run_python_srun "$(io_code)")
  printf '%s\n' "$output"
  printf '%s\n' "$output" | awk '
    BEGIN { tasks=0; bytes=0; wsec=0; rsec=0 }
    /^IOTASK / {
      tasks++
      for (i=1; i<=NF; i++) {
        split($i, a, "=")
        if (a[1] == "bytes") bytes += a[2]
        if (a[1] == "write_sec") wsec += a[2]
        if (a[1] == "read_sec") rsec += a[2]
      }
    }
    END {
      if (tasks > 0) {
        printf("IO_TASK_REPORTS=%d\n", tasks)
        printf("IO_TOTAL_BYTES=%d\n", bytes)
        printf("IO_TOTAL_GiB=%.2f\n", bytes/1024/1024/1024)
        printf("IO_AVG_WRITE_SECONDS=%.4f\n", wsec/tasks)
        printf("IO_AVG_READ_SECONDS=%.4f\n", rsec/tasks)
        if (wsec > 0) printf("IO_AGG_WRITE_MBPS=%.2f\n", (bytes/1024/1024)/wsec)
        if (rsec > 0) printf("IO_AGG_READ_MBPS=%.2f\n", (bytes/1024/1024)/rsec)
      } else {
        print "IO_TASK_REPORTS=0"
        exit 1
      }
    }'
}

run_pi_summary() {
  local output
  output=$(TEST_PI_STEPS="$PI_STEPS" run_python_srun "$(pi_code)")
  printf '%s\n' "$output"
  printf '%s\n' "$output" | awk '
    BEGIN { pi=0.0; tasks=0; true_pi=3.14159265358979323846 }
    /^PITASK / {
      tasks++
      for (i=1; i<=NF; i++) {
        split($i, a, "=")
        if (a[1] == "partial") pi += a[2]
      }
    }
    END {
      err = pi - true_pi
      if (err < 0) err = -err
      printf("FINAL_PI_ESTIMATE=%.12f\n", pi)
      printf("FINAL_PI_ABS_ERROR=%.12e\n", err)
      printf("FINAL_PI_TASK_REPORTS=%d\n", tasks)
    }'
}

run_burnin() {
  echo "[INFO] Long-duration burn-in (CPU+MEM+IO)"
  echo "[INFO] partition=$PARTITION nodes=$NODES ntasks_per_node=$NTASKS_PER_NODE total_tasks=$TOTAL_TASKS cpus_per_task=$CPUS_PER_TASK seconds=$SECONDS mem_mb=$MEM_MB io_mb=$IO_MB workdir=$WORKDIR"
  local burn_output
  burn_output=$(TEST_SECONDS="$SECONDS" TEST_MEM_MB="$MEM_MB" TEST_IO_MB="$IO_MB" TEST_WORKDIR="$WORKDIR" run_python_srun "$(burn_code)")
  printf '%s\n' "$burn_output"
  printf '%s\n' "$burn_output" | awk '
    BEGIN { tasks=0; iters=0; cycles=0; elapsed=0; rolling=0 }
    /^BURNTASK / {
      tasks++
      for (i=1; i<=NF; i++) {
        split($i, a, "=")
        if (a[1] == "iters") iters += a[2]
        if (a[1] == "io_cycles") cycles += a[2]
        if (a[1] == "elapsed") elapsed += a[2]
        if (a[1] == "rolling") rolling += a[2]
      }
    }
    END {
      if (tasks > 0) {
        printf("BURN_TASK_REPORTS=%d\n", tasks)
        printf("BURN_TOTAL_ITERS=%d\n", iters)
        printf("BURN_TOTAL_IO_CYCLES=%d\n", cycles)
        printf("BURN_AVG_ELAPSED_SECONDS=%.2f\n", elapsed/tasks)
        printf("BURN_ROLLING_SUM=%.0f\n", rolling)
      } else {
        print "BURN_TASK_REPORTS=0"
        exit 1
      }
    }'

  echo "[INFO] Final deterministic PI check after burn-in"
  run_pi_summary
}

case "$COMMAND" in
  mem) run_mem ;;
  io) run_io ;;
  burnin) run_burnin ;;
  *) echo "[ERROR] Unknown command: $COMMAND" >&2; usage; exit 1 ;;
esac
