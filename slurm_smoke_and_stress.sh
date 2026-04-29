#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
用法:
  ./slurm_smoke_and_stress.sh smoke [選項]
  ./slurm_smoke_and_stress.sh stress [選項]

子命令:
  smoke   執行 Slurm 安裝後的基本驗證
  stress  執行 CPU 壓力測試

共同選項:
  -p, --partition NAME     Slurm partition，預設: debug
  -w, --workdir DIR        工作目錄，預設: /tmp
  -n, --nodes N            節點數，預設: 自動抓目前 idle 節點數
  -l, --nodelist LIST      指定節點清單，例如: slurm-c01,slurm-c02
  -h, --help               顯示說明

stress 專用選項:
  -t, --seconds SEC        壓測秒數，預設: 60
  -c, --cpus-per-node N    每個節點要壓滿幾顆 CPU，預設: 自動用 nproc
  --label TEXT             額外標籤，方便辨識 log，預設: cpu-stress

範例:
  ./slurm_smoke_and_stress.sh smoke
  ./slurm_smoke_and_stress.sh smoke -n 3
  ./slurm_smoke_and_stress.sh stress -t 120
  ./slurm_smoke_and_stress.sh stress -t 300 -n 2
  ./slurm_smoke_and_stress.sh stress -t 180 -l slurm-c01,slurm-c02
USAGE
}

log() { printf '[%s] %s\n' "$(date '+%F %T')" "$*"; }
die() { echo "ERROR: $*" >&2; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "找不到指令: $1"
}

slurm_time_from_seconds() {
  local total="$1"
  local h m s
  h=$(( total / 3600 ))
  m=$(( (total % 3600) / 60 ))
  s=$(( total % 60 ))
  printf '%02d:%02d:%02d' "$h" "$m" "$s"
}

get_idle_nodes_default() {
  sinfo -h -N -t idle -o '%N' | wc -l | awk '{print $1}'
}

get_idle_nodelist_default() {
  sinfo -h -N -t idle -o '%N' | paste -sd, -
}

run_smoke() {
  local partition="$1"
  local workdir="$2"
  local nodes="$3"
  local nodelist="$4"

  require_cmd scontrol
  require_cmd sinfo
  require_cmd srun
  require_cmd sbatch
  require_cmd squeue

  log 'Step 1/6: 檢查 slurmctld 狀態'
  scontrol ping

  log 'Step 2/6: 檢查節點與 partition 概況'
  sinfo -N -l

  log 'Step 3/6: 單節點 srun 測試'
  if [[ -n "$nodelist" ]]; then
    local first_node
    first_node="${nodelist%%,*}"
    srun --partition="$partition" --chdir="$workdir" -N1 -w "$first_node" hostname
  else
    srun --partition="$partition" --chdir="$workdir" -N1 hostname
  fi

  log 'Step 4/6: 多節點 srun 測試'
  if [[ -n "$nodelist" ]]; then
    local cnt
    cnt="$(awk -F',' '{print NF}' <<< "$nodelist")"
    srun --partition="$partition" --chdir="$workdir" -N "$cnt" -w "$nodelist" hostname | sort
  else
    srun --partition="$partition" --chdir="$workdir" -N "$nodes" hostname | sort
  fi

  log 'Step 5/6: sbatch 測試'
  local submit_out jobid
  submit_out="$({ sbatch --parsable --partition="$partition" --chdir="$workdir" --wrap='echo HOST=$(hostname); echo PWD=$(pwd); sleep 2'; } 2>&1)"
  echo "$submit_out"
  jobid="${submit_out%%;*}"
  [[ "$jobid" =~ ^[0-9]+$ ]] || die "無法解析 job id: $submit_out"

  log "Step 6/6: 等待 job $jobid 完成"
  while squeue -h -j "$jobid" | grep -q .; do
    sleep 1
  done
  sacct -j "$jobid" --format=JobID,JobName,Partition,State,ExitCode -n || true

  log 'Smoke test 完成'
}

run_stress() {
  local partition="$1"
  local workdir="$2"
  local nodes="$3"
  local nodelist="$4"
  local seconds="$5"
  local cpus_per_node="$6"
  local label="$7"

  require_cmd srun
  require_cmd sinfo

  [[ "$seconds" =~ ^[0-9]+$ ]] || die "--seconds 必須是整數秒"
  [[ "$seconds" -ge 1 ]] || die "--seconds 必須 >= 1"

  local slurm_time ntasks
  slurm_time="$(slurm_time_from_seconds "$seconds")"

  if [[ -n "$nodelist" ]]; then
    ntasks="$(awk -F',' '{print NF}' <<< "$nodelist")"
  else
    ntasks="$nodes"
    nodelist="$(get_idle_nodelist_default)"
  fi

  [[ -n "$nodelist" ]] || die '找不到可用節點，請先確認 sinfo 狀態或手動指定 --nodelist'

  log "開始 CPU stress test"
  log "partition     = $partition"
  log "workdir       = $workdir"
  log "seconds       = $seconds"
  log "slurm_time    = $slurm_time"
  log "nodes         = $ntasks"
  log "nodelist      = $nodelist"
  log "cpus_per_node = ${cpus_per_node:-auto}"
  log "label         = $label"

  local remote_script
  read -r -d '' remote_script <<'REMOTE' || true
set -euo pipefail
DURATION="${DURATION:?}"
CPU_LIMIT="${CPU_LIMIT:-auto}"
LABEL="${LABEL:-cpu-stress}"
HOST="$(hostname)"
WORKERS="$(nproc)"

if [[ "$CPU_LIMIT" != "auto" ]]; then
  WORKERS="$CPU_LIMIT"
fi

if command -v stress-ng >/dev/null 2>&1; then
  echo "[$HOST] mode=stress-ng workers=$WORKERS duration=${DURATION}s label=$LABEL"
  stress-ng --cpu "$WORKERS" --timeout "${DURATION}s" --metrics-brief
else
  echo "[$HOST] mode=bash-busy-loop workers=$WORKERS duration=${DURATION}s label=$LABEL"
  end_epoch=$(( $(date +%s) + DURATION ))
  pids=()
  for _i in $(seq 1 "$WORKERS"); do
    bash -c 'while [ "$(date +%s)" -lt '"$end_epoch"' ]; do :; done' &
    pids+=("$!")
  done
  rc=0
  for pid in "${pids[@]}"; do
    wait "$pid" || rc=$?
  done
  exit "$rc"
fi
REMOTE

  DURATION="$seconds" CPU_LIMIT="${cpus_per_node:-auto}" LABEL="$label" \
  srun \
    --partition="$partition" \
    --chdir="$workdir" \
    --nodes="$ntasks" \
    --ntasks="$ntasks" \
    --nodelist="$nodelist" \
    --exclusive \
    --time="$slurm_time" \
    --export=ALL,DURATION="$seconds",CPU_LIMIT="${cpus_per_node:-auto}",LABEL="$label" \
    bash -lc "$remote_script"

  log 'Stress test 完成'
}

main() {
  require_cmd sinfo
  require_cmd awk
  require_cmd paste

  [[ $# -ge 1 ]] || { usage; exit 1; }
  local subcmd="$1"
  shift

  local partition="debug"
  local workdir="/tmp"
  local nodes=""
  local nodelist=""
  local seconds=60
  local cpus_per_node=""
  local label="cpu-stress"

  while [[ $# -gt 0 ]]; do
    case "$1" in
      -p|--partition)
        partition="$2"; shift 2 ;;
      -w|--workdir)
        workdir="$2"; shift 2 ;;
      -n|--nodes)
        nodes="$2"; shift 2 ;;
      -l|--nodelist)
        nodelist="$2"; shift 2 ;;
      -t|--seconds)
        seconds="$2"; shift 2 ;;
      -c|--cpus-per-node)
        cpus_per_node="$2"; shift 2 ;;
      --label)
        label="$2"; shift 2 ;;
      -h|--help)
        usage; exit 0 ;;
      *)
        die "未知參數: $1" ;;
    esac
  done

  if [[ -z "$nodes" ]]; then
    if [[ -n "$nodelist" ]]; then
      nodes="$(awk -F',' '{print NF}' <<< "$nodelist")"
    else
      nodes="$(get_idle_nodes_default)"
    fi
  fi

  [[ "$nodes" =~ ^[0-9]+$ ]] || die '--nodes 必須是整數'
  [[ "$nodes" -ge 1 ]] || die '找不到可用節點，請先確認 sinfo 或手動指定 --nodes / --nodelist'

  case "$subcmd" in
    smoke)
      run_smoke "$partition" "$workdir" "$nodes" "$nodelist" ;;
    stress)
      run_stress "$partition" "$workdir" "$nodes" "$nodelist" "$seconds" "$cpus_per_node" "$label" ;;
    *)
      die "未知子命令: $subcmd" ;;
  esac
}

main "$@"
