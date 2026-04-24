#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
export ANSIBLE_CONFIG="$ROOT_DIR/ansible.cfg"
export SLURM_CONTROL_HOST="${SLURM_CONTROL_HOST:-slurm-ctrl}"
export ANSIBLE_REMOTE_USER="${ANSIBLE_REMOTE_USER:-mitac}"

PLAYBOOKS=(
  "$ROOT_DIR/playbooks/10_preflight.yml"
  "$ROOT_DIR/playbooks/20_build_debs.yml"
  "$ROOT_DIR/playbooks/30_deploy_slurm.yml"
  "$ROOT_DIR/playbooks/40_verify_slurm.yml"
)

usage() {
  cat <<USAGE
Usage:
  ./deploy_slurm.sh [ansible-playbook options]

Examples:
  ./deploy_slurm.sh -K
  SLURM_CONTROL_HOST=slurm-ctrl ANSIBLE_REMOTE_USER=mitac ./deploy_slurm.sh -K
  ./deploy_slurm.sh --limit slurm_controller -K
USAGE
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

for pb in "${PLAYBOOKS[@]}"; do
  echo "=================================================================="
  echo "[RUN] $(basename "$pb")"
  echo "=================================================================="
  ansible-playbook -i "$ROOT_DIR/inventory/hosts_from_etc_hosts.py" "$pb" "$@"
done
