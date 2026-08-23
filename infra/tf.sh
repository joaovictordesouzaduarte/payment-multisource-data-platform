#!/usr/bin/env bash
# Interactive Terraform workspace via Docker (no local Terraform install).
#
#   ./infra/tf.sh up       # build + start workspace
#   ./infra/tf.sh shell    # open a shell in the container
#   ./infra/tf.sh down     # stop workspace
#
# Inside the shell (safe plan-review workflow):
#   terraform init
#   terraform plan -out=tfplan
#   terraform show tfplan
#   terraform apply tfplan
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${ROOT}/infra/docker-compose.terraform.yml"
SERVICE="terraform"

usage() {
  echo "Usage: ./infra/tf.sh <up|shell|down|status>"
  echo
  echo "Commands:"
  echo "  up       Build image and start the interactive Terraform workspace"
  echo "  shell    Exec into the running workspace (/workspace)"
  echo "  down     Stop and remove the workspace container"
  echo "  status   Show compose service status"
  echo
  echo "Safe apply (inside shell):"
  echo "  terraform init"
  echo "  terraform plan -out=tfplan"
  echo "  terraform show tfplan"
  echo "  terraform apply tfplan"
}

ensure_tfvars() {
  local tfvars="${ROOT}/infra/terraform/terraform.tfvars"
  local example="${ROOT}/infra/terraform/terraform.tfvars.example"
  if [[ ! -f "${tfvars}" && -f "${example}" ]]; then
    cp "${example}" "${tfvars}"
    echo "Created infra/terraform/terraform.tfvars from example (edit as needed)."
  fi
}

compose() {
  docker compose -f "${COMPOSE_FILE}" "$@"
}

cmd="${1:-}"
if [[ -z "${cmd}" ]]; then
  usage
  exit 1
fi
shift || true

cd "${ROOT}"

case "${cmd}" in
  up)
    ensure_tfvars
    compose up -d --build
    echo
    echo "Workspace is up. Next:"
    echo "  ./infra/tf.sh shell"
    ;;
  shell)
    if ! compose ps --status running -q "${SERVICE}" 2>/dev/null | grep -q .; then
      echo "Workspace is not running. Start it with: ./infra/tf.sh up" >&2
      exit 1
    fi
    # exec cannot call a shell function named compose — invoke docker directly
    exec docker compose -f "${COMPOSE_FILE}" exec -it "${SERVICE}" sh
    ;;
  down)
    compose down
    ;;
  status)
    compose ps
    ;;
  -h|--help|help)
    usage
    ;;
  *)
    echo "Unknown command: ${cmd}" >&2
    usage
    exit 1
    ;;
esac
