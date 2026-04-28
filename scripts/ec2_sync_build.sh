#!/usr/bin/env bash
#
# Sync this repo to the "app" EC2 instance and build `splake` + `perf_stress`,
# then copy `perf_stress` to the "catalog" EC2 instance.
#
# Intended to run from a machine that has:
#   - this repo checked out
#   - AWS CLI configured
#   - SSH access to both instances
#
# Defaults match the current benchmark instances but can be overridden via env vars.
#
# Usage:
#   bash scripts/ec2_sync_build.sh
#
# Env overrides:
#   AWS_REGION=us-east-1
#   APP_INSTANCE_ID=i-...
#   CATALOG_INSTANCE_ID=i-...
#   SSH_USER=ubuntu
#   EC2_KEY_FILE=~/.ssh/datalake-benchmark-key.pem
#   REMOTE_DIR=~/datalake
#   CARGO_PROFILE=release
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

AWS_REGION="${AWS_REGION:-us-east-1}"
APP_INSTANCE_ID="${APP_INSTANCE_ID:-i-0a60220b597ce9dc1}"
CATALOG_INSTANCE_ID="${CATALOG_INSTANCE_ID:-i-0b8a8b15477dcf1fb}"
SSH_USER="${SSH_USER:-ubuntu}"
EC2_KEY_FILE="${EC2_KEY_FILE:-$HOME/.ssh/datalake-benchmark-key.pem}"
REMOTE_DIR="${REMOTE_DIR:-/home/$SSH_USER/datalake}"
CARGO_PROFILE="${CARGO_PROFILE:-release}"

die() { echo "error: $*" >&2; exit 1; }

need_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required command: $1"
}

need_cmd aws
need_cmd ssh
need_cmd scp
need_cmd rsync

[[ -f "$EC2_KEY_FILE" ]] || die "EC2 key not found: $EC2_KEY_FILE"

get_public_ip() {
  local instance_id="$1"
  aws ec2 describe-instances \
    --region "$AWS_REGION" \
    --instance-ids "$instance_id" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' \
    --output text
}

APP_PUBLIC_IP="$(get_public_ip "$APP_INSTANCE_ID")"
CATALOG_PUBLIC_IP="$(get_public_ip "$CATALOG_INSTANCE_ID")"

[[ -n "$APP_PUBLIC_IP" && "$APP_PUBLIC_IP" != "None" ]] || die "failed to resolve APP public IP for $APP_INSTANCE_ID"
[[ -n "$CATALOG_PUBLIC_IP" && "$CATALOG_PUBLIC_IP" != "None" ]] || die "failed to resolve CATALOG public IP for $CATALOG_INSTANCE_ID"

SSH_OPTS=(
  -i "$EC2_KEY_FILE"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
)

echo "App:     $APP_INSTANCE_ID ($APP_PUBLIC_IP)"
echo "Catalog: $CATALOG_INSTANCE_ID ($CATALOG_PUBLIC_IP)"

echo "==> Sync repo to app instance ($REMOTE_DIR) ..."
rsync -az --delete \
  --exclude '.git' \
  --exclude 'target' \
  --exclude '.DS_Store' \
  -e "ssh ${SSH_OPTS[*]}" \
  "$PROJECT_ROOT/" "$SSH_USER@$APP_PUBLIC_IP:$REMOTE_DIR/"

echo "==> Build on app instance (profile=$CARGO_PROFILE) ..."
ssh "${SSH_OPTS[@]}" "$SSH_USER@$APP_PUBLIC_IP" bash -lc "set -euo pipefail
  cd '$REMOTE_DIR'
  sudo apt-get update -y >/dev/null
  sudo apt-get install -y build-essential pkg-config libssl-dev curl ca-certificates >/dev/null
  if ! command -v cargo >/dev/null 2>&1; then
    echo 'Installing Rust toolchain (rustup) ...'
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source \"\$HOME/.cargo/env\"
  else
    source \"\$HOME/.cargo/env\" 2>/dev/null || true
  fi
  cargo --version
  cargo build --profile '$CARGO_PROFILE' --bin splake --bin perf_stress
"

echo "==> Copy perf_stress to catalog instance ..."
TMP_BIN="/tmp/perf_stress.$(date +%s)"
scp "${SSH_OPTS[@]}" "$SSH_USER@$APP_PUBLIC_IP:$REMOTE_DIR/target/$CARGO_PROFILE/perf_stress" "$TMP_BIN"
scp "${SSH_OPTS[@]}" "$TMP_BIN" "$SSH_USER@$CATALOG_PUBLIC_IP:/home/$SSH_USER/perf_stress"
rm -f "$TMP_BIN"

echo "==> Done"
echo "Next:"
echo "  - On app instance:    bash scripts/ec2_start_splake.sh start"
echo "  - On catalog instance: bash scripts/ec2_run_perf_stress.sh --background --duration 86400"
