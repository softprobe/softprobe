#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")"

docker buildx build --platform linux/amd64 --push -t gcr.io/cs-poc-sasxbttlzroculpau4u6e2l/softprobe-otlp-backend:latest .