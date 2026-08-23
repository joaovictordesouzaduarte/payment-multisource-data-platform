#!/bin/sh
# Configure AWS CLI from env (.env / compose) when the workspace starts.
set -eu

AWS_DIR="${AWS_DIR:-/root/.aws}"
REGION="${AWS_REGION:-${AWS_DEFAULT_REGION:-us-east-1}}"

# Docker creates a file when the host path is missing. Treat that as a credentials file.
if [ -f "$AWS_DIR" ]; then
  export AWS_SHARED_CREDENTIALS_FILE="$AWS_DIR"
  AWS_DIR="/tmp/aws-cli"
fi

mkdir -p "$AWS_DIR"

if [ -n "${AWS_ACCESS_KEY_ID:-}" ] && [ -n "${AWS_SECRET_ACCESS_KEY:-}" ] && [ -w "$AWS_DIR" ]; then
  umask 077
  {
    echo "[default]"
    echo "aws_access_key_id = ${AWS_ACCESS_KEY_ID}"
    echo "aws_secret_access_key = ${AWS_SECRET_ACCESS_KEY}"
    if [ -n "${AWS_SESSION_TOKEN:-}" ]; then
      echo "aws_session_token = ${AWS_SESSION_TOKEN}"
    fi
  } >"${AWS_DIR}/credentials"

  {
    echo "[default]"
    echo "region = ${REGION}"
    echo "output = json"
  } >"${AWS_DIR}/config"

  export AWS_SHARED_CREDENTIALS_FILE="${AWS_DIR}/credentials"
  export AWS_CONFIG_FILE="${AWS_DIR}/config"
fi

export AWS_REGION="$REGION"
export AWS_DEFAULT_REGION="$REGION"

echo "AWS CLI ready (region=${REGION})."

exec "$@"
