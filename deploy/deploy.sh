#!/usr/bin/env bash
# Deploy script for edunabazar (Docker Compose on server)
# Usage: ssh user@server 'bash -s' < deploy/deploy.sh
#   or:  copy project to server, then run: bash deploy/deploy.sh

set -euo pipefail

APP_DIR="${APP_DIR:-/opt/edunabazar}"
BRANCH="${BRANCH:-main}"
REPO="${REPO:-https://github.com/RuKi81/edunabazar.git}"
COMPOSE_FILE="docker-compose.prod.yml"

echo "=== Deploy started at $(date) ==="

# Clone or pull
if [ -d "$APP_DIR/.git" ]; then
    echo "--- Pulling latest code ---"
    cd "$APP_DIR"
    git fetch origin
    git reset --hard "origin/$BRANCH"
else
    echo "--- Cloning repository ---"
    git clone -b "$BRANCH" "$REPO" "$APP_DIR"
    cd "$APP_DIR"
fi

# Ensure .env exists
if [ ! -f "$APP_DIR/.env" ]; then
    echo "ERROR: .env file not found at $APP_DIR/.env"
    echo "Copy .env.example to .env and fill in production values:"
    echo "  cp .env.example .env && nano .env"
    exit 1
fi

# Commit baked into the image (Dockerfile ARG GIT_SHA) so the running
# container can be checked against the deployed commit.
export GIT_SHA="$(git rev-parse HEAD)"
echo "--- Deploying $GIT_SHA ---"

# Build and restart
echo "--- Building containers ---"
docker compose -f "$COMPOSE_FILE" build --no-cache web

# --force-recreate for the code-carrying services: a plain `up -d` has been
# observed leaving web/worker on the previous image (new image built,
# container never swapped), i.e. prod serving stale code after a "successful"
# deploy.
echo "--- Starting services ---"
docker compose -f "$COMPOSE_FILE" up -d --force-recreate web worker
docker compose -f "$COMPOSE_FILE" up -d

echo "--- Running migrations ---"
docker compose -f "$COMPOSE_FILE" exec web python manage.py migrate --noinput

echo "--- Collecting static files ---"
docker compose -f "$COMPOSE_FILE" exec web python manage.py collectstatic --noinput

echo "--- Cleaning up old images ---"
docker image prune -f

# Install/update cron jobs idempotently from deploy/cron.block.
# The installer scrubs legacy duplicate entries and replaces only the
# marker-bounded managed block, so re-running deploy never grows the crontab.
echo "--- Installing cron jobs (idempotent) ---"
bash "$APP_DIR/deploy/install_cron.sh"

echo "=== Deploy finished at $(date) ==="
echo "--- Service status ---"
docker compose -f "$COMPOSE_FILE" ps
