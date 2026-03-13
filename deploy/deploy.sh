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

# Build and restart
echo "--- Building containers ---"
docker compose -f "$COMPOSE_FILE" build --no-cache web

echo "--- Starting services ---"
docker compose -f "$COMPOSE_FILE" up -d

echo "--- Running migrations ---"
docker compose -f "$COMPOSE_FILE" exec web python manage.py migrate --noinput

echo "--- Collecting static files ---"
docker compose -f "$COMPOSE_FILE" exec web python manage.py collectstatic --noinput

echo "--- Cleaning up old images ---"
docker image prune -f

echo "=== Deploy finished at $(date) ==="
echo "--- Service status ---"
docker compose -f "$COMPOSE_FILE" ps
