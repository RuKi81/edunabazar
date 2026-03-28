#!/bin/bash
# PostgreSQL daily backup script
# Add to crontab: 0 3 * * * /opt/edunabazar/deploy/db/backup.sh >> /var/log/pg_backup.log 2>&1

set -euo pipefail

# --- Config ---
BACKUP_DIR="${BACKUP_DIR:-/mnt/nas/pg_backups}"
KEEP_DAYS="${KEEP_DAYS:-14}"
DB_CONTAINER="${DB_CONTAINER:-db-db-1}"
DB_NAME="${DB_NAME:-enb_DB}"
DB_USER="${DB_USER:-enb_app}"
DATE=$(date +%Y-%m-%d_%H%M)
BACKUP_FILE="${BACKUP_DIR}/${DB_NAME}_${DATE}.sql.gz"

# --- Run ---
mkdir -p "$BACKUP_DIR"

echo "[$(date)] Starting backup of ${DB_NAME}..."

docker exec "$DB_CONTAINER" pg_dump -U "$DB_USER" -d "$DB_NAME" --no-owner --no-acl \
  | gzip > "$BACKUP_FILE"

SIZE=$(du -h "$BACKUP_FILE" | cut -f1)
echo "[$(date)] Backup complete: ${BACKUP_FILE} (${SIZE})"

# --- Cleanup old backups ---
find "$BACKUP_DIR" -name "${DB_NAME}_*.sql.gz" -mtime +"$KEEP_DAYS" -delete
echo "[$(date)] Old backups (>${KEEP_DAYS} days) cleaned up"
