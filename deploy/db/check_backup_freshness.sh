#!/bin/bash
# Alert if the newest PostgreSQL dump is stale (cron did not run / failed).
#
# Context: the DB once lived MONTHS without a single backup because the
# backup cron pointed to a non-existent path and nobody noticed (see
# docs/TODO_infra.md). This is the missing "who watches the watchman" bit.
#
# Install on VM2 (runs hourly; alerts are de-duplicated by GlitchTip):
#   cp /opt/edunabazar-db/check_backup_freshness.sh . && chmod +x ...
#   crontab: 0 * * * * /opt/edunabazar-db/check_backup_freshness.sh >> /var/log/pg_backup_check.log 2>&1
#
# Alerting channel — GlitchTip (Sentry store API via plain curl, no SDK
# needed). Put the DSN of a dedicated "infra" project into
# /opt/edunabazar-db/backup_alert.env:
#   GLITCHTIP_DSN=https://<key>@errors.edunabazar.ru/<project_id>
# Without the env file the script still works and reports via exit code /
# log only.

set -euo pipefail

BACKUP_DIR="${BACKUP_DIR:-/var/backups/postgres}"
DB_NAME="${DB_NAME:-enb_DB}"
MAX_AGE_HOURS="${MAX_AGE_HOURS:-26}"   # daily 03:00 cron + dump duration margin
ENV_FILE="${ENV_FILE:-/opt/edunabazar-db/backup_alert.env}"

[ -f "$ENV_FILE" ] && . "$ENV_FILE"

send_alert() {
    local msg="$1"
    echo "[$(date)] ALERT: $msg"
    if [ -n "${GLITCHTIP_DSN:-}" ]; then
        # DSN = https://KEY@HOST/PROJECT_ID  ->  POST /api/PROJECT_ID/store/
        local key host_and_project host project
        key=$(echo "$GLITCHTIP_DSN" | sed -E 's|https?://([^@]+)@.*|\1|')
        host_and_project=$(echo "$GLITCHTIP_DSN" | sed -E 's|https?://[^@]+@||')
        host=${host_and_project%/*}
        project=${host_and_project##*/}
        curl -fsS -m 15 -X POST "https://${host}/api/${project}/store/" \
            -H "Content-Type: application/json" \
            -H "X-Sentry-Auth: Sentry sentry_version=7, sentry_key=${key}" \
            -d "{\"message\": \"$msg\", \"level\": \"error\", \"logger\": \"backup-freshness\", \"platform\": \"other\", \"server_name\": \"vm2-db\"}" \
            || echo "[$(date)] WARNING: failed to deliver alert to GlitchTip"
    fi
}

newest=$(find "$BACKUP_DIR" -name "${DB_NAME}_*.sql.gz" -printf '%T@ %p\n' 2>/dev/null \
         | sort -rn | head -1 | cut -d' ' -f2-)

if [ -z "$newest" ]; then
    send_alert "PG backup check: no ${DB_NAME}_*.sql.gz dumps found in ${BACKUP_DIR} at all"
    exit 1
fi

age_sec=$(( $(date +%s) - $(stat -c %Y "$newest") ))
age_hours=$(( age_sec / 3600 ))

if [ "$age_hours" -ge "$MAX_AGE_HOURS" ]; then
    send_alert "PG backup check: newest dump is ${age_hours}h old (limit ${MAX_AGE_HOURS}h): ${newest}"
    exit 1
fi

echo "[$(date)] OK: newest dump ${newest} is ${age_hours}h old"
