#!/usr/bin/env bash
# Idempotent crontab installer.
#
# Reads the managed crontab block from deploy/cron.block (delimited by
#   "# >>> EDUNABAZAR-CRON BEGIN <<<" / "... END <<<"
# ) and replaces any prior managed block in the user's crontab. Also strips
# legacy entries from before this installer existed (when CI appended cron
# lines without idempotency, leaving hundreds of duplicated comments).
#
# Lines outside our markers — e.g. the certbot renewal job, default Ubuntu
# comments — are preserved verbatim.
#
# Safe to run repeatedly. Exits 0 on success.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BLOCK_FILE="${SCRIPT_DIR}/cron.block"

if [ ! -f "$BLOCK_FILE" ]; then
    echo "ERROR: cron block file not found: $BLOCK_FILE" >&2
    exit 1
fi

BEGIN_MARKER='# >>> EDUNABAZAR-CRON BEGIN <<<'
END_MARKER='# >>> EDUNABAZAR-CRON END <<<'

# Names of managed commands — used to scrub legacy duplicates from before
# the marker era. Each pattern matches both the cron line and any leftover
# header comment we used to emit.
LEGACY_CMD_REGEX='fetch_news|check_monitoring|check_raster_monitoring|cleanup_stale_runs|cleanup_rasters|detect_vegetation_alerts|detect_district_ndvi_alerts|send_agrocosmos_updates|ensure_all_regions_monitored|calc_ndvi_baseline|recompute_district_ndvi_status|prewarm_agro_caches'

# Comment patterns we historically emitted (anchored at start-of-line).
LEGACY_COMMENT_REGEX='^# (Daily news fetch|Daily MODIS monitoring|Daily S2\+L8 monitoring|Nightly vegetation alert|Nightly district-level MODIS alert|Daily NDVI update digest|Cleanup stale PipelineRuns|Weekly raster retention|Weekly MODIS raster retention|Annual rollover|Annual baseline rebuild)'

EXISTING="$(crontab -l 2>/dev/null || true)"

# Step 1: drop any existing managed block (between markers, inclusive).
WITHOUT_BLOCK="$(printf '%s\n' "$EXISTING" | sed "/^${BEGIN_MARKER}\$/,/^${END_MARKER}\$/d")"

# Step 2: scrub legacy lines that predate the markers.
# `grep -v` exits 1 when every input line matches the pattern (e.g. crontab
# consisting solely of legacy duplicates). Under `set -e` that would abort
# the install; `|| true` keeps the pipeline going with empty stdout.
CLEANED="$(printf '%s\n' "$WITHOUT_BLOCK" \
    | { grep -vE "$LEGACY_CMD_REGEX" || true; } \
    | { grep -vE "$LEGACY_COMMENT_REGEX" || true; } \
    | { grep -vE '^PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin$' || true; } \
    | awk 'BEGIN{blank=0} /^[[:space:]]*$/{blank++; if(blank<=1) print; next} {blank=0; print}')"

# Step 3: append fresh managed block.
NEW_CRONTAB="$(printf '%s\n%s\n' "$CLEANED" "$(cat "$BLOCK_FILE")")"

# Step 4: install. `crontab -` reads from stdin and replaces atomically.
printf '%s' "$NEW_CRONTAB" | crontab -

echo "Cron installed (managed block from $BLOCK_FILE)."
