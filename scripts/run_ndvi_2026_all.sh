#!/usr/bin/env bash
# scripts/run_ndvi_2026_all.sh
#
# Постановка archive_ndvi + baseline за 2026 год для ВСЕХ субъектов РФ.
# Использует 4 параллельных воркера. Команда run_baseline_ndvi
# создаёт по одной PipelineRun(task=archive_ndvi) на регион;
# после успешного завершения каждой задачи воркер автоматически
# вызывает calc_ndvi_baseline.
#
# ВАЖНО: после фикса OOM на хосте pve (zfs ARC limit + swap) БД
# стабильна, но 4 параллельных пайплайна потребляют до ~6 ГБ RAM
# на web/worker и устойчивые INSERT-нагрузки на БД. Если RAM
# меньше 16 ГБ — снижай CONCURRENCY до 2.
#
# По умолчанию baseline (`calc_ndvi_baseline`) ПРОПУСКАЕТСЯ для 2026 —
# baseline считается по историческим годам, для текущего года он не нужен.
# Если потребуется — запусти со SKIP_BASELINE=0.
#
# Usage:
#   bash scripts/run_ndvi_2026_all.sh                # обычный запуск (без baseline)
#   FORCE=1 bash scripts/run_ndvi_2026_all.sh        # пересчитать всё
#   DRY_RUN=1 bash scripts/run_ndvi_2026_all.sh      # только показать план
#   CONCURRENCY=2 bash scripts/run_ndvi_2026_all.sh  # снизить нагрузку
#   SKIP_BASELINE=0 bash scripts/run_ndvi_2026_all.sh  # включить расчёт baseline

set -euo pipefail

COMPOSE_FILE="${COMPOSE_FILE:-/opt/edunabazar/docker-compose.prod.yml}"
YEAR="${YEAR:-2026}"
CONCURRENCY="${CONCURRENCY:-4}"
FORCE_FLAG=""
DRY_RUN_FLAG=""
SKIP_BASELINE_FLAG="--skip-baseline"
[[ "${FORCE:-0}" == "1" ]] && FORCE_FLAG="--force"
[[ "${DRY_RUN:-0}" == "1" ]] && DRY_RUN_FLAG="--dry-run"
[[ "${SKIP_BASELINE:-1}" == "0" ]] && SKIP_BASELINE_FLAG=""

DC="docker compose -f ${COMPOSE_FILE}"

echo "════════════════════════════════════════════════════════"
echo "  NDVI archive ${YEAR} — все субъекты РФ"
echo "  compose : ${COMPOSE_FILE}"
echo "  workers : ${CONCURRENCY}"
echo "  flags   : ${FORCE_FLAG} ${DRY_RUN_FLAG} ${SKIP_BASELINE_FLAG}"
echo "════════════════════════════════════════════════════════"

# 1) Масштабируем воркеры до нужного количества
echo "[1/5] Scaling worker → ${CONCURRENCY}…"
${DC} up -d --scale worker="${CONCURRENCY}" --no-recreate worker

# 2) Проверяем что web и worker живы
echo "[2/5] Containers status:"
${DC} ps web worker

# 3) Sanity check: baseline для года уже стоит в очереди?
echo "[3/5] Existing pipeline runs for year=${YEAR}:"
${DC} exec -T web python manage.py shell -c "
from agrocosmos.models import PipelineRun
qs = PipelineRun.objects.filter(year=${YEAR}, task_type='archive_ndvi')
from collections import Counter
c = Counter(qs.values_list('status', flat=True))
print('  totals:', dict(c))
print('  running/queued:', list(qs.filter(status__in=['queued','running']).values_list('id','region__name','status')[:20]))
"

# 4) Показываем dry-run plan (всегда — для прозрачности)
echo "[4/5] Plan (dry-run preview):"
${DC} exec -T web python manage.py run_baseline_ndvi \
    --regions all \
    --year-from "${YEAR}" --year-to "${YEAR}" \
    --concurrency "${CONCURRENCY}" \
    ${FORCE_FLAG} ${SKIP_BASELINE_FLAG} \
    --dry-run --no-monitor || true

# 5) Постановка задач
if [[ -n "${DRY_RUN_FLAG}" ]]; then
    echo "[5/5] DRY_RUN=1 — задачи НЕ ставились в очередь."
    exit 0
fi

echo "[5/5] Enqueueing tasks…"
${DC} exec -T web python manage.py run_baseline_ndvi \
    --regions all \
    --year-from "${YEAR}" --year-to "${YEAR}" \
    --concurrency "${CONCURRENCY}" \
    ${FORCE_FLAG} ${SKIP_BASELINE_FLAG} \
    --no-monitor

echo
echo "Готово. Воркеры подберут задачи автоматически."
echo
echo "Мониторинг прогресса:"
echo "  ${DC} exec -T web python manage.py shell -c \\"
echo "    \"from agrocosmos.models import PipelineRun;"
echo "     from collections import Counter;"
echo "     qs = PipelineRun.objects.filter(year=${YEAR}, task_type='archive_ndvi');"
echo "     print(dict(Counter(qs.values_list('status', flat=True))))\""
echo
echo "  ${DC} logs -f --tail=20 worker"
