/**
 * Shared NDVI chart helpers for agrocosmos pages.
 *
 * Keeps Chart.js dataset construction consistent across:
 *  - dashboard.html       (multi-year + single farmland charts)
 *  - report_region.html   (region overall + per-district charts)
 *  - report_district.html (region vs district + overall + per-crop charts)
 *
 * Exposes a single global ``window.NdviChart`` with pure functions —
 * no DOM side effects, no dependency on specific HTML structure.
 */
(function () {
  'use strict';

  /** "2026-03-29" → "03-29". Safe for nullish input. */
  function mmdd(dateIso) {
    if (!dateIso || typeof dateIso !== 'string') return null;
    return dateIso.substring(5);
  }

  /**
   * Build the 3 dataset objects that render the grey ±σ band plus the
   * dashed multi-year mean line.
   *
   * @param {Array<{date: string, mean_ndvi: number, std_ndvi: number}>} baseline
   * @param {Array<string>} labels  MM-DD ordered labels for the target chart
   * @returns {Array<object>}  Chart.js datasets (possibly empty)
   */
  function buildBaselineDatasets(baseline, labels) {
    if (!baseline || !baseline.length) return [];
    var blLookup = {};
    baseline.forEach(function (b) { blLookup[mmdd(b.date)] = b; });

    var blVals  = labels.map(function (l) { var b = blLookup[l]; return b ? b.mean_ndvi : null; });
    var blUpper = labels.map(function (l) { var b = blLookup[l]; return b ? Math.min((b.mean_ndvi || 0) + (b.std_ndvi || 0), 1.0) : null; });
    var blLower = labels.map(function (l) { var b = blLookup[l]; return b ? Math.max((b.mean_ndvi || 0) - (b.std_ndvi || 0), 0) : null; });

    var bandStyle = {
      borderColor: 'transparent',
      backgroundColor: 'rgba(158,158,158,0.18)',
      borderWidth: 0,
      pointRadius: 0,
      tension: 0.3,
      spanGaps: true,
      order: 12,
    };

    return [
      Object.assign({ label: '±σ (верх)',             data: blUpper, fill: '+1'   }, bandStyle),
      {
        label: 'Среднее многолетнее',
        data: blVals,
        borderColor: '#9E9E9E',
        backgroundColor: 'transparent',
        borderWidth: 2,
        borderDash: [6, 4],
        fill: false,
        pointRadius: 0,
        tension: 0.3,
        spanGaps: true,
        order: 11,
      },
      Object.assign({ label: '±σ (низ)',              data: blLower, fill: '-1'   }, bandStyle),
    ];
  }

  /**
   * Append a dashed "coverage" line from the last observed data point to
   * the end of its 16-day MODIS compositing window (``lastPeriodEnd``).
   *
   * Mutates ``labels`` (inserts ``endMmDd`` if missing and re-sorts) and
   * ``datasets`` (pushes the extension dataset). No-op if the series is
   * empty, has no valid last value, or the period end ≤ last observed date.
   *
   * @param {object} opts
   * @param {Array<object>} opts.datasets   target Chart.js datasets array
   * @param {Array<string>} opts.labels     MM-DD labels (mutated)
   * @param {Array<object>} opts.series     e.g. [{date:'2026-03-22', mean_ndvi:0.45}, ...]
   * @param {string|null}   opts.lastPeriodEnd  ISO date of composite window end
   * @param {string}        opts.color      stroke color (matches the main line)
   * @param {string}        [opts.valueKey='mean_ndvi']  field in series items
   * @param {string}        [opts.label='(период)']      dataset label (hidden from legend)
   */
  function pushExtensionLine(opts) {
    var datasets = opts.datasets;
    var labels = opts.labels;
    var series = opts.series;
    var lastPeriodEnd = opts.lastPeriodEnd;
    var color = opts.color;
    var valueKey = opts.valueKey || 'mean_ndvi';
    var label = opts.label || '(период)';

    if (!lastPeriodEnd || !series || !series.length) return;
    var endMmDd = mmdd(lastPeriodEnd);
    var lastEntry = series[series.length - 1];
    var lastMmDd = mmdd(lastEntry.date);
    var lastValue = lastEntry[valueKey];
    if (lastValue === null || lastValue === undefined || lastValue === 0) return;
    if (!endMmDd || !lastMmDd || endMmDd <= lastMmDd) return;

    if (labels.indexOf(endMmDd) === -1) {
      labels.push(endMmDd);
      labels.sort();
    }

    datasets.push({
      label: label,
      data: labels.map(function (l) { return (l === lastMmDd || l === endMmDd) ? lastValue : null; }),
      borderColor: color,
      backgroundColor: 'transparent',
      borderWidth: 2,
      borderDash: [6, 4],
      fill: false,
      pointRadius: 0,
      tension: 0,
      spanGaps: true,
      order: 10,
    });
  }

  /**
   * Legend label filter for Chart.js: hides the ±σ band datasets and the
   * "(период)" extension line so only meaningful series appear in legends.
   */
  function legendFilter(item) {
    if (!item || !item.text) return true;
    return item.text.indexOf('±σ') < 0 && item.text.indexOf('(период)') < 0;
  }

  window.NdviChart = {
    mmdd: mmdd,
    buildBaselineDatasets: buildBaselineDatasets,
    pushExtensionLine: pushExtensionLine,
    legendFilter: legendFilter,
  };
})();
