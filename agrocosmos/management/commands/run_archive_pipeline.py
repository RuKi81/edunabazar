from __future__ import annotations

"""Orchestrator: runs the archive MODIS NDVI pipeline + baseline recalc.

Mirrors :mod:`run_ndvi_pipeline` (which covers S2/L8/fusion/postprocess
for the current year) but for the long-running historical MODIS run
used to populate NdviBaseline.

Stages:
    1. ``modis_ndvi --region-id R --date-from Y1-01-01 --date-to Y2-12-31``
       — downloads 2-weekly composites across multiple years and writes
       raw per-farmland VegetationIndex rows.
    2. ``calc_ndvi_baseline --region-id R`` (optional, enabled by default)
       — aggregates the raw series into agro_ndvi_baseline so the
       dashboard's grey dashed baseline line lights up.

Designed to be picked up by the ``run_ndvi_worker`` container so it
survives web deploys. A ``PipelineRun`` row identified by ``--run-id``
is updated on every stage transition and every ~30 s (heartbeat_at).
On any failure the run is marked ``status=failed`` with the traceback
captured in ``log``.

Usage:
    python manage.py run_archive_pipeline --run-id 123 --region-id 45 \
        --year-from 2020 --year-to 2025 [--min-valid 0.5] [--skip-baseline]
"""

import os
import signal
import sys
import threading
import time
import traceback
from datetime import date, datetime
from io import StringIO

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone

from agrocosmos.models import PipelineRun, Region


HEARTBEAT_INTERVAL_SEC = 30
LOG_MAX_DB_CHARS = 16000


class _TeeStream:
    """Forward writes to a live sink (stdout/log file) AND a StringIO."""

    def __init__(self, capture: StringIO, live_sink=None):
        self._capture = capture
        self._live = live_sink if live_sink is not None else sys.stdout

    def write(self, s):
        try:
            self._live.write(s)
            self._live.flush()
        except Exception:
            pass
        self._capture.write(s)
        return len(s)

    def flush(self):
        try:
            self._live.flush()
        except Exception:
            pass

    def isatty(self):
        return False

    @property
    def encoding(self):
        return getattr(self._live, 'encoding', 'utf-8')


class _Stopwatch:
    def __init__(self):
        self.t0 = time.time()

    def elapsed(self) -> str:
        s = int(time.time() - self.t0)
        h, rem = divmod(s, 3600)
        m, s = divmod(rem, 60)
        return f'{h}h{m:02d}m{s:02d}s'


class Command(BaseCommand):
    help = (
        'Run the archive MODIS NDVI pipeline for a region across a year '
        'range, then recalculate NdviBaseline. Progress tracked via a '
        'PipelineRun row so the admin panel shows live status.'
    )

    # ------------------------------------------------------------------ CLI

    def add_arguments(self, parser):
        parser.add_argument('--run-id', type=int, required=True,
                            help='PipelineRun id to update')
        parser.add_argument('--region-id', type=int, required=True,
                            help='Region ID')
        parser.add_argument('--year-from', type=int, default=2020,
                            help='First year of archive window (default 2020)')
        parser.add_argument('--year-to', type=int, default=None,
                            help='Last year of archive window '
                                 '(default: current_year - 1)')
        parser.add_argument('--min-valid', type=float, default=0.5,
                            help='Min valid pixel ratio (default 0.5)')
        parser.add_argument('--overwrite', action='store_true',
                            help='Re-download existing MODIS rasters')
        parser.add_argument('--skip-baseline', action='store_true',
                            help='Skip calc_ndvi_baseline after the download')

    # ------------------------------------------------------------------ state

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._run_id: int | None = None
        self._log_lines: list[str] = []
        self._log_file_path: str | None = None
        self._hb_stop = threading.Event()
        self._hb_thread: threading.Thread | None = None
        self._total_records = 0

    # ------------------------------------------------------------------ helpers

    def _log(self, line: str) -> None:
        ts = datetime.utcnow().strftime('%H:%M:%S')
        stamped = f'[{ts}] {line}'
        print(stamped, flush=True)
        self._log_lines.append(stamped)

    def _read_log_file_tail(self) -> str | None:
        path = self._log_file_path
        if not path:
            return None
        try:
            with open(path, 'rb') as f:
                try:
                    f.seek(-LOG_MAX_DB_CHARS, os.SEEK_END)
                    prefix = b'...(truncated)...\n'
                except OSError:
                    f.seek(0)
                    prefix = b''
                data = prefix + f.read()
            return data.decode('utf-8', errors='replace')
        except OSError:
            return None

    def _flush_log_to_db(self, *, final: bool = False) -> None:
        if not self._run_id:
            return
        tail = self._read_log_file_tail()
        if tail is None:
            tail = '\n'.join(self._log_lines)
            if len(tail) > LOG_MAX_DB_CHARS:
                tail = '...(truncated)...\n' + tail[-LOG_MAX_DB_CHARS:]
        try:
            fields = {'log': tail, 'heartbeat_at': timezone.now()}
            if final:
                fields['finished_at'] = timezone.now()
            PipelineRun.objects.filter(pk=self._run_id).update(**fields)
        except Exception as exc:
            print(f'[flush_log] warning: {exc}', flush=True)

    def _heartbeat_loop(self) -> None:
        while not self._hb_stop.wait(HEARTBEAT_INTERVAL_SEC):
            self._flush_log_to_db()

    def _start_heartbeat(self) -> None:
        self._hb_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True,
        )
        self._hb_thread.start()

    def _stop_heartbeat(self) -> None:
        self._hb_stop.set()
        if self._hb_thread:
            self._hb_thread.join(timeout=5)

    def _mark(self, status: str, records: int | None = None) -> None:
        if not self._run_id:
            return
        fields = {
            'status': status,
            'heartbeat_at': timezone.now(),
        }
        if status in ('completed', 'failed'):
            fields['finished_at'] = timezone.now()
        if records is not None:
            fields['records_count'] = records
        try:
            PipelineRun.objects.filter(pk=self._run_id).update(**fields)
        except Exception as exc:
            print(f'[mark] warning: {exc}', flush=True)

    # ------------------------------------------------------------------ stages

    def _run_subcommand(self, name: str, **kwargs) -> str:
        capture = StringIO()
        tee = _TeeStream(capture)
        kwargs['stdout'] = tee
        kwargs['stderr'] = tee
        call_command(name, **kwargs)
        text = capture.getvalue()
        for line in text.splitlines():
            self._log_lines.append(line)
        return text

    def _stage_modis(self, *, region_id: int, year_from: int, year_to: int,
                     min_valid: float, overwrite: bool) -> int:
        self._log(
            f'─── stage: modis_ndvi region={region_id} '
            f'{year_from}..{year_to} min_valid={min_valid:.0%} ───'
        )
        kwargs = {
            'region_id': region_id,
            'date_from': f'{year_from}-01-01',
            'date_to': f'{year_to}-12-31',
            'min_valid_ratio': min_valid,
            'overwrite': overwrite,
        }
        text = self._run_subcommand('modis_ndvi', **kwargs)

        # modis_ndvi prints exactly one summary line of the form
        # ``  Records saved: 3554358`` at the very end. We pick that up
        # rather than the per-composite ``→ N records saved`` lines so
        # the total isn't double-counted.
        records = 0
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith('Records saved:'):
                try:
                    records = int(stripped.split(':', 1)[1].strip())
                except (ValueError, IndexError):
                    pass
        self._log(f'─── MODIS done: ~{records} records ───')
        self._flush_log_to_db()
        return records

    def _stage_baseline(self, *, region_id: int) -> None:
        self._log(f'─── stage: calc_ndvi_baseline region={region_id} ───')
        self._run_subcommand('calc_ndvi_baseline', region_id=region_id)
        self._log('─── baseline done ───')
        self._flush_log_to_db()

    # ------------------------------------------------------------------ main

    def handle(self, *args, **options):
        self._run_id = options['run_id']
        try:
            self._log_file_path = (
                PipelineRun.objects.filter(pk=self._run_id)
                .values_list('log_file', flat=True).first() or None
            )
        except Exception:
            self._log_file_path = None

        region_id = options['region_id']
        year_from = options['year_from']
        year_to = options['year_to'] or (date.today().year - 1)
        min_valid = options['min_valid']
        overwrite = options['overwrite']
        skip_baseline = options['skip_baseline']

        if year_to < year_from:
            self.stderr.write('--year-to must be >= --year-from')
            self._mark('failed')
            return

        # ── SIGTERM / SIGINT: mark failed so we don't leak running ──
        def _on_term(signum, _frame):
            self._log(f'[signal] received signal {signum}, marking failed')
            self._flush_log_to_db(final=True)
            self._mark('failed')
            sys.exit(1)

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _on_term)
            except (ValueError, OSError):
                pass

        # ── Resolve scope ──
        try:
            reg = Region.objects.get(pk=region_id)
        except Region.DoesNotExist as exc:
            self._log(f'scope error: {exc}')
            self._flush_log_to_db(final=True)
            self._mark('failed')
            return

        # ── Bootstrap: save PID, mark running ──
        try:
            PipelineRun.objects.filter(pk=self._run_id).update(
                pid=os.getpid(),
                status='running',
                heartbeat_at=timezone.now(),
            )
        except Exception as exc:
            print(f'[bootstrap] warning: {exc}', flush=True)

        sw = _Stopwatch()
        self._log('═══════════════════════════════════════════════')
        self._log(f'  Archive pipeline run_id={self._run_id}  pid={os.getpid()}')
        self._log(f'  Region: {reg.name} (id={reg.pk})')
        self._log(f'  Window: {year_from}..{year_to}   Min valid: {min_valid:.0%}')
        flags = []
        if overwrite: flags.append('overwrite')
        if skip_baseline: flags.append('skip-baseline')
        self._log(f'  Flags: [{", ".join(flags) or "none"}]')
        self._log('═══════════════════════════════════════════════')

        self._start_heartbeat()
        try:
            self._total_records = self._stage_modis(
                region_id=region_id, year_from=year_from, year_to=year_to,
                min_valid=min_valid, overwrite=overwrite,
            )

            if not skip_baseline:
                self._stage_baseline(region_id=region_id)

            self._log(
                f'═══ Archive pipeline completed in {sw.elapsed()} — '
                f'{self._total_records} records, baseline {"skipped" if skip_baseline else "recalculated"} ═══'
            )
            self._flush_log_to_db(final=True)
            self._mark('completed', records=self._total_records)

        except Exception:
            tb = traceback.format_exc()
            self._log('!!! Archive pipeline failed !!!')
            for line in tb.splitlines():
                self._log_lines.append(line)
            self._flush_log_to_db(final=True)
            self._mark('failed')
            raise
        finally:
            self._stop_heartbeat()
