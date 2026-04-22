from __future__ import annotations

"""Orchestrator: runs the full NDVI pipeline as a single detached process.

Pipeline stages (configurable):
    1. fetch_raster_ndvi --sensor s2  (download + zonal stats)
    2. fetch_raster_ndvi --sensor l8  (download + zonal stats)
    3. compute_fused_ndvi --overwrite (HLS fusion)
    4. ndvi_postprocess --source fused (anomaly flag + smoothing)

Designed to be launched via ``subprocess.Popen(..., start_new_session=True)``
from the admin panel, so it survives gunicorn worker recycling and any
``docker compose up`` on the web container.

Usage:
    python manage.py run_ndvi_pipeline --district-id 5 --year 2025 \
        --run-id 29 [--overwrite] [--fusion] [--skip-s2] [--skip-l8]

A ``PipelineRun`` row identified by ``--run-id`` is updated on every stage
transition and every ~30 seconds (heartbeat_at) while work is in flight.
On any failure the run is marked ``status=failed`` with the traceback
captured in ``log``.
"""
import os
import signal
import sys
import threading
import time
import traceback
from datetime import datetime
from io import StringIO

from django.core.management import call_command
from django.core.management.base import BaseCommand
from django.utils import timezone

from agrocosmos.models import District, PipelineRun, Region


HEARTBEAT_INTERVAL_SEC = 30
LOG_MAX_DB_CHARS = 16000   # store tail of this size in PipelineRun.log


class _TeeStream:
    """Forward every write to real stdout *and* a StringIO buffer.

    Lets sub-commands stream their output into ``logs/pipeline/run_<id>.log``
    in real time (so ``tail -f`` works), while we retain a copy for parsing
    ``N records saved`` lines and storing a tail in the DB.
    """

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

    # Django's OutputWrapper sometimes probes for isatty / encoding.
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
        'Run the full NDVI pipeline (S2 + L8 + fusion + postprocess) as a '
        'single detached process with progress tracking via PipelineRun.'
    )

    # ------------------------------------------------------------------ CLI

    def add_arguments(self, parser):
        parser.add_argument('--run-id', type=int, required=True,
                            help='PipelineRun id to update')
        parser.add_argument('--region-id', type=int, help='Region ID')
        parser.add_argument('--district-id', type=int, help='District ID')
        parser.add_argument('--year', type=int, required=True, help='Year')
        parser.add_argument('--min-valid', type=float, default=0.7,
                            help='Min valid pixel ratio (default 0.7)')
        parser.add_argument('--overwrite', action='store_true',
                            help='Re-download existing rasters')
        parser.add_argument('--fusion', action='store_true',
                            help='Run compute_fused_ndvi + ndvi_postprocess')
        parser.add_argument('--skip-s2', action='store_true',
                            help='Skip Sentinel-2 stage')
        parser.add_argument('--skip-l8', action='store_true',
                            help='Skip Landsat stage')

    # ------------------------------------------------------------------ state

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._run_id: int | None = None
        self._log_lines: list[str] = []
        self._hb_stop = threading.Event()
        self._hb_thread: threading.Thread | None = None
        self._total_records = 0

    # ------------------------------------------------------------------ helpers

    def _log(self, line: str) -> None:
        """Append a line to stdout and the in-memory accumulator."""
        ts = datetime.utcnow().strftime('%H:%M:%S')
        stamped = f'[{ts}] {line}'
        print(stamped, flush=True)
        self._log_lines.append(stamped)

    def _flush_log_to_db(self, *, final: bool = False) -> None:
        """Persist the tail of the in-memory log to PipelineRun.log + heartbeat."""
        if not self._run_id:
            return
        joined = '\n'.join(self._log_lines)
        if len(joined) > LOG_MAX_DB_CHARS:
            joined = '…(truncated)…\n' + joined[-LOG_MAX_DB_CHARS:]
        try:
            fields = {'log': joined, 'heartbeat_at': timezone.now()}
            if final:
                fields['finished_at'] = timezone.now()
            PipelineRun.objects.filter(pk=self._run_id).update(**fields)
        except Exception as exc:   # never let db error kill the pipeline
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
        """Run a Django mgmt command with live streaming to stdout + capture.

        Returns the captured text so caller can parse progress markers.
        """
        capture = StringIO()
        tee = _TeeStream(capture)
        kwargs['stdout'] = tee
        kwargs['stderr'] = tee
        call_command(name, **kwargs)
        text = capture.getvalue()
        # Keep a copy of sub-command output for the DB tail too.
        for line in text.splitlines():
            self._log_lines.append(line)
        return text

    def _stage_raster(self, sensor: str, *, region_id, district_id, year,
                      min_valid, overwrite) -> int:
        self._log(f'─── stage: fetch_raster_ndvi --sensor {sensor} ───')
        kwargs = {
            'sensor': sensor,
            'year': year,
            'min_valid_ratio': min_valid,
            'overwrite': overwrite,
        }
        if district_id:
            kwargs['district_id'] = district_id
        else:
            kwargs['region_id'] = region_id

        text = self._run_subcommand('fetch_raster_ndvi', **kwargs)

        records = 0
        for line in text.splitlines():
            if 'records saved' in line.lower():
                try:
                    token = line.split('records saved')[0].split()[-1]
                    records += int(''.join(c for c in token if c.isdigit()) or 0)
                except (ValueError, IndexError):
                    pass
        self._log(f'─── {sensor.upper()} done: {records} records ───')
        self._flush_log_to_db()
        return records

    def _stage_fusion(self, *, region_id, district_id, year) -> None:
        self._log('─── stage: compute_fused_ndvi --overwrite ───')
        kwargs = {'year': year, 'overwrite': True}
        if district_id:
            kwargs['district_id'] = district_id
        else:
            kwargs['region_id'] = region_id
        self._run_subcommand('compute_fused_ndvi', **kwargs)
        self._log('─── fusion done ───')
        self._flush_log_to_db()

    def _stage_postprocess(self, *, region_id, year) -> None:
        self._log('─── stage: ndvi_postprocess --source fused ───')
        self._run_subcommand(
            'ndvi_postprocess',
            region_id=region_id, year=year, source='fused',
        )
        self._log('─── postprocess done ───')
        self._flush_log_to_db()

    # ------------------------------------------------------------------ main

    def handle(self, *args, **options):
        self._run_id = options['run_id']
        region_id = options.get('region_id')
        district_id = options.get('district_id')
        year = options['year']
        min_valid = options['min_valid']
        overwrite = options['overwrite']
        do_fusion = options['fusion']
        skip_s2 = options['skip_s2']
        skip_l8 = options['skip_l8']

        # ── Handle SIGTERM / SIGINT: mark failed so we don't leak running ──
        def _on_term(signum, _frame):
            self._log(f'[signal] received signal {signum}, marking failed')
            self._flush_log_to_db(final=True)
            self._mark('failed')
            sys.exit(1)

        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                signal.signal(sig, _on_term)
            except (ValueError, OSError):
                pass   # not main thread or unsupported; that's fine

        # ── Resolve scope (validate) ──
        try:
            if district_id:
                dist = District.objects.select_related('region').get(pk=district_id)
                scope_desc = f'район "{dist.name}" ({dist.region.name})'
                if not region_id:
                    region_id = dist.region_id
            elif region_id:
                reg = Region.objects.get(pk=region_id)
                scope_desc = f'регион "{reg.name}"'
            else:
                self.stderr.write('--region-id or --district-id required')
                self._mark('failed')
                return
        except (District.DoesNotExist, Region.DoesNotExist) as exc:
            self._log(f'scope error: {exc}')
            self._flush_log_to_db(final=True)
            self._mark('failed')
            return

        # ── Update run: save our own PID for the admin to track ──
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
        self._log(f'  Pipeline run_id={self._run_id}  pid={os.getpid()}')
        self._log(f'  Scope: {scope_desc}   Year: {year}')
        flags = []
        if overwrite: flags.append('overwrite')
        if do_fusion: flags.append('+fusion')
        if skip_s2:   flags.append('skip-s2')
        if skip_l8:   flags.append('skip-l8')
        self._log(f'  Flags: [{", ".join(flags) or "none"}]   Min valid: {min_valid:.0%}')
        self._log('═══════════════════════════════════════════════')

        self._start_heartbeat()
        try:
            if not skip_s2:
                self._total_records += self._stage_raster(
                    's2', region_id=region_id, district_id=district_id,
                    year=year, min_valid=min_valid, overwrite=overwrite,
                )

            if not skip_l8:
                self._total_records += self._stage_raster(
                    'l8', region_id=region_id, district_id=district_id,
                    year=year, min_valid=min_valid, overwrite=overwrite,
                )

            if do_fusion:
                self._stage_fusion(
                    region_id=region_id, district_id=district_id, year=year,
                )
                self._stage_postprocess(region_id=region_id, year=year)

            self._log(f'═══ Pipeline completed in {sw.elapsed()} — '
                      f'{self._total_records} raster records ═══')
            self._flush_log_to_db(final=True)
            self._mark('completed', records=self._total_records)

        except Exception:
            tb = traceback.format_exc()
            self._log('!!! Pipeline failed !!!')
            for line in tb.splitlines():
                self._log_lines.append(line)
            self._flush_log_to_db(final=True)
            self._mark('failed')
            raise
        finally:
            self._stop_heartbeat()
