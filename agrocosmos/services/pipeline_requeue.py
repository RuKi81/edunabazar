"""Requeue support for worker-executed pipeline runs interrupted by SIGTERM.

When the ``worker`` container is restarted (deploy, ``docker compose up``),
the in-flight pipeline receives SIGTERM. Instead of marking the run
``failed`` and losing hours of downloaded composites, the orchestrator
puts the run back into ``status=queued`` so the freshly started worker
picks it up again. Downloads are resumable (existing rasters on disk are
skipped), so the run continues roughly where it left off.

``_requeues`` is tracked inside ``launch_args`` (underscore-prefixed keys
are stripped by the worker before ``call_command``) and capped so a run
can never loop forever.
"""
from __future__ import annotations

MAX_REQUEUES = 5
REQUEUE_COUNT_KEY = '_requeues'


def try_requeue(run_id: int | None) -> bool:
    """Put a SIGTERM-interrupted run back into the queue.

    Returns True if the run was requeued (worker will pick it up after
    restart), False if the caller should mark it failed instead.

    Requeue happens only when:
    - the run row exists, is still ``running`` and has non-empty
      ``launch_args`` (i.e. it was launched via the worker queue and can
      be re-executed with the same arguments);
    - the requeue counter has not exceeded :data:`MAX_REQUEUES`.
    """
    if not run_id:
        return False
    from django.utils import timezone

    from agrocosmos.models import PipelineRun

    try:
        run = (
            PipelineRun.objects.filter(pk=run_id)
            .values('status', 'launch_args').first()
        )
        if not run or run['status'] != PipelineRun.Status.RUNNING:
            return False
        args = dict(run['launch_args'] or {})
        if not args:
            return False  # legacy detached launch — nothing to re-run
        count = int(args.get(REQUEUE_COUNT_KEY) or 0)
        if count >= MAX_REQUEUES:
            return False
        args[REQUEUE_COUNT_KEY] = count + 1
        updated = PipelineRun.objects.filter(
            pk=run_id, status=PipelineRun.Status.RUNNING,
        ).update(
            status=PipelineRun.Status.QUEUED,
            launch_args=args,
            pid=None,
            heartbeat_at=timezone.now(),
        )
        return updated == 1
    except Exception:
        return False
