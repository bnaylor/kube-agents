"""Refuse a kanban_link whose child card has already started.

Installed into the image at ``/opt/hermes/tools/kanban_link_running_guard.py``
and wired into the ``kanban_link`` tool handler (``_handle_link`` in
``tools/kanban_tools.py``) by
``deploy/docker/patches/apply_kanban_link_running_guard.py``.

Why
---
In upstream's kanban a ``task_links`` row is a "runs after" edge: the child
can't be claimed until every parent is done (``claim_task``'s
``_parents_satisfied`` gate in ``hermes_cli/kanban_db.py``). ``link_tasks``
demotes a ``ready`` child to ``todo`` and does nothing else, so linking a card
that is already running leaves its run in place but gated behind the new
parent.

On a ``next`` rehearsal install (issue #3248, 2026-10-15) the Platform Agent
fanned out a child with ``kanban_create`` and then called
``kanban_link(parent_id=<its own card>, child_id=<the child>)``. The child was
already running. Its run's ``kanban_complete`` calls were refused, the run
ended as ``timed_out``, the re-claim came back ``claim_rejected
{'reason': 'parents_not_done'}``, and the child only re-ran after the parent
finished. The thread got two answers, the second with no "Late result" lead,
because #2849's fold steps aside for a child its creator has linked.

A link only means something for a card that hasn't started: it's how you
queue work to run after another card. A card that is running, claimed, or has
already had a run has nothing left to gate, only a run to strand. So the
handler refuses those links, whoever the parent is, and says what to do
instead.

What counts as started
----------------------
Upstream's own markers, read through upstream's own helpers:

* ``tasks.status == 'running'`` (``kb.get_task``);
* a ``tasks.claim_lock`` (the claim CAS in ``_claim_and_open_run`` sets it);
* any ``task_runs`` row for the card (``kb.latest_run``), open or closed.

Scope
-----
Only the ``kanban_link`` tool. ``kanban_create``'s ``parents`` argument is
untouched: creating a card with ``parents=[<your card>]`` is how a worker
queues follow-up work, and the Platform SOUL depends on it. A link to a card
that has never started (``todo``, ``ready``, ``triage``, ...) is untouched.

The check and upstream's ``link_tasks`` run in separate transactions
(``link_tasks`` opens its own ``write_txn``, which refuses to nest), so a
dispatcher claim landing in the milliseconds between them is not caught. The
incident is a link made well after the claim.

Fail-open: a board that can't be read lets the link through to upstream,
which reports unknown ids and cycles itself.
"""

from __future__ import annotations

import logging
import os

try:  # In the image, /opt/hermes is on sys.path.
    from tools.kanban_ownership import WORKER_TASK_ENV
except ImportError:  # Local unittest discovery: the patches dir is top-level.
    from kanban_ownership import WORKER_TASK_ENV

logger = logging.getLogger(__name__)

#: ``tasks.status`` for a card a worker holds (``_claim_and_open_run``).
RUNNING_STATUS = "running"

#: Stands in for the caller's card id when the caller isn't a worker.
UNKNOWN_OWN_CARD = "your card"

LINK_REFUSAL = (
    "kanban_link refused: {child} is already running, and a link now would "
    "make it wait for {parent} and strand its run. A card you want running "
    "now needs no link; to queue work to run after yours, create it with "
    "parents=[{own}]."
)


def child_has_started(kb, conn, child_id: str) -> bool:
    """Whether ``child_id`` is running, claimed, or has had any run."""
    task = kb.get_task(conn, child_id)
    if task is None:
        return False
    if task.status == RUNNING_STATUS or task.claim_lock:
        return True
    return kb.latest_run(conn, child_id) is not None


def refuse_started_child_link(kb, conn, parent_id: str, child_id: str) -> str | None:
    """The ``kanban_link`` gate. ``None`` when the link may proceed."""
    if not parent_id or not child_id or parent_id == child_id:
        return None  # upstream's own checks answer these
    try:
        if kb.get_task(conn, parent_id) is None:
            return None  # upstream reports the unknown id
        if not child_has_started(kb, conn, child_id):
            return None
    except Exception as exc:  # noqa: BLE001 — fail open, but say so
        logger.warning(
            "kanban_link_running_guard: could not read %s, so the link to it "
            "is unguarded: %r", child_id, exc,
        )
        return None
    own = os.environ.get(WORKER_TASK_ENV, "").strip() or UNKNOWN_OWN_CARD
    logger.warning(
        "kanban_link_running_guard: refused kanban_link %s -> %s: the child "
        "has already started", parent_id, child_id,
    )
    return LINK_REFUSAL.format(child=child_id, parent=parent_id, own=own)
