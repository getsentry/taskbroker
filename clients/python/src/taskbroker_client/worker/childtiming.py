"""Shared-memory busy/wait accounting for worker children.

Each child owns a slot and writes its own cumulative totals into it. The parent
reads every slot once per flush and diffs against its previous reading, so the
cost is O(children) per second rather than O(tasks) per second.

The parent reads rather than having children emit their own metrics because a
child only knows a segment's length once it ends: a child sitting in a 30s task
would report nothing for 30 flushes and then 30s at once. The parent folds the
open segment forward at read time instead, so that child contributes to every
interval it spans.

Slot layout, five doubles per child::

    0  version         seqlock; odd means a write is in progress
    1  busy_total      cumulative seconds closed into busy
    2  wait_total      cumulative seconds closed into wait
    3  segment_start   time.monotonic() when the open segment began
    4  segment_kind    KIND_NONE, KIND_WAIT or KIND_BUSY

Every value is absolute, so a torn read costs one transient sample that the next
flush re-derives from the totals rather than accumulating drift.
`time.monotonic()` is CLOCK_MONOTONIC, which is system-wide, so a child's
timestamps are directly comparable in the parent.
"""

from __future__ import annotations

import ctypes
from dataclasses import dataclass

SLOT_VERSION = 0
SLOT_BUSY_TOTAL = 1
SLOT_WAIT_TOTAL = 2
SLOT_SEGMENT_START = 3
SLOT_SEGMENT_KIND = 4
SLOT_WIDTH = 5

# NONE must be 0.0 so a freshly zeroed slot reads as "nothing open". Any other
# value and the parent would fold `now - 0.0` forward as elapsed time.
KIND_NONE = 0.0
KIND_WAIT = 1.0
KIND_BUSY = 2.0

# Handed to a child when the pool has no slot left. Reads and writes are no-ops.
NO_SLOT = -1

SEQLOCK_READ_ATTEMPTS = 3


@dataclass(frozen=True)
class SampleResult:
    """One child's busy and wait, and the window they were measured over.

    `busy + wait` should equal `eligible`. The pool sums all three and compares
    them to catch time that was double-counted or dropped.
    """

    busy: float = 0.0
    wait: float = 0.0
    eligible: float = 0.0


def slot_count(concurrency: int) -> int:
    """Twice concurrency, so a generation of unreaped exiting children can
    overlap a generation of replacements."""
    return max(1, concurrency * 2)


class ChildTimeWriter:
    """Child-side writer for one slot.

    The child is the only writer, so it keeps authoritative totals as plain
    floats and republishes the whole slot on each transition.
    """

    __slots__ = ("_shm", "_slot", "_base", "_busy_total", "_wait_total", "_start", "_kind")

    def __init__(self, shm: ctypes.Array[ctypes.c_double] | None, slot: int) -> None:
        self._shm = shm
        self._slot = NO_SLOT if shm is None else slot
        self._base = slot * SLOT_WIDTH
        self._busy_total = 0.0
        self._wait_total = 0.0
        self._start = 0.0
        self._kind = KIND_NONE

    def _publish(self) -> None:
        """Write the slot behind an odd version, so a reader can tell it raced.

        The five stores are not atomic together, and a reader that caught a new
        `busy_total` beside a stale `segment_start` would count the same span
        twice. Bracketing them makes that detectable.
        """
        if self._slot == NO_SLOT or self._shm is None:
            return

        shm = self._shm
        base = self._base

        version = shm[base + SLOT_VERSION]
        shm[base + SLOT_VERSION] = version + 1.0
        shm[base + SLOT_BUSY_TOTAL] = self._busy_total
        shm[base + SLOT_WAIT_TOTAL] = self._wait_total
        shm[base + SLOT_SEGMENT_START] = self._start
        shm[base + SLOT_SEGMENT_KIND] = self._kind
        shm[base + SLOT_VERSION] = version + 2.0

    def _close_open(self, now: float) -> None:
        if self._kind == KIND_BUSY:
            self._busy_total += max(0.0, now - self._start)
        elif self._kind == KIND_WAIT:
            self._wait_total += max(0.0, now - self._start)
        self._kind = KIND_NONE

    def mark_running(self, now: float) -> None:
        """Open the wait clock. A warmed-up child with no task yet is waiting."""
        if self._kind != KIND_NONE:
            return

        self._start = now
        self._kind = KIND_WAIT
        self._publish()

    def mark_busy(self, now: float) -> None:
        """Close the open wait segment and open a busy one."""
        self._close_open(now)
        self._start = now
        self._kind = KIND_BUSY
        self._publish()

    def mark_idle(self, now: float) -> None:
        """Close the open busy segment and open a wait one."""
        self._close_open(now)
        self._start = now
        self._kind = KIND_WAIT
        self._publish()

    def close(self, now: float) -> None:
        """Close whatever is open so a departing child stops folding time forward."""
        self._close_open(now)
        self._publish()


@dataclass
class ChildTimeAccounting:
    """Parent-side reader for one child's slot."""

    shm: ctypes.Array[ctypes.c_double] | None
    slot: int = NO_SLOT
    _prev_busy: float = 0.0
    _prev_wait: float = 0.0
    _accounted: bool = False
    _measured_from: float = 0.0

    def mark_running(self, now: float) -> None:
        """Start counting this child, baselining against the slot as it stands.

        Baselining rather than zeroing discards whatever the child banked before
        the parent saw its `running` message, which is the same window over
        which it is absent from the pool's running count.
        """
        reading = self._read(now)
        if reading is None:
            self._prev_busy = 0.0
            self._prev_wait = 0.0
        else:
            self._prev_busy, self._prev_wait = reading

        self._accounted = True
        self._measured_from = now

    def mark_stopped(self) -> None:
        """Stop counting, so an exiting child's tail misses the live pool."""
        self._accounted = False

    def sample(self, now: float) -> SampleResult:
        """Return this child's busy and wait since the previous sample."""
        if not self._accounted:
            return SampleResult()

        reading = self._read(now)
        if reading is None:
            # Advance nothing. The next sample then covers both windows, and its
            # busy and eligible grow together instead of one outrunning the other.
            return SampleResult()

        busy_now, wait_now = reading
        eligible = max(0.0, now - self._measured_from)
        # A total going backwards means a torn read or a slot reused under a
        # live writer. Clamping drops that time, which the pool then sees as a
        # deficit against `eligible`.
        busy = max(0.0, busy_now - self._prev_busy)
        wait = max(0.0, wait_now - self._prev_wait)

        self._prev_busy = busy_now
        self._prev_wait = wait_now
        self._measured_from = now
        return SampleResult(busy, wait, eligible)

    def _read(self, now: float) -> tuple[float, float] | None:
        """Seqlock read of absolute busy/wait, including the segment still open.

        Retries on an odd version (a write is in progress) or a changed one (a
        write started and finished mid-read). Returns None if it never got a
        clean pass, which the caller treats as "defer", not "zero".
        """
        if self.slot == NO_SLOT or self.shm is None:
            return None

        shm = self.shm
        base = self.slot * SLOT_WIDTH

        for _ in range(SEQLOCK_READ_ATTEMPTS):
            version = shm[base + SLOT_VERSION]
            if version % 2.0:
                continue

            busy = shm[base + SLOT_BUSY_TOTAL]
            wait = shm[base + SLOT_WAIT_TOTAL]
            start = shm[base + SLOT_SEGMENT_START]
            kind = shm[base + SLOT_SEGMENT_KIND]

            if shm[base + SLOT_VERSION] != version:
                continue

            # Fold in the segment the child is in right now, so a long task
            # contributes to every interval it spans instead of landing all at
            # once when it finally ends.
            if kind == KIND_BUSY:
                busy += max(0.0, now - start)
            elif kind == KIND_WAIT:
                wait += max(0.0, now - start)

            return (busy, wait)

        return None
