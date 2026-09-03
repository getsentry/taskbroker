"""Shared-memory busy/wait accounting for worker children.

Children write their own cumulative totals into a slot; the parent diffs them at
flush. Costs O(children) per second rather than O(tasks) per second, and there is
no queue to fall behind.

Slot layout, five doubles per child::

    0  version         seqlock; odd means a write is in progress
    1  busy_total      cumulative seconds closed into busy
    2  wait_total      cumulative seconds closed into wait
    3  segment_start   time.monotonic() when the open segment began
    4  segment_kind    KIND_NONE, KIND_WAIT or KIND_BUSY

Values are absolute and cumulative, so a torn read costs one transient sample
that the next flush re-derives. time.monotonic() is CLOCK_MONOTONIC, which is
system-wide, so a child's timestamps are valid in the parent.
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

# NONE must be 0.0 so a zeroed slot reads as "nothing open".
KIND_NONE = 0.0
KIND_WAIT = 1.0
KIND_BUSY = 2.0

# Handed to a child when the pool has no slot left. Reads and writes are no-ops.
NO_SLOT = -1

SEQLOCK_READ_ATTEMPTS = 3


@dataclass(frozen=True)
class SampleResult:
    """One child's contribution to a flush, plus why it may be short.

    `eligible` is the window this child's busy and wait were measured over, and
    the pool sums it to get occupancy's denominator. A headcount times the flush
    interval would instead bill a child baselined part-way through as whole.
    """

    busy: float = 0.0
    wait: float = 0.0
    eligible: float = 0.0
    reason: str = "ok"


def slot_count(concurrency: int) -> int:
    """Twice concurrency, so a generation of unreaped exiting children can
    overlap a generation of replacements."""
    return max(1, concurrency * 2)


class ChildTimeWriter:
    """Child-side writer for one slot.

    Sole writer, so it keeps authoritative totals as plain floats and
    republishes the whole slot on each transition.
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
        if self._slot == NO_SLOT or self._shm is None:
            return

        shm = self._shm
        base = self._base

        version = shm[base + SLOT_VERSION]
        # Odd: a reader that sees this discards what it read.
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
    """Parent-side reader for one child's slot.

    Holds the previous absolute reading and returns deltas, so a child in a long
    task contributes to every interval it spans.
    """

    shm: ctypes.Array[ctypes.c_double] | None
    slot: int = NO_SLOT
    _prev_busy: float = 0.0
    _prev_wait: float = 0.0
    _accounted: bool = False
    # Start of the window the next delta will cover. Advances only on a
    # successful read, so a deferred sample carries its eligibility forward with
    # it and `busy + wait <= eligible` survives a seqlock retry.
    _measured_from: float = 0.0

    def mark_running(self, now: float) -> None:
        """Start counting this child, baselining against the slot as it stands.

        Baselining rather than zeroing drops whatever the child banked before the
        parent saw its `running` message, which is the same window over which it
        is absent from `running_count`.
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
        """Return this child's busy/wait since the previous sample.

        `eligible` is the width of that same window, so it is the honest ceiling
        on what this child could have contributed. It is shorter than the flush
        interval for a child baselined part-way through one.
        """
        if not self._accounted:
            return SampleResult(reason="not_accounted")

        eligible = max(0.0, now - self._measured_from)

        reading = self._read(now)
        if reading is None:
            # Neither baseline advances, so the next sample covers both windows.
            return SampleResult(eligible=eligible, reason="read_failed")

        busy_now, wait_now = reading
        raw_busy = busy_now - self._prev_busy
        raw_wait = wait_now - self._prev_wait
        # A total going backwards means a torn read or a reused slot, and the
        # clamp below silently drops that time.
        reason = "clamped" if raw_busy < 0.0 or raw_wait < 0.0 else "ok"

        self._prev_busy = busy_now
        self._prev_wait = wait_now
        self._measured_from = now
        return SampleResult(max(0.0, raw_busy), max(0.0, raw_wait), eligible, reason)

    def _read(self, now: float) -> tuple[float, float] | None:
        """Seqlock read of absolute busy/wait, including the segment still open.

        None means the read could not be taken cleanly.
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

            # Fold in the segment the child is in right now.
            if kind == KIND_BUSY:
                busy += max(0.0, now - start)
            elif kind == KIND_WAIT:
                wait += max(0.0, now - start)

            return (busy, wait)

        return None
