"""Shared-memory busy/wait accounting for worker children.

Once a second the parent needs to know how many seconds each child spent
executing versus waiting for work. To accomplish that each child owns a slot
in a ``RawArray`` of doubles and writes its own cumulative totals there. The
parent reads and diffs the slots at flush time.

Slot layout, five doubles per child::

    0  version         seqlock; odd means a write is in progress
    1  busy_total      cumulative seconds closed into busy
    2  wait_total      cumulative seconds closed into wait
    3  segment_start   time.monotonic() when the currently-open segment began
    4  segment_kind    KIND_NONE, KIND_WAIT or KIND_BUSY

Two properties carry the design.

Every value is absolute and cumulative rather than a delta, which is what makes
a torn read survivable: a bad sample is transient and the next one re-derives
the truth from the slot, so error cannot accumulate.

``KIND_NONE`` is zero, so a freshly zeroed slot reads as "this child has not
accounted for anything yet" rather than as an open segment starting at time
zero.

``time.monotonic()`` is CLOCK_MONOTONIC, which is system-wide, so a child's
timestamps are directly comparable in the parent.
"""

from __future__ import annotations

import ctypes
from dataclasses import dataclass

# Offsets within a slot, and the slot stride.
SLOT_VERSION = 0
SLOT_BUSY_TOTAL = 1
SLOT_WAIT_TOTAL = 2
SLOT_SEGMENT_START = 3
SLOT_SEGMENT_KIND = 4
SLOT_WIDTH = 5

# Kind values. NONE must be 0.0 so that a zeroed slot means "nothing open".
KIND_NONE = 0.0
KIND_WAIT = 1.0
KIND_BUSY = 2.0

# Slot index handed to a child when the pool has none left. Every read and
# write becomes a no-op and the parent leaves that child out of occupancy.
NO_SLOT = -1

# A writer holds the seqlock for four stores, so a reader that loses three
# races in a row is seeing something other than ordinary contention.
SEQLOCK_READ_ATTEMPTS = 3


def slot_count(concurrency: int) -> int:
    """How many slots a pool of `concurrency` children needs.

    Twice concurrency. `spawn_children_thread` counts only non-exiting children
    when deciding how many to spawn, so a full generation of exiting-but-unreaped
    children can briefly coexist with a full generation of replacements.
    """
    return max(1, concurrency * 2)


class ChildTimeWriter:
    """Child-side writer for one slot.

    The child is the only writer for its slot, so it keeps the authoritative
    totals as plain Python floats and republishes the whole slot on each
    transition. That avoids a read-modify-write against shared memory.
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
        # Odd version: a reader that sees this discards what it read.
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

    Holds the previous absolute reading and returns deltas. A child sitting in a
    long task therefore contributes to every interval it spans, instead of
    dumping its whole duration into the interval it happens to finish in.
    """

    shm: ctypes.Array[ctypes.c_double] | None
    slot: int = NO_SLOT
    _prev_busy: float = 0.0
    _prev_wait: float = 0.0
    _accounted: bool = False

    def mark_running(self, now: float) -> None:
        """Start counting this child, baselining against the slot as it stands.

        Baselining rather than zeroing means whatever the child banked between
        spawning and the parent seeing its `running` message is not credited
        retroactively. The child is excluded from `running_count` over that same
        window, so the numerator and the denominator start together.
        """
        reading = self._read(now)
        if reading is None:
            # Slots are zeroed at allocation, so a failed first read costs at
            # most the few microseconds since the child came up.
            self._prev_busy = 0.0
            self._prev_wait = 0.0
        else:
            self._prev_busy, self._prev_wait = reading

        self._accounted = True

    def mark_stopped(self) -> None:
        """Stop counting this child, so an exiting child's tail does not land on
        the live pool's occupancy."""
        self._accounted = False

    def sample(self, now: float) -> tuple[float, float]:
        """Return (busy, wait) seconds accrued since the previous sample."""
        if not self._accounted:
            return (0.0, 0.0)

        reading = self._read(now)
        if reading is None:
            # Leave the baseline alone: the next sample then covers both
            # intervals. Deferring the attribution beats dropping it.
            return (0.0, 0.0)

        busy_now, wait_now = reading
        busy = max(0.0, busy_now - self._prev_busy)
        wait = max(0.0, wait_now - self._prev_wait)
        self._prev_busy = busy_now
        self._prev_wait = wait_now
        return (busy, wait)

    def _read(self, now: float) -> tuple[float, float] | None:
        """Seqlock read of absolute busy/wait, including the segment still open.

        None means the read could not be taken cleanly and the caller should
        keep whatever baseline it already has.
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
