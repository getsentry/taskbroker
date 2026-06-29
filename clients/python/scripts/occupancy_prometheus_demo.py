"""
Local demo: watch worker occupancy on the Prometheus endpoint.

Spins up a real TaskWorkerProcessingPool (no broker needed), feeds it slow
`examples.timed` tasks to saturate the child slots, and exposes occupancy on a
Prometheus /metrics endpoint. Use it to confirm the scrape endpoint works and
that occupancy tracks real load.

    python scripts/occupancy_prometheus_demo.py --port 9100 --concurrency 4

Then in another terminal:

    watch -n1 'curl -s localhost:9100/metrics | grep taskworker_worker_occupancy'

You should see occupancy climb toward 1.0 while tasks are fed, then fall to 0
once the feeder stops.
"""

from __future__ import annotations

import argparse
import multiprocessing as mp
import threading
import time

import msgpack
from sentry_protos.taskbroker.v1.taskbroker_pb2 import TaskActivation

from taskbroker_client.types import InflightTaskActivation
from taskbroker_client.worker.worker import TaskWorkerProcessingPool


def make_task(task_id: int, sleep_seconds: float) -> InflightTaskActivation:
    return InflightTaskActivation(
        host="localhost:0",
        receive_timestamp=time.monotonic(),
        activation=TaskActivation(
            id=str(task_id),
            taskname="examples.timed",
            namespace="examples",
            parameters_bytes=msgpack.packb(
                {"args": [sleep_seconds], "kwargs": {}}, use_bin_type=True
            ),
            processing_deadline_duration=60,
        ),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=9100, help="Prometheus port")
    parser.add_argument("--concurrency", type=int, default=4, help="child processes")
    parser.add_argument("--task-seconds", type=float, default=2.0, help="task duration")
    parser.add_argument("--feed-seconds", type=float, default=20.0, help="how long to feed load")
    parser.add_argument("--drain-seconds", type=float, default=15.0, help="observe drain after")
    args = parser.parse_args()

    pool = TaskWorkerProcessingPool(
        app_module="examples.app:app",
        mp_context=mp.get_context("fork"),
        # No broker in this demo: results are simply dropped.
        send_result_fn=lambda results, is_draining: None,
        concurrency=args.concurrency,
        processing_pool_name="local-demo",
        prometheus_port=args.port,
    )
    pool.start_metrics_thread()
    pool.start_result_thread()
    pool.start_spawn_children_thread()

    while pool.ready_count < args.concurrency:
        time.sleep(0.1)
    print(
        f"{args.concurrency} children ready. "
        f"Scrape: curl -s localhost:{args.port}/metrics | grep occupancy"
    )

    stop_feeding = threading.Event()

    def feeder() -> None:
        i = 0
        while not stop_feeding.is_set():
            # push_task blocks when the child queue is full, which naturally
            # keeps the slots saturated.
            pool.push_task(make_task(i, args.task_seconds))
            i += 1

    threading.Thread(target=feeder, name="feeder", daemon=True).start()

    def report(phase: str, seconds: float) -> None:
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            busy = max(0, min(pool._busy_counter.value, args.concurrency))
            occ = busy / args.concurrency if args.concurrency else 0.0
            print(f"[{phase}] busy={busy}/{args.concurrency} occupancy={occ:.2f}")
            time.sleep(1)

    report("feeding", args.feed_seconds)
    print("--- stopping feeder, watching drain ---")
    stop_feeding.set()
    report("draining", args.drain_seconds)

    pool.shutdown()


if __name__ == "__main__":
    main()
