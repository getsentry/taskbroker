#!/usr/bin/env python3
from __future__ import annotations

import json
import sys
from typing import Any


def get_env(container: dict[str, Any]) -> dict[str, str]:
    return {
        item["name"]: item["value"]
        for item in container.get("env", [])
        if "name" in item and "value" in item
    }


def main() -> int:
    workloads = json.load(sys.stdin).get("items", [])
    targets = []

    for workload in workloads:
        kind = workload.get("kind", "Workload")
        metadata = workload.get("metadata", {})
        name = metadata.get("name", "")
        spec = workload.get("spec", {})
        pod_template = spec.get("template", {})
        pod_metadata = pod_template.get("metadata", {})
        labels = pod_metadata.get("labels", {})
        selector_labels = spec.get("selector", {}).get("matchLabels", {})
        app = labels.get("app") or selector_labels.get("app")

        if not app:
            print(f"Skipping {kind}/{name}: missing app label", file=sys.stderr)
            continue

        taskbroker = next(
            (
                container
                for container in pod_template.get("spec", {}).get("containers", [])
                if container.get("name") == "taskbroker"
            ),
            None,
        )
        if not taskbroker:
            print(f"Skipping {kind}/{name}: missing taskbroker container", file=sys.stderr)
            continue

        env = get_env(taskbroker)
        targets.append(
            (
                app,
                kind,
                name,
                env.get("TASKBROKER_DATABASE_ADAPTER", "unknown"),
                env.get("TASKBROKER_PG_DATABASE_NAME", ""),
            )
        )

    seen = set()
    for app, kind, name, database_adapter, pg_database_name in sorted(targets):
        if app in seen:
            print(f"Skipping {kind}/{name}: duplicate app label {app}", file=sys.stderr)
            continue
        seen.add(app)
        print("\t".join((app, kind, name, database_adapter, pg_database_name)))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
