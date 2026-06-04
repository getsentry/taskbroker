#!/bin/bash

set -euo pipefail

eval "$(regions-project-env-vars --region="${SENTRY_REGION}")"
/devinfra/scripts/get-cluster-credentials

IMAGE="us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}"
NAMESPACE="${NAMESPACE:-default}"

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DISCOVER_MIGRATION_TARGETS="${DISCOVER_MIGRATION_TARGETS:-}"

if [ -z "${DISCOVER_MIGRATION_TARGETS}" ]; then
  for candidate in \
    "${script_dir}/discover-migration-targets.py" \
    "taskbroker/gocd/templates/bash/discover-migration-targets.py" \
    "gocd/templates/bash/discover-migration-targets.py"; do
    if [ -f "${candidate}" ]; then
      DISCOVER_MIGRATION_TARGETS="${candidate}"
      break
    fi
  done
fi

if [ -z "${DISCOVER_MIGRATION_TARGETS}" ]; then
  echo "Could not find discover-migration-targets.py"
  exit 1
fi

migration_targets="$(
  kubectl get deployments,statefulsets \
    --namespace="${NAMESPACE}" \
    --selector="${LABEL_SELECTOR}" \
    --output=json \
    | python3 "${DISCOVER_MIGRATION_TARGETS}"
)"

if [ -z "${migration_targets}" ]; then
  echo "No taskbroker workloads found for selector ${LABEL_SELECTOR}"
  exit 0
fi

while IFS=$'\t' read -r app kind name database_adapter pg_database_name; do
  label_selector="app=${app},service=taskbroker"
  job_name="tb-migrate-${app}"
  job_name="${job_name:0:50}"

  echo "Running taskbroker migrations for ${kind}/${name} (${database_adapter}${pg_database_name:+:${pg_database_name}}) using selector ${label_selector}"

  k8s-spawn-job \
    --label-selector="${label_selector}" \
    --container-name="taskbroker" \
    --namespace="${NAMESPACE}" \
    --try-deployments-and-statefulsets \
    "${job_name}" \
    "${IMAGE}" \
    /opt/taskbroker \
    -- \
    --run migrations
done <<< "${migration_targets}"
