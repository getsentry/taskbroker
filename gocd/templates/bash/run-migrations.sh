#!/bin/bash

# IMPORTANT: this script is inlined into the GoCD pipeline definition via
# importstr, and GoCD runs parameter substitution over it. A literal hash that
# is not in the first column breaks the config-repo parse, silently freezing the
# pipeline on its last good render.

eval "$(regions-project-env-vars --region="${SENTRY_REGION}")"
/devinfra/scripts/get-cluster-credentials

# Find all Deployments where the number of ready pods is greater than zero
deployments=$(kubectl get deployments -A --no-headers | awk '$2 ~ /^task-.*-broker$/ && $5+0 > 0 {print $2}')

run_migrations() {
  local name="$1"
  local label_selector="app=$name"

  echo "Running migrations for $name..."

# Reuse the broker's own args
  local broker_args=()
  read -r -a broker_args < <(kubectl get deployment "$name" -o json \
    | jq -r '.spec.template.spec.containers[] | select(.name == "taskbroker") | .args // [] | join(" ")')

  if ! k8s-spawn-job \
    --label-selector="${label_selector}" \
    --container-name="taskbroker" \
    --try-deployments-and-statefulsets \
    "${name}-migrations" \
    "us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}" \
    /opt/taskbroker \
    -- \
    "${broker_args[@]}" \
    --run migrations; then
    echo "Migrations failed for $name"
    return 1
  fi

  echo "Done: $name"
}

pids=()

for name in $deployments; do
  run_migrations "$name" &
  pids+=("$!")
done

status=0

for pid in "${pids[@]}"; do
  if ! wait "$pid"; then
    status=1
  fi
done

exit "$status"
