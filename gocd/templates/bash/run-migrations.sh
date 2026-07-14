#!/bin/bash

eval "$(regions-project-env-vars --region="${SENTRY_REGION}")"
/devinfra/scripts/get-cluster-credentials

# DEBUG (temporary): make k8s-spawn-job pprint the spawned pod spec.
export DEBUG=1

# Find all Deployments where the number of ready pods is greater than zero
deployments=$(kubectl get deployments -A --no-headers | awk '$2 ~ /^task-.*-broker$/ && $5+0 > 0 {print $2}')

run_migrations() {
  local name="$1"
  local label_selector="app=$name"

  echo "Running migrations for $name..."

  # ---- DEBUG (temporary): find where --config is lost. Remove after diagnosing. ----
  echo "DEBUG[$name]: kubectl client => $(kubectl version --client 2>&1 | head -1)"
  echo "DEBUG[$name]: context namespace => [$(kubectl config view --minify -o jsonpath='{..namespace}' 2>&1)]"
  echo "DEBUG[$name]: deployment lookup (default ns) => $(kubectl get deployment "$name" -o name 2>&1)"
  echo "DEBUG[$name]: live container command/args/mounts (jq) =>"
  kubectl get deployment "$name" -o json 2>/dev/null \
    | jq -c '.spec.template.spec.containers[] | select(.name == "taskbroker") | {command, args, mounts: [.volumeMounts[]?.mountPath]}' 2>&1 \
    | sed "s/^/DEBUG[$name]:   /"
  echo "DEBUG[$name]: argsA filter-jsonpath  => [$(kubectl get deployment "$name" -o jsonpath='{.spec.template.spec.containers[?(@.name=="taskbroker")].args[*]}' 2>&1)]"
  echo "DEBUG[$name]: argsB nofilter-jsonpath => [$(kubectl get deployment "$name" -o jsonpath='{.spec.template.spec.containers[*].args[*]}' 2>&1)]"
  echo "DEBUG[$name]: argsC jq               => [$(kubectl get deployment "$name" -o json 2>/dev/null | jq -r '.spec.template.spec.containers[] | select(.name == "taskbroker") | .args // [] | join(" ")' 2>&1)]"
  # ---- end DEBUG ----

  # Reuse the broker's own args.
  local broker_args=()
  read -r -a broker_args < <(kubectl get deployment "$name" \
    -o jsonpath='{.spec.template.spec.containers[?(@.name=="taskbroker")].args[*]}') || true
  echo "DEBUG[$name]: broker_args count=${#broker_args[@]} => ${broker_args[*]}"

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
