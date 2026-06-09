#!/bin/bash

eval "$(regions-project-env-vars --region="${SENTRY_REGION}")"
/devinfra/scripts/get-cluster-credentials

# We ignore StatefulSets as those NEVER run AlloyDB, and we only care about migrations for AlloyDB here
deployments=$(kubectl get deployments -o name | awk -F/ '/task-.*-broker/ {print $2}')

for name in $deployments; do
  LABEL_SELECTOR="app=$name"
  echo "Running migrations for $name..."

  if ! k8s-spawn-job \
    --label-selector="${LABEL_SELECTOR}" \
    --container-name="taskbroker" \
    --try-deployments-and-statefulsets \
    "${name}-migrations" \
    "us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}" \
    /opt/taskbroker \
    -- \
    --run migrations; then
    echo "Migrations failed for $name, ignoring so the pipeline can continue"
    continue
  fi

  echo "Done: $name"
done

exit 0
