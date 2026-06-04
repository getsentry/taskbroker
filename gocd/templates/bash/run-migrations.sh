#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")
/devinfra/scripts/get-cluster-credentials

deployments=$(kubectl get deployments | grep 'task-.*-broker' | awk '{print $1}')

for name in $deployments; do
  LABEL_SELECTOR="app=$name"
  echo "Running migrations for $name..."

  k8s-spawn-job \
    --label-selector="${LABEL_SELECTOR}" \
    --container-name="taskbroker" \
    "taskbroker-migrations" \
    "us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}" \
    /opt/taskbroker \
    -- \
    --run migrations

  echo "Done: $name"
done
