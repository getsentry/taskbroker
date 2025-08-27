#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

WAIT_TIMEOUT=""
if [ "${SENTRY_REGION}" = "us" ]; then
	WAIT_TIMEOUT="--wait-timeout-mins=60"
fi

/devinfra/scripts/get-cluster-credentials && k8s-deploy \
		--label-selector="${LABEL_SELECTOR}" \
		--image="us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}" \
		--type="statefulset" \
		--container-name="taskbroker" \
		${WAIT_TIMEOUT:+"${WAIT_TIMEOUT}"};
