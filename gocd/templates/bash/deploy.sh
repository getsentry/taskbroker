#!/bin/bash
set -e

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

WAIT_TIMEOUT=""
if [ "${SENTRY_REGION}" = "us" ]; then
	WAIT_TIMEOUT="--wait-timeout-mins=90"
fi

/devinfra/scripts/get-cluster-credentials

# With ARGOCD_ONLY, k8s-deploy waits forever when nothing matches (e.g. the s4s2 canary has no StatefulSet).
statefulsets=$(kubectl get statefulset -A -l "${LABEL_SELECTOR}" -o name)
if [ -n "${statefulsets}" ]; then
	k8s-deploy \
		--label-selector="${LABEL_SELECTOR}" \
		--image="us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}" \
		--type="statefulset" \
		--container-name="taskbroker" \
		${WAIT_TIMEOUT:+"${WAIT_TIMEOUT}"}
fi

k8s-deploy \
		--label-selector="${LABEL_SELECTOR}" \
		--image="us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}" \
		--type="deployment" \
		--container-name="taskbroker" \
		${WAIT_TIMEOUT:+"${WAIT_TIMEOUT}"};
