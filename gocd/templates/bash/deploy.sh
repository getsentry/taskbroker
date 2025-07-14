#!/bin/bash

eval $(regions-project-env-vars --region="${SENTRY_REGION}")

/devinfra/scripts/get-cluster-credentials &&
	k8s-deploy \
		--label-selector="${LABEL_SELECTOR}" \
		--image="us-central1-docker.pkg.dev/sentryio/taskbroker/image:${GO_REVISION_TASKBROKER_REPO}" \
		--type="statefulset" \
		--container-name="taskbroker";
