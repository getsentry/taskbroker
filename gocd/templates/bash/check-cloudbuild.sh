#!/bin/bash

/devinfra/scripts/checks/googlecloud/check_cloudbuild.py \
	sentryio \
	taskbroker \
	build-on-taskbroker-branch-push \
	"${GO_REVISION_TASKBROKER_REPO}" \
	main
