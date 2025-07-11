#!/bin/bash

checks-googlecloud-check-cloudbuild \
	sentryio \
	taskbroker \
	build-on-taskbroker-branch-push \
	"${GO_REVISION_TASKBROKER_REPO}" \
	main
