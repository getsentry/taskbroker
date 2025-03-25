#!/bin/bash

/devinfra/scripts/checks/githubactions/checkruns.py \
	"getsentry/taskbroker" \
	"${GO_REVISION_TASKBROKER_REPO}" \
    "Tests (ubuntu)"
