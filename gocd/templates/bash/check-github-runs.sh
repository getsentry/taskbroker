#!/bin/bash

checks-githubactions-checkruns \
	"getsentry/taskbroker" \
	"${GO_REVISION_TASKBROKER_REPO}" \
    "Tests (ubuntu)"
