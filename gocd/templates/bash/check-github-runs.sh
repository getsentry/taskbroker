#!/bin/bash

checks-githubactions-checkruns \
	"getsentry/taskbroker" \
	"${GO_REVISION_TASKBROKER_REPO}" \
	"Build and push production images" \
	"Tests (ubuntu)"
