#!/bin/bash

checks-githubactions-checkruns \
	"getsentry/taskbroker" \
	"${GO_REVISION_TASKBROKER_REPO}" \
	"Build and push production images to single-region registry" \
	"Build and push production images to multi-region registry" \
	"Tests (ubuntu)"
