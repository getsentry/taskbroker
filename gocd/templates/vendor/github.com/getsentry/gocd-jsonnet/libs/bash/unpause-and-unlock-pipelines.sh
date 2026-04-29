#!/bin/bash

# Note: $ALL_PIPELINE_FLAGS has no quoting, for word expansion
# shellcheck disable=SC2086
if [[ "${ALL_PIPELINE_FLAGS:-}" ]]; then
  set -- $ALL_PIPELINE_FLAGS
fi

# Unpause and unlock all pipelines in the pipedream
gocd-unpause-and-unlock-pipelines \
  "$@"
