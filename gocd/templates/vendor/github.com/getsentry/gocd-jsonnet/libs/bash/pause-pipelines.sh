#!/bin/bash

# Note: $ALL_PIPELINE_FLAGS has no quoting, for word expansion
# shellcheck disable=SC2086
if [[ "${ALL_PIPELINE_FLAGS:-}" ]]; then
  set -- $ALL_PIPELINE_FLAGS
fi

# The user that triggered the rollback
TRIGGERED_BY="${TRIGGERED_BY:-}"

pause_message='This pipeline is being rolled back, please check with team before un-pausing.'

# Include triggered by in the pause message if it is not empty
if [ -n "$TRIGGERED_BY" ]; then
  pause_message="$pause_message Triggered by: $TRIGGERED_BY"
fi

# Pause all pipelines in the pipedream
gocd-pause-and-cancel-pipelines \
  --pause-message="$pause_message" \
  "$@"
