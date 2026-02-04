#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
ROOT_DIR=$(dirname $SCRIPT_DIR)

cd $ROOT_DIR/clients/python

uv version "${NEW_VERSION}"
