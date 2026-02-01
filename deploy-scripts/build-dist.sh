#!/usr/bin/env bash
set -Eeuo pipefail

# Always run from project root, regardless of where the script is invoked
cd -- "$(dirname -- "$0")"/..

./deploy-scripts/build-all-requirements-txt.sh && ./deploy-scripts/build-all-lambdas.sh
