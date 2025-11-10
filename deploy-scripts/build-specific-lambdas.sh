#!/usr/bin/env bash
set -Eeuo pipefail

# "Import" all shared functions and variables
source "$(dirname -- "$0")/_build_helpers.sh"

# --- Main Execution ---
echo "INFO: Building specified services..."
echo

# Create the build/dist directories if they don't exist
echo "SETUP: Cleaning and creating build directories..."
rm -rf "${BUILD_DIR}" "${DIST_DIR}"
mkdir -p "${BUILD_DIR}" "${DIST_DIR}"
echo

# Explicitly list lambdas to build
build_lambda_zip "start_job_worker"
build_lambda_zip "process_step_worker"
build_lambda_zip "complete_job_worker"
build_lambda_zip "create_job_endpoint"
build_lambda_zip "list_job_events_endpoint"

echo "SUCCESS: Build complete. All artifacts are in ${DIST_DIR_REL}"
