#!/usr/bin/env bash
set -Eeuo pipefail

# "Import" all shared functions and variables
source "$(dirname -- "$0")/_build_helpers.sh"

# --- Main Execution ---
echo "INFO: Building requirements for specified services..."
echo

# Explicitly list lambdas to generate requirements for
build_requirements_txt "start_job_worker"
build_requirements_txt "process_step_worker"
build_requirements_txt "complete_job_worker"
build_requirements_txt "create_job_endpoint"
build_requirements_txt "list_job_events_endpoint"

echo "SUCCESS: Done. All services have been processed."
