#!/usr/bin/env bash
set -Eeuo pipefail

# "Import" all shared functions and variables from the helper file
# This line assumes the helper file is in the same directory.
source "$(dirname -- "$0")/_build_helpers.sh"

# Create the build/dist directories if they don't exist
echo "SETUP: Cleaning and creating build directories..."
rm -rf "${BUILD_DIR}" "${DIST_DIR}"
mkdir -p "${BUILD_DIR}" "${DIST_DIR}"
echo

# --- Main Execution ---
echo "INFO: Auto-discovering services in ${SERVICES_DIR_REL}/"

# Find all Lambda service subfolders
SERVICE_PATHS=()
while IFS= read -r -d '' SvcDir; do
	SERVICE_PATHS+=("$SvcDir")
done < <(find "${SERVICES_DIR}" -mindepth 1 -maxdepth 1 -type d -print0 | sort -z)

if ((${#SERVICE_PATHS[@]} == 0)); then
	echo "INFO: No service folders found in ${SERVICES_DIR_REL}."
	exit 0
fi

echo "INFO: Found ((${#SERVICE_PATHS[@]})) potential directories. Scanning for service handlers..."
echo
SCRIPT_FAILED=0
for svc_path in "${SERVICE_PATHS[@]}"; do
	svc_name="$(basename "${svc_path}")"
	handler_file="${svc_path}/${svc_name}.py"

	if [[ -f "${handler_file}" ]]; then
		echo "--- Found service: ${svc_name} ---"
		if ! build_lambda_zip "${svc_name}"; then
			SCRIPT_FAILED=1
		fi
	else
		echo "--- Skipping: ${svc_name} (Not a service) ---"
		echo
	fi
done

if [[ "${SCRIPT_FAILED}" -eq 1 ]]; then
	echo "ERROR: One or more services failed to build."
	exit 1
else
	echo "SUCCESS: Build complete. All artifacts are in ${DIST_DIR_REL}"
fi
