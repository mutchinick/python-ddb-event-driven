#!/usr/bin/env bash

# This script is meant to be sourced, not run directly.
# It contains shared variables and functions for building.

set -Eeuo pipefail

# --- 1. SET UP ALL SHARED VARIABLES ---
# All paths are relative to the project root
PROJECT_ROOT="$(pwd)"
SERVICES_DIR="${PROJECT_ROOT}/services"
DIST_DIR="${PROJECT_ROOT}/.dist/services"
BUILD_DIR="${PROJECT_ROOT}/.build"

# Relative paths for cleaner logging
SERVICES_DIR_REL="services"
DIST_DIR_REL=".dist/services"

# Binaries
PYTHON_BIN="${PYTHON_BIN:-python3}"
PIP_BIN="${PYTHON_BIN}"
PIPREQS_BIN="${PROJECT_ROOT}/.venv/bin/pipreqs"

# --- 2. DEFINE THE 'build_requirements_txt' FUNCTION (FOR REQUIREMENTS) ---
build_requirements_txt() {
	local name="$1"
	local svc_dir="${SERVICES_DIR}/${name}"
	local out_file="${svc_dir}/requirements.txt"
	local tmp_req="${svc_dir}/.requirements.pipreqs.tmp"
	local extra="${svc_dir}/extra-requirements.txt"
	local out_file_rel="services/${name}/requirements.txt"
	local handler_file="${svc_dir}/${name}.py"
	local tmp_scan_dir="${BUILD_DIR}/.tmp_scan/${name}"

	if [[ ! -d "${svc_dir}" ]]; then
		echo "ERROR: Directory services/${name} not found."
		return 1
	fi
	if [[ ! -f "${handler_file}" ]]; then
		echo "ERROR: Handler file ${handler_file} not found. Cannot analyze dependencies."
		return 1
	fi

	echo "PROCESSING: services/${name}"
	rm -rf "${tmp_scan_dir}"
	mkdir -p "${tmp_scan_dir}"
	rsync -a --exclude="__pycache__/" "${svc_dir}/" "${tmp_scan_dir}/"

	local used_modules
	used_modules=$(grep -oE "^from services\.([a-zA-Z0-9_.]*)" "${handler_file}" |
		sed -E 's/from services\.//' |
		sed -E 's/\..*//' |
		sort -u)

	for mod_name in ${used_modules}; do
		local mod_path="${SERVICES_DIR}/${mod_name}"
		if [[ -d "${mod_path}" && "${mod_name}" != "${name}" ]]; then
			echo "  -> Copying shared module: services/${mod_name} to scan directory"
			rsync -a --exclude="__pycache__/" "${mod_path}/" "${tmp_scan_dir}/"
		fi
	done

	if ! "${PIPREQS_BIN}" "${tmp_scan_dir}" \
		--force \
		--savepath "${tmp_req}" \
		--encoding utf-8; then
		echo "WARNING: pipreqs failed for services/${name}. The requirements file may be incomplete."
		rm -rf "${tmp_scan_dir}"
		return 1
	fi

	{
		if [[ -s "${tmp_req}" ]]; then cat "${tmp_req}"; fi
		if [[ -f "${extra}" ]]; then cat "${extra}"; fi
	} |
		sed -E '/^(boto3|botocore|awslambdaric|services|aws_lambda_typing|mypy_boto3_dynamodb|pytest|pytest-mock|pytest-cov)([=<>!].*)?$/d' |
		awk 'NF' |
		sort -u >"${out_file}"

	if [[ -s "${out_file}" ]]; then
		echo "SUCCESS: Wrote $(wc -l <"${out_file}") requirement(s) to ${out_file_rel}"
	else
		: >"${out_file}"
		echo "SUCCESS: No requirements found. Created empty ${out_file_rel}"
	fi

	rm -f "${tmp_req}"
	rm -rf "${tmp_scan_dir}"
	echo
}

# --- 3. DEFINE THE 'build_lambda_zip' FUNCTION (FOR LAMBDA ZIPS) ---
build_lambda_zip() {
	local name="$1"
	local svc_dir="${SERVICES_DIR}/${name}"
	local req_file="${svc_dir}/requirements.txt"
	local build_path="${BUILD_DIR}/${name}"
	local out_zip="${DIST_DIR}/${name}.zip"

	local svc_dir_rel="services/${name}"
	local req_file_rel="${svc_dir_rel}/requirements.txt"
	local out_zip_rel=".dist/services/${name}.zip"
	local handler_file="${svc_dir}/${name}.py"

	if [[ ! -d "${svc_dir}" ]]; then
		echo "ERROR: Directory ${svc_dir_rel} not found."
		return 1
	fi
	if [[ ! -f "${handler_file}" ]]; then
		echo "ERROR: Handler file ${handler_file} not found."
		return 1
	fi

	echo "BUILDING: ${svc_dir_rel}"
	rm -rf "${build_path}"
	mkdir -p "${build_path}"

	# 1. Install dependencies from requirements.txt FIRST
	if [[ -f "${req_file}" && -s "${req_file}" ]]; then
		echo "INFO: Installing dependencies from ${req_file_rel} for Linux x86_64..."
		if ! ${PIP_BIN} -m pip install -q \
			-r "${req_file}" \
			-t "${build_path}" \
			--platform manylinux2014_x86_64 \
			--python-version 3.12 \
			--only-binary=:all:; then
			echo "ERROR: pip install failed for ${req_file_rel}"
			return 1
		fi
	else
		echo "INFO: No dependencies to install for ${svc_dir_rel}."
	fi

	# 2. Copy the handler .py file to the ROOT of the build path.
	echo "INFO: Copying handler to root: ${name}.py"
	cp "${handler_file}" "${build_path}/${name}.py"

	# 3. Create the 'services' package structure for shared modules.
	mkdir -p "${build_path}/services"
	touch "${build_path}/services/__init__.py"

	# 4. Find and copy ALL shared modules (any folder starting with __).
	#    This is the simple, reliable fix for transitive dependencies.
	echo "INFO: Copying all shared service modules (__*)..."
	find "${SERVICES_DIR}" -mindepth 1 -maxdepth 1 -type d -name "__*" -exec \
		rsync -a --exclude="__pycache__/" {} "${build_path}/services/" \;

	# 5. Zip the artifact
	(cd "${build_path}" && zip -X -r "${out_zip}" . >/dev/null)

	# 6. Report size
	local size
	size=$(du -h "${out_zip}" | awk '{print $1}')
	echo "SUCCESS: Created ${out_zip_rel} (Size: ${size})"
	echo # Blank line for readability
}
