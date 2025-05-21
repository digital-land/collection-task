#!/bin/bash

set -e

ENTRY_DATE=$(date +%Y-%m-%d)
echo "Build digital-land-builder for entry date: $ENTRY_DATE"

# run the script to run digital-land-builder
OUTPUT_DIR="/tmp/performance/digital-land-builder/entry-date=${ENTRY_DATE}"
mkdir -p "$OUTPUT_DIR"
# OUTPUT_FILE="${OUTPUT_DIR}/whatever we call the output"

TMP_DIR=$(mktemp -d) # Make folder for digital-land-builder files
echo "Using temporary directory: $TMP_DIR"
mkdir -p "${TMP_DIR}"/bin

mkdir -p dataset/
GITHUB_DIR="https://raw.githubusercontent.com/digital-land/digital-land-builder/refs/heads/main"

# A helper function to fetch and optionally chmod a file
fetch_script() {
    local file=$1
    local mode=${2:-}
    curl -qfsL "${GITHUB_DIR}/${file}" > "${TMP_DIR}/${file}"
    if [[ "$mode" == "exec" ]]; then
        chmod +x "${TMP_DIR}/${file}"
    fi
}

# Download scripts
fetch_script "bin/download-collection.sh" "exec"
fetch_script "bin/download-pipeline.sh" "exec"
fetch_script "bin/concat.sh" "exec"
fetch_script "bin/download_issues.py"
fetch_script "bin/download-operational-issues.sh" "exec"
fetch_script "bin/download_column_field.py"
fetch_script "bin/download_converted_resources.py"
fetch_script "bin/concat-issues.py" "exec"
fetch_script "bin/concat-column-field.py" "exec"
fetch_script "bin/concat-converted-resource.py" "exec"
fetch_script "bin/download_expectations.py"

### Run them
#"${TMP_DIR}"/bin/download-collection.sh
#"${TMP_DIR}"/bin/download-pipeline.sh
#"${TMP_DIR}"/bin/concat.sh
#python "${TMP_DIR}"/bin/download_issues.py
#"${TMP_DIR}"/bin/download-operational-issues.sh
#python "${TMP_DIR}"/bin/download_column_field.py
#python "${TMP_DIR}"/bin/download_converted_resources.py
#"${TMP_DIR}"/./bin/concat-issues.py
#"${TMP_DIR}"/./bin/concat-column-field.py
#"${TMP_DIR}"/./bin/concat-converted-resource.py
#python3 "${TMP_DIR}"/bin/download_expectations.py

echo "Digital Land build complete"