#!/bin/bash

set -e

ENTRY_DATE=$(date +%Y-%m-%d)

echo "Performance build for entry date: $ENTRY_DATE"

# run the script to generate the provision-quality dataset
echo "Running generate_provision_quality.py for entry date: $ENTRY_DATE"
python3 src/generate_provision_quality.py

echo "done"