#!/usr/bin/env bash
set -euo pipefail

# Gadi setup for esmf-trace
module use /g/data/vk83/modules
module load model-tools/babeltrace2/2.1.2

# Create venv and install
python3 -m venv .venv
. .venv/bin/activate

python3 -m pip install --upgrade pip setuptools wheel

# Install the package and development/notebook tooling
python3 -m pip install -e ".[devel, interactive]"

# Workspace bundle: install other ACCESS repos into the same venv for workflow convenience
python3 -m pip install -r requirements-access.txt

# Fail if the module's Python bindings are not visible in the virtual environment
python3 -c "import bt2"
