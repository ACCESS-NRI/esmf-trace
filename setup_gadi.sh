#!/usr/bin/env bash
set -euo pipefail

# Gadi setup for esmf-trace
module use /g/data/vk83/modules
module load model-tools/babeltrace2/2.1.2

# Create venv and install
python3 -m venv .venv
. .venv/bin/activate

pip install --upgrade pip setuptools wheel

# Install the package in editable mode for development
pip install -e ".[devel]"

# Install the interactive tooling for the notebooks
pip install -e ".[interactive]"

# Workspace bundle: install other ACCESS repos into the same venv for workflow convenience
pip install -r requirements-access.txt
