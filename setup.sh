#!/usr/bin/env bash
set -euo pipefail

# Gadi setup for esmf-trace
module use /g/data/vk83/modules
module load model-tools/babeltrace2/2.1.2

# Create venv and install
python3 -m venv .venv
. .venv/bin/activate

pip install --upgrade pip setuptools wheel

pip install -r requirements.txt

# These are not merged yet, but we want to be able to install them in the meantime,
# because they are needed for the ARE work.
pip install -r requirements-access.txt

# Install and try to pull the "access" extras if available
pip install -e .
pip install -e ".[access]" || true
