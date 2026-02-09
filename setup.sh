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

pip install -r requirements-access.txt

pip install -e .
