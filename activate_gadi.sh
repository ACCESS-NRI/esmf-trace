#!/usr/bin/env bash

# Activate esmf-trace Gadi environment
# source activate_gadi.sh

if [[ "${BASH_SOURCE[0]}" == "${0}" ]]; then
    echo "Error: activate_gadi.sh must be sourced."
    echo "Use:"
    echo "  source activate_gadi.sh"
    exit 1
fi


if [[ ! -d ".venv" ]]; then
    echo "Error: .venv does not exist."
    echo "Run ./setup_gadi.sh first."
    return 1
fi

module use /g/data/vk83/modules
module load model-tools/babeltrace2/2.1.2

source .venv/bin/activate

echo "esmf-trace environment activated"

# Fail if the module's Python bindings are not visible in the virtual environment
python3 -c "import bt2"
