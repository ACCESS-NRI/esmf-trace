[![CI](https://github.com/ACCESS-NRI/esmf-trace/actions/workflows/ci.yml/badge.svg)](https://github.com/ACCESS-NRI/esmf-trace/actions/workflows/ci.yml)
[![CD](https://github.com/ACCESS-NRI/esmf-trace/actions/workflows/CD.yml/badge.svg)](https://github.com/ACCESS-NRI/esmf-trace/actions/workflows/CD.yml)
[![Check links](https://github.com/ACCESS-NRI/esmf-trace/actions/workflows/check_links.yml/badge.svg)](https://github.com/ACCESS-NRI/esmf-trace/actions/workflows/check_links.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue?style=flat-square)](https://opensource.org/license/apache-2-0)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

# esmf-trace

## About
**esmf-trace** is a lightweight tool for extracting and visualising runtime profiling data from the
[ESMF NUOPC coupler](https://earthsystemmodeling.org/). 

The goal of **esmf-trace** is to make this profiling data easier to use by:
 - Extracting raw timing information from ESMF traces
 - Saving the results in a clean, user-friendly format
 - Providing optional interactive visualisations for quick exploration

This helps developers and researchers identify performance bottlenecks and optimise ACCESS model workflows.

## Background
Some ACCESS model configurations (such as *ACCESS-OM3*) rely on the ESMF NUOPC coupler to connect different model components (e.g. *MOM6*, *CICE6*, *WW3*).

ESMF includes a built-in profiling system that automatically collects timing statistics for all model phases. For ACCESS models under a Payu workflow, this can be enabled by setting the environment variable in `config.yaml`:

```bash
env:
  ESMF_RUNTIME_PROFILE: "on"
  ESMF_RUNTIME_TRACE: "on"
  ESMF_RUNTIME_PROFILE_OUTPUT: "BINARY"
```

timing data for every ESMF component invoked during a coupled model run is recorded.

## Installation and dependencies

**esmf-trace** requires Python 3.10 or newer. Its Python package dependencies are declared in [`pyproject.toml`](https://github.com/ACCESS-NRI/esmf-trace/blob/main/pyproject.toml) and are installed automatically by `pip`,

```bash
python3 -m pip install .
```

### [Babeltrace 2](https://github.com/efficios/babeltrace)

Reading CTF (Common Trace Format) traces also requires the Babeltrace 2 Python bindings, which are imported as `bt2`. Babeltrace 2 is an external dependency rather than a PyPI dependency, so it is intentionally not listed in `pyproject.toml`.

On Gadi, load the ACCESS-NRI module before using **esmf-trace**:

```bash
module use /g/data/vk83/modules
module load model-tools/babeltrace2/2.1.2
```

### Gadi development environment
To set up a development and notebook environment on Gadi, run the repository setup script ([setup_gadi.sh](https://github.com/ACCESS-NRI/esmf-trace/blob/main/setup_gadi.sh)).

```bash
./setup_gadi.sh
```

The setup script:
  - loads the required Babeltrace 2 module,
  - creates a Python virtual environment (`.venv`),
  - installs `esmf-trace` with development and interactive dependencies,
  - installs additional ACCESS workspace dependencies.

After setup, the environment needs to be activated in each new Gadi session:

```bash
source activate_gadi.sh
```

The activation script loads the required Babeltrace 2 module and activates the Python virtual environment.
