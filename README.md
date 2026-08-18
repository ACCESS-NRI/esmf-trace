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

To set up a development and notebook environment on Gadi, run the repository setup script ([setup_gadi.sh](https://github.com/ACCESS-NRI/esmf-trace/blob/main/setup_gadi.sh)). The script loads the required modules, creates a `.venv` virtual environment, and installs the development, interactive and ACCESS workspace dependencies:

```bash
./setup_gadi.sh
```

One can verify that the bindings are available with,

```bash
python3 -c "import bt2"
```
