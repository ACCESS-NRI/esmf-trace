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

## Dependencies
The tool currently depends on:
 - [babeltrace2](https://babeltrace.org/) for reading and writing CTF (Common Trace Format) traces.
 - [plotly](https://github.com/plotly/plotly.py) for generating interactive plots.