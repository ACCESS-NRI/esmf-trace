# Running esmf-trace in Jupyter Notebooks on Gadi

Profiling and optimisation workflows read ESMF traces through the Babeltrace 2 Python bindings (`bt2`), which is both CPU- and memory-hungry. Run these notebooks in [ARE](https://are.nci.org.au/) JupyterLab, which executes on a Gadi compute node.

!!! warning "Do not profile on a login node"
    Gadi login nodes are a shared resource, reserved for lightweight interactive work such as editing, quick checks and small debugging runs. Reading a full ESMF trace there will hit the login-node limits and slow the node down for everyone.

## Before you start

- An `esmf-trace` virtual environment built on Gadi with [`setup_gadi.sh`](https://github.com/ACCESS-NRI/esmf-trace/blob/main/setup_gadi.sh). Note its path, e.g. `/g/data/<project>/<user>/esmf-trace/.venv`.
- Membership of every project the session needs: `vk83` (for the Babeltrace 2 module), the project holding the virtual environment, and the project holding the traces.

## Step 1: Start an ARE JupyterLab session

Log in to [ARE](https://are.nci.org.au/), open **JupyterLab**, and set:

| Field | Value |
| ----- | ----- |
| **Walltime (hours)** | long enough for the analysis, e.g. `2` |
| **Queue** | `normal` |
| **Compute Size** | start with `Small`, and increase if a large trace exhausts memory |
| **Project** | the project the session is charged to |
| **Storage** | every filesystem the session touches, joined by `+`, e.g. `gdata/vk83+gdata/<project>+scratch/<project>` |

`gdata/vk83` is required: without it the Babeltrace 2 module cannot be loaded.

## Step 2: Load Babeltrace 2 and the virtual environment

Expand **Advanced options** and fill in three fields:

| Field | Value |
| ----- | ----- |
| **Module directories** | `/g/data/vk83/modules` |
| **Modules** | `model-tools/babeltrace2/2.1.2` |
| **Python or Conda virtual environment base** | `/path/to/esmf-trace/.venv` |

![ARE-module-selection](/assets/ARE-module-selection.png){: loading="lazy" }

Together these three fields are the ARE equivalent of what [`activate_gadi.sh`](https://github.com/ACCESS-NRI/esmf-trace/blob/main/activate_gadi.sh) does in a terminal:

```bash
module use /g/data/vk83/modules
module load model-tools/babeltrace2/2.1.2
source /path/to/esmf-trace/.venv/bin/activate
```

## Step 3: Verify the session

Launch the session, open a notebook with the default `Python 3` kernel, and run:

```python
import sys, bt2
print(sys.executable)
print(bt2.__file__)
```

The setup is correct when the paths point at your virtual environment and at the module's `bt2` similar to below:

```
/path/to/esmf-trace/.venv/bin/python

/g/data/vk83/apps/spack/1.1/release/linux-x86_64_v3/babeltrace2-2.1.2-ddk2nojzixku5pf45trntc53iahswh3n/lib/python3.11/site-packages/bt2/__init__.py

```

If the check fails:

- `ModuleNotFoundError: No module named 'bt2'` - `gdata/vk83` is missing from **Storage**, or **Module directories** / **Modules** are not set as above.
- `sys.executable` is not your virtual environment - **Python or Conda virtual environment base** must be the environment directory itself (`.../.venv`), not `.../.venv/bin/activate`.

??? warning "Appendix: VS Code Jupyter kernel on a login node (not recommended)"

    This custom kernel was a workaround for a Babeltrace 2 issue in ARE Jupyter sessions (see [model-tools#20](https://github.com/ACCESS-NRI/model-tools/pull/20#issue-3915759209)), fixed in [model-tools#24](https://github.com/ACCESS-NRI/model-tools/pull/24). ARE is now the supported path. Use this only when working on a login node is unavoidable, and never for computationally intensive analysis.

    **1. Create the kernel directory**

    ```bash
    mkdir -p ~/.local/share/jupyter/kernels/esmf-trace-bt2
    ```

    **2. Write `kernel.json`**, replacing `/path/to/venv` with your virtual environment. It loads the Babeltrace 2 module and activates the environment before the kernel starts.

    ```bash
    cat > ~/.local/share/jupyter/kernels/esmf-trace-bt2/kernel.json <<'JSON'
    {
      "argv": [
        "bash",
        "-lc",
        "module use /g/data/vk83/modules && module load model-tools/babeltrace2/2.1.2 && source /path/to/venv/bin/activate && exec python -m ipykernel_launcher -f {connection_file}"
      ],
      "display_name": "Python (esmf-trace + bt2)",
      "language": "python"
    }
    JSON
    ```

    **3. Reload VS Code** so it discovers the kernel: Command Palette (`Cmd/Ctrl + Shift + P`) -> `Developer: Reload Window`.

    **4. Select the kernel** in your notebook: kernel picker (top right) -> `Select Another Kernel` -> `Jupyter Kernel...` -> `Python (esmf-trace + bt2)`.

    ![vscode select_another_kernel](/assets/select_another_kernel.png){: loading="lazy" }

    **5. Verify** with the same `import bt2` check as [Step 3](#step-3-verify-the-session).
