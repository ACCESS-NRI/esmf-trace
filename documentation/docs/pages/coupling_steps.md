# Coupling steps

`coupling_steps` measures load balance between the mediator (MED) and each component that runs concurrently with it,
one row per coupling step. It answers "who waits for whom, and for how long" from a `*_timeseries.json`.

CMEPS couples hub-and-spoke. A component X on PETs that do not run MED (the ocean in ACCESS-OM3) meets the MED side
only at its `[MED-TO-X]` and `[X-TO-MED]` connectors, once per coupling step:

```text
X PET:    [MED-TO-X] -> [X] ------------------------------> [MED-TO-X] (next step)
MED PET:  [MED-TO-X] -> work on the MED PETs -> [X-TO-MED] -> work up to the next [MED-TO-X]
```

Components that share the MED PETs (ATM, ICE and ROF in ACCESS-OM3) run in sequence with the mediator, so their time
counts as MED-side work.

## Columns

| Column | Meaning |
|---|---|
| `walltime_s` | Time from one `[X]` call to the next, averaged over the X PETs |
| `active_s` | Duration of the `[X]` run phase |
| `wait_s` | X idle in the next `[MED-TO-X]`, waiting for the MED side |
| `med_wait_s` | MED side idle in `[X-TO-MED]`, waiting for X |
| `med_concurrent_s` | MED-PET time between sending data to X and needing it back (overlaps X) |
| `med_serial_s` | MED-PET time after receiving X's data and before the next `[MED-TO-X]` (X always waits for this) |
| `<tag>_active_s` | Time of each component on the MED PETs in the step, e.g. `ice_active_s`, `med_active_s` |

Every value is a timestamp difference on a single PET, so clocks on different nodes are never compared. With several
PETs on a side, each step keeps the slowest PET: the smallest wait and the largest active, concurrent and serial time.
`walltime_s` is the exception. The PETs of X start each step together, up to jitter: a PET that starts one step late
starts the next one early. Their mean therefore adds up to the elapsed time, whereas the largest value per step would
add up to more.

## Input

The timeseries needs the regions directly under the driver's run phase, on at least one MED PET and one PET of each
component analysed. In the run config, keep every component and use a depth of at least 3, for example:

```yaml
default_settings:
  model_component: null
  max_depth: 3
runs:
  - exact_path: /path/to/archive
    base_prefix: case_a
    pets: "0,24"          # PET 0 runs MED; PET 24 is the first ocean PET
```

`pets` also accepts a step, e.g. `"0,24-311:48"` for PET 0 and every 48th PET from 24 to 311.

## Library usage

```python
from access.esmf_trace import coupling_steps_for_case, coupling_summary

steps = coupling_steps_for_case(
    "/scratch/tm70/user/postprocessing",
    "postprocessing_case_a",
    components="OCN",
    start_index=2,     # drop two warm-up steps
    end_index=-1,      # and the final output boundary
)
summary = coupling_summary(steps, coupling_timestep_seconds=3600, ncpus=288)
```

`coupling_steps(df, ...)` does the same for one timeseries frame. `components=None` analyses every component on PETs
apart from MED. `pets={"MED": 0, "OCN": "24-311:48"}` chooses PETs per component.

`coupling_summary` gives one row per component: the mean of each timing column as `<column>_mean_s` (e.g.
`wait_mean_s`, `ice_active_mean_s`), and

- `idle_on_med_s` / `idle_on_component_s`: mean seconds per step that one side sat idle because the other side's
  overlapping work was longer. This is the most that giving the slower side more PETs could save.
- `idle_on_med_steps` / `idle_on_component_steps`: the number of steps in which that happened.
- `seconds_per_sim_day` (needs `coupling_timestep_seconds`) and `core_hours_per_sim_year` (also needs `ncpus`, the
  whole job's allocation).
