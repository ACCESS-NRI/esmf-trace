# Post-summary configuration

`esmf-trace` summarises many `*_timeseries.json` outputs (produced by `run-from-yaml`) into a single table of per-component timing statistics. This is driven by a **post-summary config**: a YAML file (or equivalent dict) with a `default_settings` block and a list of `runs`.

```bash
esmf-trace post-summary-from-yaml --config post_summary.yaml
```

## Minimal example

```yaml
default_settings:
  post_base_path: /scratch/tm70/user/postprocessing
  model_component: "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[OCN] RunPhase1"

runs:
  - name: postprocessing_case_a
  - name: postprocessing_case_b
    pets: "0,104"
```

Each run's `name` must match a subdirectory of `post_base_path` laid out as:

```
<post_base_path>/<name>/output000/<prefix>_timeseries.json
<post_base_path>/<name>/output001/<prefix>_timeseries.json
...
```

!!! warning Unrecognised keys
    Unrecognised keys are rejected rather than ignored, so a typo fails immediately with an error naming the offending key and listing the valid ones. Keys that belong to a *run* config (`stream_prefix`, `cmap`, ...) are rejected here too - they have no effect in a post-summary config, and accepting them would imply otherwise.

Post-summary reads the `*_timeseries.json` files that `run-from-yaml` already wrote. It can only select from what those files contain — `model_component` and `pets` are filters over existing rows, not a request to go back to the raw trace.

So if `run-from-yaml` was itself run with a restrictive `model_component` (or a shallow `max_depth`, or a short `pets` list), the components it left out are simply absent here, and
asking for them yields nothing. That case is now reported:

```
-- warning: 23 of 24 requested model_component(s) matched no rows in postprocessing_MC-100km-ryf_node_1_queue_normalsr_shared_13_ocn_91: [ESMF]/[ensemble] Init 1/[ESM0001] IPDv02p5/[MED] IPDv03p7, [ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[ATM] RunPhase1 (+21 more)
```

If you see this, widen the **run** config and re-run `run-from-yaml` - no post-summary setting can recover data that was never captured.

### Matching is exact

`model_component` values are compared for **equality** (after stripping surrounding
whitespace), not as substrings or patterns. The selector must be the complete path as it
appears in the timeseries JSON:

```yaml
# matches
model_component: '[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[OCN] RunPhase1'

# matches nothing - not a substring search
model_component: '[OCN] RunPhase1'
```

An abbreviated selector silently selects no rows, and if nothing else matches either, the run
fails with `No rows produced. Check config selections and filters.` The unmatched-component
warning above fires first and names what missed. To see the exact strings available, read the
`model_component` field of any `*_timeseries.json` under `post_base_path`.

## `default_settings` reference

| Key | Type | Default | Description |
|---|---|---|---|
| `post_base_path` | path | *required* | Root directory containing one subdirectory per case `name`. |
| `model_component` | str \| list[str] | all | Component selector string(s) to keep. Must be the **full** selector — see [matching is exact](#matching-is-exact). A comma-separated string is split into a list. |
| `pets` | str \| list[int] \| int | all | PET indices to keep. Accepts `"0,3-5,8"` (commas and ranges), `[0, 3, 4, 5, 8]`, or a bare `7`. Note the CLI's `--pets` is stricter — plain integers only. |
| `stats_start_index` / `stats_end_index` | int | none | `iloc[start:end]` slice applied per `(case, output, component, pet)` series before aggregating — see [which samples](#which-samples-the-statistics-use). |
| `timeseries_suffix` | str | `_timeseries.json` | Filename suffix used to find timeseries files under each `outputNNN/` directory. |
| `all_runs_summary_path` | path | none | Where to write the summary table spanning **every run**. Must end in `.json`; also writes a sibling `<stem>_table.parquet`. |
| `include_combined` | bool | `true` | Include the pooled-across-outputs `combine` row per `(case, component)`. |
| `include_per_output` | bool | `true` | Include one row per `(case, output, component)`. |

Setting **both** `include_combined` and `include_per_output` to `false` selects nothing, and is
rejected — as `ConfigError` when it comes from a config, `ValueError` when passed directly to
`post_summary_from_config`.

## Per-run fields

Each entry in `runs` accepts:

| Key | Description |
|---|---|
| `name` | *(required)* Case name — subdirectory under `post_base_path`. |
| `output_index` | Which `outputNNN` directories to include, e.g. `"0,2-4"` or `[0, 2, 3, 4]`. Default: all. |
| `pets`, `model_component`, `stats_start_index`, `stats_end_index` | Same meaning as in `default_settings`; a run that sets its own value uses that instead of the default (see [override precedence](#override-precedence)). |
| `summary_path` | Write **this run's own** summary rows to a separate `.json` file. |

!!! warning "The two destinations are deliberately different keys"
    `default_settings.all_runs_summary_path` and a run's `summary_path` are separate settings,
    not a default and its override. A run gets its own file only if it declares `summary_path`
    itself; there is nothing to inherit.

## Override precedence

Values can come from four places, in this order of priority:

1. **Explicit `post_summary_from_config` parameters** — `include_combined`,
   `include_per_output` and `all_runs_summary_path`.
2. **CLI flags / library `post_overrides`** — apply to *every* run, even one that sets its own
   value for that field.
3. **The run's own value** in its `runs[]` entry.
4. **`default_settings`**, used when none of the above set a value.

```bash
# overrides stats_start_index for every run, regardless of what's in the YAML
esmf-trace post-summary-from-yaml --config post_summary.yaml --stats-start-index 2
```

## Two kinds of pooling

Rows get pooled on two independent axes, and only the first is called "combined":

| | Pools | Controlled by | Produces |
|---|---|---|---|
| **Within a case** | that case's `outputNNN` directories | `include_combined` | a `combine` row per `(case, component)` |
| **Across cases** | whole runs | always — every listed run | the table written to `all_runs_summary_path` |

So `include_combined` has nothing to do with `all_runs_summary_path`. Turning it off still
summarises every run; it only drops the per-case `combine` rows.

Given two outputs for one case/component, the summary normally produces three rows:

```
case_a/output000/<component>
case_a/output001/<component>
case_a/combine/<component>     <- pooled across every selected output & PET
```

(`<component>` stands in for the full selector, which is too long to show inline.)

- `--include-per-output --no-include-combined` -> drop the `combine` rows, keep only per-output.
- `--no-include-per-output --include-combined` -> keep only the pooled `combine` rows.

!!! tip "One output per case? Turn `include_combined` off"
    With a single `outputNNN` directory there is nothing to pool, so the `combine` row is an
    exact duplicate of the per-output row. Setting `include_combined: false` halves the output.

## Which samples the statistics use

Every statistic is computed from one raw quantity: **`duration_s`**, where each record in a timeseries JSON is **one call** of that component on that PET (a single enter -> exit span).

Samples reach the aggregation in three steps:

1. **Filter** — drop rows outside `model_component` and `pets`.
2. **Slice** — within each `(case, output, component, pet)` series, sort by `start` and take
   `iloc[stats_start_index:stats_end_index]`. This is applied **per series**, so
   `stats_start_index: 2` drops each PET's own first 2 calls, not the first 2 rows overall.
3. **Aggregate** — pool whatever survives and compute the columns below.

`model_component` is a grouping key throughout, so **components are never pooled together**. Each component gets its own rows and its own sample pool, and it does not matter that different components have different call counts - a component called 6 times per output and one called 2000 times per output simply produce separate rows.

What each pool contains:

| Row | Pool |
|---|---|
| `per-output` | one `(case, output, component)`, across the selected PETs |
| `combine` | one `(case, component)`, across the selected outputs **and** PETs |

The `combine` row is recomputed from those raw samples, never derived from the per-output statistics - averaging those would be wrong for `hits`, `tstd` and `ncpus` in particular.

### Pooling is weighted by call count

Within a single component's pool, every **call** carries equal weight — not every output, and
not every PET. If a component's series all have the same length, this makes no difference and
you can ignore it. It only matters when they don't, for example when restart segments cover
different spans:

```
output000: 6 calls x 1.0 s
output001: 2 calls x 10.0 s

combine tavg = mean([1,1,1,1,1,1,10,10]) = 3.25     <- what you get
               mean([1.0, 10.0])         = 5.5      <- NOT this (mean of per-output means)
```

`tmedian` shifts the same way: 6 of those 8 samples are `1.0`, so the median is `1.0` and the slower segment barely registers. The same applies across PETs if one PET contributes fewer calls than another for the same component.

## Output columns

| Column | Meaning |
|---|---|
| `name` | Row label: `case_name/output_name/model_component`. Also the DataFrame index. |
| `case_name` | The run's `name`. |
| `output_name` | `outputNNN`, or `combine` for a pooled row. |
| `model_component` | The ESMF component selector. |
| `ncpus` | Number of **distinct PETs that contributed samples** to this row — see the warning below. |
| `hits` | Number of samples (calls) pooled into this row. |
| `tmin` / `tmax` | Smallest / largest single `duration_s` sample. |
| `tavg` / `tmedian` | Mean / median over the pooled samples. |
| `tstd` | Sample standard deviation (`ddof=1`). |
| `pemin` / `pemax` | Smallest / largest **PET index** present (not a duration). |

!!! warning "`ncpus` is not the run's core count"
    It counts distinct PETs *after* the `pets` filter. A run on 104 cores summarised with
    `pets: 0,13` reports `ncpus` of 1 or 2, because only those PETs were sampled. For scaling
    work, take the core count from the run's layout, not from this column.

`name`, `case_name`, `output_name` and `model_component` are omitted from the **printed** table only — real ESMF selectors are far too long to tabulate, and they are already in the row label. Both saved files carry the full set.

## CLI reference

```bash
esmf-trace post-summary-from-yaml --config post_summary.yaml \
  --model-component \
      "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[OCN] RunPhase1" \
      "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[ICE] RunPhase1" \
  --pets 0 104 \
  --stats-start-index 2 --stats-end-index -1 \
  --timeseries-suffix _timeseries.json \
  --all-runs-summary-path all_runs.json \
  --include-combined --no-include-per-output
```

| Flag | Notes |
|---|---|
| `--model-component NAME [NAME ...]` | Full component selector string(s). |
| `--pets N [N ...]` | Plain integers, space separated: `--pets 0 13`. Each token is parsed as an `int`, so **any** comma or range form is rejected — `0,13`, `3-5` and `0,3-5` all fail with `invalid int value`. The comma/range syntax is YAML-only ([default_settings reference](#default_settings-reference)). |
| `--stats-start-index`, `--stats-end-index` | Integers, same slicing semantics as the YAML fields. |
| `--timeseries-suffix` | Overrides the filename suffix to match. |
| `--all-runs-summary-path PATH` | Must end in `.json`. Writes a sibling `parquet` too. |
| `--include-combined` / `--no-include-combined` | Boolean toggle (`argparse.BooleanOptionalAction`); omit to leave the config's value unchanged. |
| `--include-per-output` / `--no-include-per-output` | Boolean toggle (`argparse.BooleanOptionalAction`); omit to leave the config's value unchanged. |

## Library usage

```python
from access.esmf_trace import post_summary_from_config

df = post_summary_from_config(
    "post_summary.yaml",
    post_overrides={"stats_start_index": 2},
    all_runs_summary_path="all_runs.json",
)
```

`df` is a `pandas.DataFrame` indexed by `name`, carrying the remaining [output columns](#output-columns). Both saved files — the per-run `summary_path` and the `all_runs_summary_path` — use that same schema.

Filter pooled rows with `output_name == "combine"` rather than splitting `name`: component selectors contain `/` themselves, so the label does not split cleanly.

A dict with the same `{"default_settings": ..., "runs": [...]}` shape works in place of a YAML path — handy for building configs programmatically, e.g. via `ACCESSPostSummaryConfigBuilder`:

```python
from access.esmf_trace import ACCESSPostSummaryConfigBuilder

builder = ACCESSPostSummaryConfigBuilder(
    post_base_path="/scratch/tm70/user/postprocessing",
    model_component=[
        "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[OCN] RunPhase1",
        "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[ICE] RunPhase1",
    ],
    include_combined=True,
    include_per_output=False,
)
config = builder.build_config([{"name": "postprocessing_case_a"}])
df = post_summary_from_config(config)
```

Unknown keys in `post_overrides` raise `ConfigError` rather than being ignored, so a typo like
`{"include_combine": False}` fails instead of silently leaving the flag at its default.
