import json
from pathlib import Path

import pandas as pd

from .config import PostRunSettings, PostSummarySettings
from .utils import output_dir_to_index, output_name_to_index


def _load_timeseries_json(p: Path) -> pd.DataFrame:
    """
    Load one <base_prefix>_timeseries.json and decorate with case/output metadata.
    json is already in seconds (duration_s).
    Layout:
      <post_base_path>/<case_name>/outputNNN/<base_prefix>_timeseries.json
    """
    with p.open() as f:
        df = pd.DataFrame(json.load(f))

    output_name = p.parent.name  # "output000"
    out_idx = output_name_to_index(output_name)
    if out_idx is None:
        raise ValueError(f"Unexpected output dir name: {output_name}")

    case_name = p.parent.parent.name  # "postprocessing_<something>"

    return df.assign(
        __case_name=case_name,
        __output_name=output_name,
        __output_index=out_idx,
        __src_path=str(p),
    )


def _slice_per_series_iloc(
    df: pd.DataFrame,
    group_cols: list[str],
    order_cols: list[str],
    start: int | None,
    end: int | None,
) -> pd.DataFrame:
    """
    Slice rows per group using iloc[start:end], after sorting each group by
    order_cols. Used to trim a fixed number of samples off each per-(case,
    output, component, pet) timeseries before computing stats (e.g. to skip
    spin-up).

    group_cols: columns identifying one independent series (rows sharing the
        same values across all of these are sorted and sliced together).
    order_cols: columns to sort each group by before slicing (e.g. "start").
    start, end: iloc slice bounds, same semantics as Python's slice(start,
        end) - e.g. start=1 keeps from the 2nd row onward, end=1 keeps only
        the 1st row. If both are None, the frame is returned unchanged
        (no slicing, no copy).

    Grouping uses dropna=False, matching every aggregation in
    _summarise_case. With pandas' default (dropna=True) a row with a missing
    group key - e.g. a null model_component - is dropped by the groupby, so
    merely setting stats_start_index would silently change which rows are
    counted rather than only how many samples each series contributes.

    Returns an empty frame (not an error) if df is empty, or if every group
    ends up empty after slicing.
    """
    if start is None and end is None:
        return df

    if df.empty:
        return df.copy()

    sl = slice(start, end)
    groups = []

    for _, g in df.groupby(group_cols, sort=False, dropna=False):
        g_sorted = g.sort_values(order_cols, kind="mergesort")
        groups.append(g_sorted.iloc[sl])

    if not groups:
        return df.iloc[0:0].copy()

    return pd.concat(groups, ignore_index=True)


def _collect_case_jsons(
    post_base_path: Path,
    case_name: str,
    output_index: list[int] | None,
    timeseries_suffix: str,
) -> list[Path]:
    """
    Collect all timeseries under output* for a single case.
    """
    case_dir = post_base_path / case_name
    if not case_dir.is_dir():
        print(f"-- warning: case dir not found: {case_dir}")
        return []

    outputs = [p for p in case_dir.glob("output*") if p.is_dir() and output_dir_to_index(p) is not None]
    outputs.sort(key=output_dir_to_index)

    if output_index is not None:
        allowed = {int(i) for i in output_index}
        outputs = [p for p in outputs if output_dir_to_index(p) in allowed]

    jsons = []
    for od in outputs:
        jsons.extend(od.glob(f"*{timeseries_suffix}"))
    return jsons


def _summarise_case(
    json_paths: list[Path],
    model_component: list[str] | None,
    pets: list[int] | None,
    stats_start_index: int | None,
    stats_end_index: int | None,
) -> pd.DataFrame:
    """
    Load, filter and summarise the timeseries JSONs for a single case.

    json_paths: *_timeseries.json files to load and pool (as produced by
        _collect_case_jsons for one case).
    model_component: keep only these component selector strings; None keeps
        all.
    pets: keep only these PET indices; None keeps all.
    stats_start_index, stats_end_index: per-series iloc slice applied (via
        _slice_per_series_iloc) before aggregating, so stats reflect only
        the selected sample range.

    Returns rows for:
      - each (case, outputNNN, model_component): stats over that output's
        own samples only.
      - each (case, model_component) with __output_name='combine': stats
        computed directly from the raw samples pooled across every selected
        output and PET for that component (not by averaging the per-output
        stats above - hits/ncpus/tstd in particular are not meaningfully
        combinable that way).
    NOTE: No case-level (component-agnostic) '<case>_combine' rows.

    Returns an empty frame with the expected columns if json_paths is empty
    or every row is filtered out.
    """
    output_cols = [
        "__row_label",
        "__case_name",
        "__output_name",
        "model_component",
        "ncpus",
        "hits",
        "tmin",
        "tmax",
        "tavg",
        "tmedian",
        "tstd",
        "pemin",
        "pemax",
    ]

    if not json_paths:
        return pd.DataFrame(columns=output_cols)

    # load timeseries from all jsons
    parts = [_load_timeseries_json(p) for p in json_paths]
    df = pd.concat(parts, ignore_index=True)

    ts = df
    if model_component is not None:
        sel = {s.strip() for s in model_component}
        ts = ts[ts["model_component"].astype(str).str.strip().isin(sel)]
    if pets is not None:
        allowed = {int(p) for p in pets}
        ts = ts[ts["pet"].isin(allowed)]

    # slice per (case, output, model_component, PET); slicer should skip empty groups
    series_keys = ["__case_name", "__output_name", "model_component", "pet"]
    ts = _slice_per_series_iloc(ts, series_keys, ["start"], stats_start_index, stats_end_index)
    if ts.empty:
        return pd.DataFrame(columns=output_cols)

    grp_out = ts.groupby(["__case_name", "__output_name", "model_component"], sort=False, dropna=False)
    per_output = grp_out.agg(
        hits=("duration_s", "count"),
        tmin=("duration_s", "min"),
        tmax=("duration_s", "max"),
        tavg=("duration_s", "mean"),
        tmedian=("duration_s", "median"),
        tstd=("duration_s", "std"),
    ).reset_index()

    ncpus_per_out = (
        ts.groupby(["__case_name", "__output_name", "model_component"], sort=False, dropna=False)
        .agg(
            ncpus=("pet", "nunique"),
            pemin=("pet", "min"),
            pemax=("pet", "max"),
        )
        .reset_index()
    )

    per_output = per_output.merge(
        ncpus_per_out,
        on=["__case_name", "__output_name", "model_component"],
        how="left",
        validate="one_to_one",
    )

    # labels and order
    per_output["__output_index"] = per_output["__output_name"].map(output_name_to_index)
    per_output["__row_label"] = (
        per_output["__case_name"]
        + "/"
        + per_output["__output_name"]
        + "/"
        + per_output["model_component"].astype(str).str.strip()
    )
    per_output = per_output.sort_values(["__case_name", "__output_index", "model_component"], kind="mergesort")

    grp_comp = ts.groupby(["__case_name", "model_component"], sort=False, dropna=False)
    combined_by_comp = grp_comp.agg(
        hits=("duration_s", "count"),
        tmin=("duration_s", "min"),
        tmax=("duration_s", "max"),
        tavg=("duration_s", "mean"),
        tmedian=("duration_s", "median"),
        tstd=("duration_s", "std"),
        ncpus=("pet", "nunique"),
        pemin=("pet", "min"),
        pemax=("pet", "max"),
    ).reset_index()

    combined_by_comp["__output_name"] = "combine"
    combined_by_comp["__row_label"] = (
        combined_by_comp["__case_name"] + "/combine/" + combined_by_comp["model_component"].astype(str).str.strip()
    )

    return pd.concat([per_output[output_cols], combined_by_comp[output_cols]], ignore_index=True)


def _resolve_save_json_path(save_json_path: str | Path | None) -> Path | None:
    """
    Validate a save path and ensure its parent directory exists.

    Returns None unchanged if save_json_path is None (meaning "don't save").
    Otherwise expands the path, requires a ".json" suffix (raising
    ValueError if it doesn't have one), creates the parent directory if
    needed, and returns the resolved Path.
    """
    if save_json_path is None:
        return None
    p = Path(save_json_path).expanduser()
    if p.suffix.lower() != ".json":
        raise ValueError(f"Invalid save_json_path: {p} — must explicitly end with '.json'!")
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


# Internal bookkeeping columns are '__'-prefixed; these are their public
# names. Everything written to disk or handed back to a caller goes through
# _to_public_table, so the per-run and combined outputs share one schema.
_PUBLIC_RENAMES = {
    "__row_label": "name",
    "__case_name": "case_name",
    "__output_name": "output_name",
}

PUBLIC_COLUMNS = [
    "name",
    "case_name",
    "output_name",
    "model_component",
    "ncpus",
    "hits",
    "tmin",
    "tmax",
    "tavg",
    "tmedian",
    "tstd",
    "pemin",
    "pemax",
]

# Printed to the terminal. Narrower than PUBLIC_COLUMNS because case_name,
# output_name and model_component are all repeated in the row label, and
# real ESMF component selectors are far too long to tabulate.
DISPLAY_COLUMNS = ["ncpus", "hits", "tmin", "tmax", "tavg", "tmedian", "tstd", "pemin", "pemax"]


def _to_public_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename the internal '__'-prefixed columns to their public names and order
    them as PUBLIC_COLUMNS.

    Both the per-run and the combined output are written from this, so the
    two files always share a schema; previously the per-run JSON exposed raw
    '__row_label'/'__case_name'/'__output_name' keys while the combined one
    used 'name' and dropped model_component entirely.
    """
    renamed = df.rename(columns=_PUBLIC_RENAMES)
    return renamed.loc[:, [c for c in PUBLIC_COLUMNS if c in renamed.columns]]


def _select_summary_rows(
    summary: pd.DataFrame,
    *,
    include_combined: bool,
    include_per_output: bool,
) -> pd.DataFrame:
    """
    Filter a _summarise_case() result down to the requested row kinds.

    summary: a frame with an "__output_name" column, where the pooled
        per-component row is labelled "combine" and all other values are
        per-output rows.
    include_per_output: keep rows where __output_name != "combine".
    include_combined: keep rows where __output_name == "combine".

    Raises ValueError if both flags are false, since that would select
    nothing.
    """
    if not include_combined and not include_per_output:
        raise ValueError("At least one of include_combined or include_per_output must be true")

    selected = []
    if include_per_output:
        selected.append(summary[summary["__output_name"] != "combine"])
    if include_combined:
        selected.append(summary[summary["__output_name"] == "combine"])
    return pd.concat(selected, ignore_index=True)


def post_summary_from_yaml(
    defaults: PostSummarySettings,
    runs: list[PostRunSettings],
    save_json_path: str | Path | None = None,
    include_combined: bool | None = None,
    include_per_output: bool | None = None,
) -> pd.DataFrame:
    """
    Summarise *_timeseries.json files for every run and build a combined
    table across all of them. This is the shared implementation behind both
    the `post-summary-from-yaml` CLI command and post_summary_from_config().

    defaults, runs: as produced by config.parse_post_summary_config() /
        config.load_post_summary_config().
    save_json_path: if given, overrides defaults.save_json_path as the
        destination for the combined summary. Writing it also writes a
        sibling "<stem>_table.parquet" of the cleaned, indexed table.
    include_combined, include_per_output: override defaults.include_combined
        / defaults.include_per_output for this call; None keeps the
        default's value. At least one must end up true.

    For each run: collects its timeseries JSONs, summarises them, filters to
    the requested row kinds, optionally writes that run's own selection to
    its `save_json_path`, then folds it into the combined table. Prints a
    narrowed view of the combined table (DISPLAY_COLUMNS) before returning
    the full one; per-run and combined output share the PUBLIC_COLUMNS
    schema.

    Raises ValueError if both include flags resolve to false, or if no run
    produced any rows (e.g. every case directory was missing or every row
    was filtered out) - not SystemExit, so this is safe to call as a library
    function as well as from the CLI.

    Returns the combined table as a DataFrame indexed by row label ("name"),
    carrying the remaining PUBLIC_COLUMNS.
    """
    post_base_path: Path = Path(defaults.post_base_path)
    timeseries_suffix: str = defaults.timeseries_suffix

    per_case_tables: list[pd.DataFrame] = []
    include_combined = defaults.include_combined if include_combined is None else include_combined
    include_per_output = defaults.include_per_output if include_per_output is None else include_per_output
    if not include_combined and not include_per_output:
        raise ValueError("At least one of include_combined or include_per_output must be true")

    for r in runs:
        case_name = r.name

        jsons = _collect_case_jsons(
            post_base_path=post_base_path,
            case_name=case_name,
            output_index=r.output_index,
            timeseries_suffix=timeseries_suffix,
        )

        case_summary = _summarise_case(
            json_paths=jsons,
            model_component=r.model_component,
            pets=r.pets,
            stats_start_index=r.stats_start_index,
            stats_end_index=r.stats_end_index,
        )

        if case_summary.empty:
            continue

        selected_case_summary = _select_summary_rows(
            case_summary,
            include_combined=include_combined,
            include_per_output=include_per_output,
        )

        # Save per-run json if this run specified a save path (strict .json)
        per_run_save = _resolve_save_json_path(r.save_json_path) if r.save_json_path is not None else None
        if per_run_save is not None:
            (
                _to_public_table(selected_case_summary)
                .reset_index(drop=True)  # ensure a clean row index
                .to_json(per_run_save, orient="records", indent=2)
            )
            print(f"-- saved per-run summary JSON: {per_run_save}")

        per_case_tables.append(selected_case_summary)

    if not per_case_tables:
        raise ValueError("No rows produced. Check config selections and filters.")

    # Build combined table across all selected runs
    combined_df = _to_public_table(pd.concat(per_case_tables, ignore_index=True))
    clean_df = combined_df.set_index("name")

    print("\n")
    print("-- Summary table:")
    print(clean_df.loc[:, [c for c in DISPLAY_COLUMNS if c in clean_df.columns]])

    combined_out = _resolve_save_json_path(save_json_path or defaults.save_json_path)

    if combined_out is not None:
        combined_df.to_json(combined_out, orient="records", indent=2)
        print("\n")
        print(f"-- saved combined summary json: {combined_out}")

        clean_parquet = combined_out.with_name(combined_out.stem + "_table.parquet")
        clean_df.to_parquet(clean_parquet, index=True)
        print(f"-- saved cleaned table parquet: {clean_parquet}")

    return clean_df
