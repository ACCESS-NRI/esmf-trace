import argparse
from dataclasses import replace
import json
from pathlib import Path

import pandas as pd

from .config import PostSummarySettings, PostRunSettings
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
    Slice rows per (group) using iloc[start:end] in each group after sorting by order_cols
    If both start and end are None -> no slicing (full series).
    If end is None -> no slicing (full series).
    """
    if start is None and end is None:
        return df

    sl = slice(start, end)
    groups = []

    for _, g in df.groupby(group_cols, sort=False):
        g_sorted = g.sort_values(order_cols, kind="mergesort")
        groups.append(g_sorted.iloc[sl])

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
    Summarise multiple output directories belonging to a single case.

    Returns rows for:
      - each (case, outputNNN, model_component),
      - each (case, model_component) with __output_name='combine' (per-component combine)
    NOTE: No case-level (component-agnostic) '<case>_combine' rows.
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
        tmedian=("duration_s", lambda x: x.quantile(0.50)),
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

    combined_by_comp = (
        per_output.groupby(["__case_name", "model_component"], sort=False, dropna=False)
        .agg(
            hits=("hits", "mean"),
            tmin=("tmin", "min"),
            tmax=("tmax", "max"),
            tavg=("tavg", "mean"),
            tmedian=("tmedian", "mean"),
            tstd=("tstd", "mean"),
            pemin=("pemin", "min"),
            pemax=("pemax", "max"),
            ncpus=("ncpus", "mean"),
        )
        .reset_index()
    )
    combined_by_comp["__output_name"] = "combine"
    combined_by_comp["__row_label"] = (
        combined_by_comp["__case_name"] + "/combine/" + combined_by_comp["model_component"].astype(str).str.strip()
    )

    return pd.concat([per_output[output_cols], combined_by_comp[output_cols]], ignore_index=True)


def _resolve_save_json_path(save_json_path: str | None) -> Path | None:
    if save_json_path is None:
        return None
    p = Path(save_json_path).expanduser()
    if p.suffix.lower() != ".json":
        raise ValueError(f"Invalid save_json_path: {p} — must explicitly end with '.json'!")
    p.parent.mkdir(parents=True, exist_ok=True)
    return p


def post_summary_from_yaml(
    defaults: PostSummarySettings,
    runs: list[PostRunSettings],
    save_json_path: str | None = None,
) -> pd.DataFrame:
    post_base_path: Path = defaults.post_base_path
    timeseries_suffix: str = defaults.timeseries_suffix

    per_case_tables: list[pd.DataFrame] = []

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

        # Save per-run json if this run specified a save path (strict .json)
        per_run_save = _resolve_save_json_path(r.save_json_path)
        if per_run_save is not None:
            (
                case_summary.reset_index(drop=True).to_json(  # ensure a clean row index
                    per_run_save, orient="records", indent=2
                )
            )
            print(f"-- saved per-run summary JSON: {per_run_save}")

        per_case_tables.append(case_summary)

    if not per_case_tables:
        raise SystemExit("No rows produced. Check YAML selections and filters.")

    # Build combined table across all selected runs
    combined_df = pd.concat(per_case_tables, ignore_index=True)

    wanted_cols = ["__row_label", "hits", "tmin", "tmax", "tavg", "tmedian", "tstd", "pemin", "pemax"]
    combined_df = combined_df.loc[:, [c for c in wanted_cols if c in combined_df.columns]]

    clean_df = combined_df.rename(columns={"__row_label": "name"}).set_index("name")

    print("\n")
    print("-- Summary table:")
    print(clean_df)

    combined_out = _resolve_save_json_path(save_json_path or defaults.save_json_path)

    if combined_out is not None:
        (combined_df.rename(columns={"__row_label": "name"}).to_json(combined_out, orient="records", indent=2))
        print("\n")
        print(f"-- saved combined summary json: {combined_out}")

        clean_parquet = combined_out.with_name(combined_out.stem + "_table.parquet")
        clean_df.to_parquet(clean_parquet, index=True)
        print(f"-- saved cleaned table parquet: {clean_parquet}")


# def run_post_summary_from_yaml(ns: argparse.Namespace) -> None:
#     """
#     cli entrypoint for post-summary config from yaml and run the summary.
#     """
#     defaults, runs = load_yaml_config(Path(ns.config), kind="post-summary")
#     assert isinstance(defaults, PostSummarySettings)

#     # apply overrides to defaults (if any)
#     overrides = {}
#     if getattr(ns, "model_component", None) is not None:
#         overrides["model_component"] = ns.model_component  # list[str]
#     if getattr(ns, "pets", None) is not None:
#         overrides["pets"] = ns.pets  # list[int]
#     if getattr(ns, "stats_start_index", None) is not None:
#         overrides["stats_start_index"] = ns.stats_start_index  # int
#     if getattr(ns, "stats_end_index", None) is not None:
#         overrides["stats_end_index"] = ns.stats_end_index  # int
#     if getattr(ns, "timeseries_suffix", None) is not None:
#         overrides["timeseries_suffix"] = ns.timeseries_suffix  # str
#     if getattr(ns, "save_json_path", None) is not None:
#         overrides["save_json_path"] = ns.save_json_path  # str

#     if overrides:
#         defaults = replace(defaults, **overrides)

#     post_summary_from_yaml(defaults, runs, save_json_path=ns.save_json_path)
