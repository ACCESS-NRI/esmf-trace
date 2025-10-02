import re
import pandas as pd


def prepare_view(
    df: pd.DataFrame,
    model_component: list[str] | None = None,
    pets: int | list[int] | None = None,
    columns: list[str]= ["start", "model_component", "pet", "duration_s"],
    sort_by: list[str] = ["pet"],
) -> pd.DataFrame:
    """
    Prepare a DataFrame view.
    """
    out = df.copy()

    out["duration_s"] = out["duration_s"] / 1e9 # seconds

    if model_component is not None:
        out = out[out["model_component"].isin(set(model_component))]

    if pets is not None:
        out = out[out["pet"].isin(set(pets))]

    cols = [c for c in columns if c in out.columns]
    out = out.loc[:, cols].sort_values(list(sort_by)).reset_index(drop=True)
    return out

def _slice_per_group(
    df: pd.DataFrame,
    start: int | None,
    end: int | None,
    group_cols: list[str],
    order_cols: list[str]= ["start"],
) -> pd.DataFrame:
    """
    Slice rows per group using iloc[start:end].
    If both start and end are None, return df unchanged.
    """
    if start is None and end is None:
        return df
    sl = slice(start, end)
    return (
        df.groupby(list(group_cols), group_keys=False, sort=False)
          .apply(lambda g: g.sort_values(order_cols, kind="mergesort").iloc[sl])
          .reset_index(drop=True)
    )

def timeseries_component(
    df: pd.DataFrame,
    model_component: list[str] | None = None,
    pets: int | list[int] | None = None,
    stats_start_index: int | None = None,
    stats_end_index: int | None = None,
) -> pd.DataFrame:
    """
    - `model_component`: the full name path of the model component(s).
    - `pets`: a pet index or list (e.g., 0, [0,1,2])
    """
    out = prepare_view(
        df=df,
        model_component=model_component,
        pets=pets,
    )

    return _slice_per_group(out, stats_start_index, stats_end_index, ["model_component", "pet"])

def stats_by_component(
    df: pd.DataFrame,
    model_component: list[str] | None = None,
    pets: int | list[int] | None = None,
    stats_start_index: int | None = None,
    stats_end_index: int | None = None,
) -> pd.DataFrame:

    base = prepare_view(
        df=df,
        model_component=model_component,
        pets=pets,
    )

    base = _slice_per_group(base, stats_start_index, stats_end_index, ["model_component", "pet"])

    # aggregate
    g = base.groupby("model_component")["duration_s"]
    out = g.agg(
        count="count",
        total_s="sum",
        mean_s="mean",
        max_s="max",
        min_s="min",
        std_s="std",
        p25_s=lambda x: x.quantile(0.25),
        p50_s=lambda x: x.quantile(0.50),
        p75_s=lambda x: x.quantile(0.75),
        # p90_s=lambda x: x.quantile(0.90),
        # p95_s=lambda x: x.quantile(0.95),
    ).reset_index()

    # per-PET metrics
    n_pets = base.groupby("model_component")["pet"].nunique().reset_index(name="n_pets")
    out = out.merge(n_pets, on="model_component", how="left")
    out["count_per_pet"] = out["count"] / out["n_pets"]
    out["total_s_per_pet"] = out["total_s"] / out["n_pets"]

    # add slice info
    out["stats_start_index"] = stats_start_index
    out["stats_end_index"] = stats_end_index

    # add other metrics
    # out["range_s"] = out["max_s"] - out["min_s"] # not very useful
    out["iqr_s"]   = out["p75_s"] - out["p25_s"] # inter-quartile range
    out["coff_var"] = out["std_s"] / out["mean_s"] # coefficient of variation
    out["max_over_mean"] = out["max_s"] / out["mean_s"] # check for outliers
    out["se_mean"] = out["std_s"] / out["count_per_pet"].pow(0.5) # standard error of the mean
    out["ci95_low"] = out["mean_s"] - 1.96 * out["se_mean"] # 95% confidence interval low end
    out["ci95_high"] = out["mean_s"] + 1.96 * out["se_mean"] # 95% confidence interval high end

    out = out.drop(columns=["count", "total_s", "p25_s", "p75_s"])

    return out.sort_values("p50_s", ascending=False).reset_index(drop=True)

def stats_by_component_pet(
    df: pd.DataFrame,
    model_component: list[str] | None = None,
    pets: int | list[int] | None = None,
    stats_start_index: int | None = None,
    stats_end_index: int | None = None,
) -> pd.DataFrame:
    base = prepare_view(
        df=df,
        model_component=model_component,
        pets=pets,
    )

    base = _slice_per_group(base, stats_start_index, stats_end_index, ["model_component", "pet"])

    g = base.groupby(["model_component", "pet"])["duration_s"]
    return g.agg(
        count="count",
        total_s="sum",
        mean_s="mean",
        min_s="min",
        max_s="max",
        std_s="std",
        p50_s=lambda x: x.quantile(0.50),
        # p90_s=lambda x: x.quantile(0.90),
        # p95_s=lambda x: x.quantile(0.95),
    ).reset_index()
