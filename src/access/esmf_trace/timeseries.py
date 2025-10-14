import pandas as pd
from .common_vars import seconds_to_nanoseconds


def timeseries_component(
    df: pd.DataFrame,
    model_component: list[str] | None = None,
    pets: int | list[int] | None = None,
    columns: list[str] | None = None,
    sort_by: list[str] | None = None
) -> pd.DataFrame:
    """
    Return a tidy timeseries view with duration from nanoseconds to seconds.
    """
    required = {"start", "model_component", "pet", "duration_s"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Input DataFrame missing required column(s): {sorted(missing)}")

    out = df.copy()

    out["duration_s"] = out["duration_s"] / seconds_to_nanoseconds # seconds

    if model_component is not None:
        out = out[out["model_component"].isin(set(model_component))]

    if pets is not None:
        out = out[out["pet"].isin(set(pets))]

    if columns is None:
        columns = ("start", "model_component", "pet", "duration_s")

    if sort_by is None:
        default_sort = [c for c in ("pet",) if c in out.columns]
        sort_by = default_sort if default_sort else list(out.columns)

    out = out.sort_values(list(sort_by)).reset_index(drop=True)
    return out
