from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from .common_vars import seconds_to_nanoseconds
from .trace_explorer import annotate_selector_columns, write_trace_explorer_html


def _prepare_flame_data(
    df: pd.DataFrame,
    pets: int | list[int] | None,
    xaxis_datetime: bool,
) -> tuple[pd.DataFrame, list[int]]:
    if "pet" not in df.columns:
        df = df.assign(pet=0)

    if pets is None:
        selected_pets = sorted(int(pet) for pet in df["pet"].unique())
    elif isinstance(pets, int):
        selected_pets = [pets]
    else:
        selected_pets = [int(pet) for pet in pets]

    out = df[df["pet"].isin(selected_pets)].copy()
    if out.empty:
        raise ValueError("no data for the selected pets")

    out = annotate_selector_columns(out)
    # Derive the displayed duration from the recorded timestamps so the
    # hover values always satisfy: duration = end - start.
    out["duration_seconds"] = (out["end"] - out["start"]) / seconds_to_nanoseconds

    if xaxis_datetime:
        out["x_start"] = pd.to_datetime(out["start"], unit="ns")
        out["x_end"] = pd.to_datetime(out["end"], unit="ns")
        # Plotly date-axis bar widths are milliseconds.
        out["width"] = (out["end"] - out["start"]) / 1_000_000
    else:
        origin_ns = out["start"].min()
        out["x_start"] = (out["start"] - origin_ns) / seconds_to_nanoseconds
        out["x_end"] = (out["end"] - origin_ns) / seconds_to_nanoseconds
        out["width"] = out["duration_seconds"]

    out["y_cat"] = [f"depth{int(depth)}_pet_{int(pet)}" for depth, pet in zip(out["depth"], out["pet"], strict=True)]
    return out, selected_pets


def plot_flame_graph(
    df: pd.DataFrame,
    pets: int | list[int] | None = None,
    xaxis_datetime: bool = False,
    html_path: Path | None = None,
):
    """
    Interactive flame graph for one or more pets.
    """
    plot_df, _ = _prepare_flame_data(df, pets, xaxis_datetime)

    fig = go.Figure()
    grouped = plot_df.groupby(["pet", "model_component", "depth"], sort=False)
    for (pet, path, depth), group in grouped:
        row = group.iloc[0]
        customdata = [
            [x_end, float(duration)] for x_end, duration in zip(group["x_end"], group["duration_seconds"], strict=True)
        ]
        if xaxis_datetime:
            timing_hover = "Start %{base}<br>End %{customdata[0]}<br>"
        else:
            timing_hover = "Start %{base:.6f} s<br>End %{customdata[0]:.6f} s<br>"

        meta = {
            "leaf": row["phase_leaf"],
            "group": row["phase_group"],
            "component": row["phase_component"],
            "label": row["phase_label"],
            "depth": int(depth),
            "pet": int(pet),
            "points": int(len(group)),
            "path": path,
        }

        # Keep one Plotly trace per logical region. The full path is stored once
        # in trace metadata / hovertemplate, not repeated once per timing span.
        fig.add_trace(
            go.Bar(
                y=group["y_cat"],
                x=group["width"],
                base=group["x_start"],
                orientation="h",
                name=path,
                showlegend=False,
                meta=meta,
                customdata=customdata,
                hovertemplate=(
                    f"<b>{path}</b><br>"
                    f"{meta['group']} · {meta['component']} · {meta['label']}<br>"
                    f"PET {int(pet)} · Depth {int(depth)}<br>"
                    f"{timing_hover}"
                    "Duration %{customdata[1]:.6f} s<extra></extra>"
                ),
            )
        )

    if html_path is not None:
        write_trace_explorer_html(
            fig,
            plot_df,
            Path(html_path),
            xaxis_datetime=xaxis_datetime,
        )

    return fig
