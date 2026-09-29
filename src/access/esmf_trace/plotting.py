import math
from bisect import bisect_right
from datetime import datetime
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go

from .common_vars import seconds_to_nanoseconds
from .trace_explorer import annotate_selector_columns, write_trace_explorer_html

SECONDS_PER_DAY = 86400  # in seconds

# Prefer the first mediator operation in the ACCESS-OM3 coupling sequence.
# The fallbacks keep the feature useful if that phase was not traced.
COUPLING_ANCHOR_CANDIDATES = (
    ("Mediator", "MED", "aofluxes_run"),
    ("Coupling", "MED-TO-OCN", "MED -> OCN"),
    ("Run phase", "OCN", "RunPhase1"),
)


def _normalise_simulation_calendar(value: str) -> str:
    calendar = str(value).strip().lower()
    aliases = {
        "gregorian": "gregorian",
        "standard": "gregorian",
        "noleap": "noleap",
        "no_leap": "noleap",
    }
    try:
        return aliases[calendar]
    except KeyError:
        raise ValueError("simulation_calendar must be one of: gregorian/standard, noleap/no_leap") from None


def _normalise_simulation_start(value: str | None, calendar: str) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    parsed = datetime.fromisoformat(text.removesuffix("Z"))
    if parsed.tzinfo is not None:
        raise ValueError("simulation_start_datetime must be timezone-free")
    if calendar == "noleap" and parsed.month == 2 and parsed.day == 29:
        raise ValueError("29 February is invalid for the noleap calendar")
    return parsed.isoformat()


def _find_coupling_anchor(df: pd.DataFrame) -> tuple[list[int], dict | None]:
    """Return recurring coupling-cycle starts and the region used as the anchor."""
    for phase_group, phase_component, phase_label in COUPLING_ANCHOR_CANDIDATES:
        candidate = df[
            (df["phase_group"] == phase_group)
            & (df["phase_component"] == phase_component)
            & (df["phase_label"] == phase_label)
        ]
        if candidate.empty:
            continue

        # Nested children inherit the same selector classification. The
        # shallowest matching depth is the actual phase span.
        candidate = candidate[candidate["depth"] == candidate["depth"].min()]
        counts = candidate.groupby("pet").size()
        if counts.empty:
            continue

        max_count = int(counts.max())
        anchor_pet = min(int(pet) for pet, count in counts.items() if int(count) == max_count)
        starts = sorted(int(value) for value in candidate.loc[candidate["pet"] == anchor_pet, "start"].unique())
        if len(starts) >= 2:
            return starts, {
                "anchorGroup": phase_group,
                "anchorComponent": phase_component,
                "anchorLabel": phase_label,
                "anchorPet": anchor_pet,
            }

    return [], None


def annotate_simulation_clock(
    df: pd.DataFrame,
    coupling_timestep_seconds: int | None,
    simulation_start_datetime: str | None = None,
    simulation_calendar: str = "gregorian",
) -> tuple[pd.DataFrame, dict | None]:
    """Map trace spans to simulation day and coupling timestamp."""
    out = df.copy()
    out["simulation_day"] = math.nan
    out["coupling_timestamp_in_day"] = math.nan
    out["coupling_timestamp"] = math.nan

    if coupling_timestep_seconds is None:
        return out, None

    dt_couple = int(coupling_timestep_seconds)
    if dt_couple <= 0:
        raise ValueError("coupling_timestep_seconds must be > 0")
    if SECONDS_PER_DAY % dt_couple:
        raise ValueError(
            "coupling_timestep_seconds must divide 86400 exactly "
            "to identify a stable timestamp within each simulation day"
        )

    calendar = _normalise_simulation_calendar(simulation_calendar)
    start_datetime = _normalise_simulation_start(simulation_start_datetime, calendar)

    start_second_of_day = 0
    if start_datetime is not None:
        parsed_start = datetime.fromisoformat(start_datetime)
        start_second_of_day = parsed_start.hour * 3600 + parsed_start.minute * 60 + parsed_start.second
        if start_second_of_day % dt_couple:
            raise ValueError("simulation_start_datetime time-of-day must align with coupling_timestep_seconds")

    anchor_starts, metadata = _find_coupling_anchor(out)
    if metadata is None:
        return out, None

    # Every anchor begins one coupling timestamp. Let the final timestamp run
    # until finalisation starts, or to trace end if finalisation was not traced.
    finalisation = out.loc[
        (out["phase_group"] == "Finalisation") & (out["start"] > anchor_starts[-1]),
        "start",
    ]
    final_boundary = int(finalisation.min()) if not finalisation.empty else int(out["end"].max()) + 1
    anchor_ends = anchor_starts[1:] + [final_boundary]

    def timestamp_index(start_ns: int) -> int | None:
        index = bisect_right(anchor_starts, int(start_ns)) - 1
        if index < 0 or int(start_ns) >= anchor_ends[index]:
            return None
        return index

    indices = out["start"].map(timestamp_index)
    valid = indices.notna()
    zero_based = indices.loc[valid].astype(int)
    timestamps_per_day = SECONDS_PER_DAY // dt_couple
    simulated_second = zero_based * dt_couple + start_second_of_day

    out.loc[valid, "simulation_day"] = (simulated_second // SECONDS_PER_DAY + 1).astype(float)
    out.loc[valid, "coupling_timestamp_in_day"] = ((simulated_second % SECONDS_PER_DAY) // dt_couple + 1).astype(float)
    out.loc[valid, "coupling_timestamp"] = (zero_based + 1).astype(float)

    metadata.update(
        {
            "couplingTimestepSeconds": dt_couple,
            "timestampsPerDay": timestamps_per_day,
            "startDatetime": start_datetime,
            "calendar": calendar,
        }
    )
    return out, metadata


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
    coupling_timestep_seconds: int | None = None,
    simulation_start_datetime: str | None = None,
    simulation_calendar: str = "gregorian",
    html_path: Path | None = None,
):
    """
    Interactive flame graph for one or more pets.
    """
    plot_df, _ = _prepare_flame_data(df, pets, xaxis_datetime)
    plot_df, simulation_clock = annotate_simulation_clock(
        plot_df,
        coupling_timestep_seconds,
        simulation_start_datetime,
        simulation_calendar,
    )

    fig = go.Figure()
    grouped = plot_df.groupby(["pet", "model_component", "depth"], sort=False)
    for (pet, path, depth), group in grouped:
        row = group.iloc[0]

        if simulation_clock is None:
            # Preserve the existing Figure/customdata API when this feature is off.
            customdata = [
                [x_end, float(duration)]
                for x_end, duration in zip(group["x_end"], group["duration_seconds"], strict=True)
            ]
        else:
            customdata = [
                [
                    x_end,
                    float(duration),
                    None if pd.isna(day) else int(day),
                    None if pd.isna(timestamp_in_day) else int(timestamp_in_day),
                    None if pd.isna(timestamp) else int(timestamp),
                ]
                for x_end, duration, day, timestamp_in_day, timestamp in zip(
                    group["x_end"],
                    group["duration_seconds"],
                    group["simulation_day"],
                    group["coupling_timestamp_in_day"],
                    group["coupling_timestamp"],
                    strict=True,
                )
            ]

        if xaxis_datetime:
            timing_hover = "Start %{base}<br>End %{customdata[0]}<br>"
        else:
            timing_hover = "Start %{base:.6f} s<br>End %{customdata[0]:.6f} s<br>"

        simulation_hover = ""
        if simulation_clock is not None and group["coupling_timestamp"].notna().all():
            simulation_hover = (
                "Simulation day %{customdata[2]:.0f} · "
                f"Coupling timestamp %{{customdata[3]:.0f}}/{simulation_clock['timestampsPerDay']}<br>"
            )

        meta = {
            "leaf": row["phase_leaf"],
            "group": row["phase_group"],
            "component": row["phase_component"],
            "label": row["phase_label"],
            "depth": int(depth),
            "pet": int(pet),
            "points": int(len(group)),
            "path": path,
            "simulation_clock": simulation_clock,
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
                    f"{simulation_hover}"
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
