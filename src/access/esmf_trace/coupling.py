"""
Per-coupling-step load balance between the mediator and each component that
runs concurrently with it.

CMEPS couples hub-and-spoke: every exchange goes through the mediator (MED).
A component X on PETs that do not run MED - the ocean in ACCESS-OM3, or a
wave model given its own PETs - runs concurrently with the MED side and meets
it only at its two connectors, once per coupling step of X:

    X PET:    [MED-TO-X] -> [X] ------------------------------> [MED-TO-X] (next step)
    MED PET:  [MED-TO-X] -> work on the MED PETs -> [X-TO-MED] -> work up to the next [MED-TO-X]

Components sharing the MED PETs (ATM, ICE and ROF in ACCESS-OM3) run in
sequence with the mediator, so their time is MED-side work, reported per step
as <tag>_active_s.

Whichever side reaches the meeting point first waits there, so for step i of X:

    wait_s              [MED-TO-X] on the X PET at step i+1 - X idles until the
                        MED side has finished step i
    med_wait_s          [X-TO-MED] on the MED PET at step i - the MED side idles
                        until X has finished step i
    med_concurrent_s    MED-PET time between handing data to X and needing it
                        back (overlaps X)
    med_serial_s        MED-PET time after receiving X's data and before the
                        next MED-TO-X (X always waits for this)

The MED PETs follow a single timeline, so with several concurrent components
the MED-side times of one include any wait on another that falls in its window.

Every quantity is a timestamp difference on a single PET, so no clock
comparison across nodes is needed.
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

from .common_vars import seconds_to_nanoseconds
from .config import _norm_pets
from .postprocess import _collect_case_jsons, _load_timeseries_json

_TAG_RE = re.compile(r"^\[([^\]]+)\]")
_DRIVER_RUN_RE = re.compile(r"^\[ESM\d+\] RunPhase\d+$")


def _run_tag(path: str) -> str | None:
    """
    Tag of a region directly under the driver's RunPhase - e.g. "OCN", "MED"
    or "MED-TO-OCN" - or None for anything else, including Init/Finalize.
    """
    parts = str(path).rsplit("/", 2)
    if len(parts) < 2 or not _DRIVER_RUN_RE.match(parts[-2].strip()):
        return None
    match = _TAG_RE.match(parts[-1].strip())
    return match.group(1) if match else None


def _select_pets(requested: int | str | list[int] | None, candidates: set[int], role: str) -> list[int]:
    if not candidates:
        raise ValueError(f"No {role} PET found in the timeseries")
    if requested is None:
        return sorted(candidates)
    pets = _norm_pets(requested)
    unknown = set(pets) - candidates
    if unknown:
        raise ValueError(f"PET(s) {sorted(unknown)} are not {role} PETs; candidates: {sorted(candidates)}")
    return pets


def _calls(events: pd.DataFrame, tag: str, pets: list[int]) -> dict[str, np.ndarray]:
    """start, end (ns) and duration_s of one tag's calls as (call, PET) arrays, PETs in `pets` order."""
    selected = events[(events["tag"] == tag) & events["pet"].isin(pets)]
    if selected.empty:
        raise ValueError(f"No [{tag}] run-phase events on PETs {pets}")
    paths = selected["model_component"].unique()
    if len(paths) > 1:
        raise ValueError(f"More than one run-phase region is tagged [{tag}]: {sorted(paths)}")
    counts = selected.groupby("pet").size().reindex(pets, fill_value=0)
    if counts.nunique() != 1:
        raise ValueError(f"Unequal [{tag}] call counts across PETs: {counts[counts != counts.max()].to_dict()}")
    selected = selected.sort_values(["pet", "start"], kind="mergesort")
    shape = (len(pets), int(counts.iloc[0]))
    return {column: selected[column].to_numpy().reshape(shape).T for column in ("start", "end", "duration_s")}


def _component_steps(events: pd.DataFrame, comp: str, comp_pets: list[int], med_pets: list[int]) -> pd.DataFrame:
    """coupling_steps() rows for one concurrent component."""
    to_comp, from_comp = f"MED-TO-{comp}", f"{comp}-TO-MED"
    run = _calls(events, comp, comp_pets)
    to_comp_at_comp = _calls(events, to_comp, comp_pets)
    to_comp_at_med = _calls(events, to_comp, med_pets)
    from_comp_at_med = _calls(events, from_comp, med_pets)

    counts = {
        f"[{comp}]": len(run["start"]),
        f"[{to_comp}]@{comp}": len(to_comp_at_comp["start"]),
        f"[{to_comp}]@MED": len(to_comp_at_med["start"]),
        f"[{from_comp}]@MED": len(from_comp_at_med["start"]),
    }
    if len(set(counts.values())) != 1:
        raise ValueError(f"Unequal coupling event counts: {counts}")
    n = len(run["start"]) - 1  # the last call has no following step to close it
    if n < 1:
        raise ValueError(f"{comp}: at least two coupling calls are needed")

    # (step, PET) arrays; per step, the slowest PET of each side is kept
    wait = to_comp_at_comp["duration_s"][1:].astype(float)
    med_wait = from_comp_at_med["duration_s"][:n].astype(float)
    med_window_start = to_comp_at_med["end"].astype("int64")
    walltime = np.diff(run["start"].astype("int64"), axis=0).mean(axis=1) / seconds_to_nanoseconds
    med_concurrent = (from_comp_at_med["start"][:n] - med_window_start[:n]) / seconds_to_nanoseconds
    med_serial = (to_comp_at_med["start"][1:] - from_comp_at_med["end"][:n]) / seconds_to_nanoseconds
    steps = pd.DataFrame(
        {
            "component": comp,
            "coupling_step": np.arange(n),
            "walltime_s": walltime,
            "active_s": run["duration_s"][:n].astype(float).max(axis=1),
            "wait_s": wait.min(axis=1),
            "med_concurrent_s": med_concurrent.max(axis=1),
            "med_wait_s": med_wait.min(axis=1),
            "med_serial_s": med_serial.max(axis=1),
        }
    )

    # Model components on each MED PET, each event assigned to the step whose
    # window [MED-TO-X end, next MED-TO-X end) on that PET holds it.
    direct = events[events["pet"].isin(med_pets) & events["tag"].notna()]
    direct = direct[~direct["tag"].str.contains("-TO-", regex=False)]
    column = {pet: i for i, pet in enumerate(med_pets)}
    step = direct.groupby("pet")["start"].transform(
        lambda s: np.searchsorted(med_window_start[:, column[s.name]], s.to_numpy("int64"), side="right") - 1
    )
    direct = direct.assign(coupling_step=step)[(step >= 0) & (step < n)]
    active = (
        direct.groupby(["coupling_step", "tag", "pet"])["duration_s"].sum().groupby(["coupling_step", "tag"]).max()
    ).unstack("tag")
    active = active.reindex(range(n)).fillna(0.0)
    active.columns = [f"{tag.lower()}_active_s" for tag in active.columns]
    return steps.join(active, on="coupling_step")


def coupling_steps(
    df: pd.DataFrame,
    components: str | list[str] | None = None,
    pets: dict[str, int | str | list[int]] | None = None,
    start_index: int | None = None,
    end_index: int | None = None,
) -> pd.DataFrame:
    """
    One row per coupling step of each component running concurrently with the
    mediator, from a *_timeseries.json frame.

    components: tag(s) of the concurrent components to analyse, e.g. "OCN" or
        ["OCN", "WAV"]. None takes every component that runs on PETs where
        MED does not.
    pets: PETs to use per tag, including "MED", as an int, a list or a range
        string such as "24-311:48"; a tag left out uses every qualifying PET
        in the frame. With several PETs on a side, each step keeps the slowest
        PET: the smallest wait (the time that side really waited for the
        other), and the largest active/concurrent/serial time.
    start_index, end_index: iloc slice of each component's steps, e.g. 2 and
        -1 to drop two warm-up steps and the final output boundary.

    Columns: component, coupling_step, walltime_s, active_s (the component's
    RunPhase duration), wait_s, med_concurrent_s, med_wait_s, med_serial_s
    (see the module docstring), plus <tag>_active_s for each model component
    on the MED PETs (e.g. ice_active_s, atm_active_s, med_active_s), summed
    over the step's MED-PET window.
    """
    tag_of = {path: _run_tag(path) for path in df["model_component"].unique()}
    events = df.assign(tag=df["model_component"].map(tag_of))
    pets = pets or {}

    pets_by_tag = {tag: {int(p) for p in group["pet"].unique()} for tag, group in events.groupby("tag")}
    all_med_pets = pets_by_tag.get("MED", set())
    med_pets = _select_pets(pets.get("MED"), all_med_pets, "MED")
    concurrent = {
        tag: tag_pets - all_med_pets
        for tag, tag_pets in pets_by_tag.items()
        if tag != "MED" and "-TO-" not in tag and tag_pets - all_med_pets
    }

    if components is None:
        components = sorted(concurrent)
    elif isinstance(components, str):
        components = [components]
    if not components:
        raise ValueError("No component runs on PETs apart from MED")

    parts = []
    for comp in components:
        if comp not in concurrent:
            raise ValueError(
                f"[{comp}] has no run-phase PETs apart from MED; concurrent components: {sorted(concurrent)}"
            )
        comp_pets = _select_pets(pets.get(comp), concurrent[comp], comp)
        steps = _component_steps(events, comp, comp_pets, med_pets)
        parts.append(steps.iloc[slice(start_index, end_index)])
    return pd.concat(parts, ignore_index=True)


def coupling_steps_for_case(
    post_base_path: str | Path,
    case_name: str,
    output_index: list[int] | None = None,
    timeseries_suffix: str = "_timeseries.json",
    **kwargs,
) -> pd.DataFrame:
    """
    coupling_steps() for every outputNNN of one postprocessed case, stacked
    with case_name and output_name columns. kwargs go to coupling_steps(),
    and are applied to each output separately.
    """
    jsons = _collect_case_jsons(Path(post_base_path), case_name, output_index, timeseries_suffix)
    if not jsons:
        raise FileNotFoundError(f"No *{timeseries_suffix} under {Path(post_base_path) / case_name}")
    parts = []
    for path in jsons:  # already in output-index order
        ts = _load_timeseries_json(path)
        steps = coupling_steps(ts, **kwargs)
        parts.append(steps.assign(case_name=case_name, output_name=ts["__output_name"].iloc[0]))
    return pd.concat(parts, ignore_index=True)
