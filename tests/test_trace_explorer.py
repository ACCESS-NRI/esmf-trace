from pathlib import Path

import pandas as pd

from access.esmf_trace.plotting import plot_flame_graph
from access.esmf_trace.trace_explorer import (
    TraceSelector,
    classify_trace_path,
    selector_colour_map,
    selector_description,
)


def _path(leaf: str) -> str:
    return f"[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/{leaf}"


def test_phase_first_classification_covers_models_driver_coupling_and_framework():
    assert classify_trace_path(_path("[OCN] IPDv03p1/internal")) == TraceSelector(
        "Initialisation",
        "OCN",
        "IPDv03p1",
    )
    assert classify_trace_path(_path("[MED] med_phases_post_ocn/foo")) == TraceSelector(
        "Mediator",
        "MED",
        "post_ocn",
    )
    assert classify_trace_path("[ESMF]/[ensemble] Init 1/[ESM0001] IPDv02p1") == TraceSelector(
        "Initialisation",
        "Driver",
        "IPDv02p1",
    )
    assert classify_trace_path(_path("[MED-TO-OCN] RunPhase1")) == TraceSelector(
        "Coupling",
        "MED-TO-OCN",
        "MED -> OCN",
    )
    assert classify_trace_path("[ESMF]/[ensemble] Init 1/(ensemble_driver.F90:SetModelServices)").group == "Framework"


def test_driver_and_model_ipd_descriptions_are_explanatory():
    driver = selector_description(TraceSelector("Initialisation", "Driver", "IPDv02p1"))
    ocean = selector_description(TraceSelector("Initialisation", "OCN", "IPDv03p1"))

    assert "driver" in driver.lower()
    assert "Advertise" in ocean


def test_selector_colours_are_unique():
    selectors = {
        "a": TraceSelector("Initialisation", "OCN", "IPDv03p1"),
        "b": TraceSelector("Initialisation", "OCN", "IPDv03p3"),
        "c": TraceSelector("Mediator", "MED", "post_ocn"),
        "d": TraceSelector("Mediator", "MED", "profile"),
        "e": TraceSelector("Coupling", "MED-TO-OCN", "MED -> OCN"),
        "f": TraceSelector("Coupling", "OCN-TO-MED", "OCN -> MED"),
    }

    colours = selector_colour_map(selectors)

    assert len(set(colours.values())) == len(colours)


def test_plot_writes_explorer_and_preserves_region_count(tmp_path: Path):
    ns = 1_000_000_000

    df = pd.DataFrame(
        [
            {
                "model_component": _path("[OCN] IPDv03p1/internal"),
                "start": 0,
                "end": ns,
                "duration_s": ns,
                "depth": 3,
                "pet": 0,
            },
            {
                "model_component": _path("[OCN] IPDv03p1/internal"),
                "start": 2 * ns,
                "end": 3 * ns,
                "duration_s": ns,
                "depth": 3,
                "pet": 0,
            },
            {
                "model_component": _path("[MED-TO-OCN] RunPhase1"),
                "start": ns,
                "end": 2 * ns,
                "duration_s": ns,
                "depth": 2,
                "pet": 0,
            },
        ]
    )

    html_path = tmp_path / "trace.html"
    fig = plot_flame_graph(df, html_path=html_path)

    # Two logical region traces, but three timing spans.
    assert len(fig.data) == 2

    text = html_path.read_text()

    assert '"regionCount":2' in text
    assert '"spanCount":3' in text
    assert "Initialisation" in text
    assert "Coupling" in text
    assert "Reset view" in text

    # Reset-view contract.
    assert "const FULL_X=Object.freeze(P.fullX.slice())" in text
    assert "range:FULL_X.slice()" in text
    assert '"xaxis.range[0]":FULL_X[0]' in text
    assert '"xaxis.range[1]":FULL_X[1]' in text
    assert "fixedrange:true" in text
    assert 'modeBarButtonsToRemove:["autoScale2d","resetScale2d"' in text
    assert "plotOperationRunning" in text
    assert "runPlotOperations" in text
    assert "Filtering changes visibility/Y rows only and never touches the X range" in text
    assert 'document.getElementById("reset").onclick=scheduleResetView' in text

    # Region descriptions remain compact in cards but are fully readable.
    assert "-webkit-line-clamp:2" in text
    assert 'id="leaf-preview"' in text
    assert 'id="leaf-tooltip"' in text
    assert "showLeafPreview" in text
    assert "showLeafTooltip" in text
    assert 'b.setAttribute("aria-label"' in text

    ocn_trace = next(trace for trace in fig.data if trace.meta["component"] == "OCN")
    assert list(ocn_trace.base) == [0.0, 2.0]
    assert list(ocn_trace.x) == [1.0, 1.0]
    assert [list(values) for values in ocn_trace.customdata] == [[1.0, 1.0], [3.0, 1.0]]
    assert "End %{customdata[0]:.6f} s" in ocn_trace.hovertemplate
    assert "Duration %{customdata[1]:.6f} s" in ocn_trace.hovertemplate


def test_flame_graph_duration_is_derived_from_start_and_end():
    ns = 1_000_000_000
    df = pd.DataFrame(
        [
            {
                "model_component": _path("[ATM] RunPhase1"),
                "start": 0,
                "end": ns,
                "duration_s": ns,
                "depth": 1,
                "pet": 0,
            },
            {
                "model_component": _path("[OCN] RunPhase1"),
                "start": 2 * ns,
                "end": 5 * ns,
                # Deliberately inconsistent: visual duration must still come from end - start.
                "duration_s": 99 * ns,
                "depth": 1,
                "pet": 0,
            },
        ]
    )

    fig = plot_flame_graph(df)
    ocn_trace = next(trace for trace in fig.data if trace.meta["component"] == "OCN")

    assert list(ocn_trace.base) == [2.0]
    assert list(ocn_trace.x) == [3.0]
    assert [list(values) for values in ocn_trace.customdata] == [[5.0, 3.0]]
