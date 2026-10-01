# Flame graph

`esmf-trace` writes an interactive flame graph for every processed outputNNN directory. The flame graph is presented as the `ESMF Trace Explorer`: a phase-first view of the `ESMF/NUOPC` timing hierarchy that is designed to remain usable when a trace contains hundreds of timing regions.

The generated `flamegraph` html file is written alongside the corresponding `run_settings` and `timeseries` json files:

```bash
<post_base_path>/postprocessing_<base_prefix>/outputNNN/<base_prefix>_run_settings.json
<post_base_path>/postprocessing_<base_prefix>/outputNNN/<base_prefix>_timeseries.json
<post_base_path>/postprocessing_<base_prefix>/outputNNN/<base_prefix>_flamegraph.html
```

The HTML is self-contained, including `Plotly`, so it can be copied elsewhere and opened directly in a browser. If the file is on a remote system such as Gadi, its directory can also be served with `python3 -m http.server 8000` and accessed through the appropriate remote-session or port-forwarding setup.


## ESMF trace explorer overview

The screenshot below shows the default ESMF Trace Explorer layout. An example html - `flamegraph.html` can be found under `esmf-trace/examples`.

![flame-graph-demonstration](/assets/flame_graph_demonstration.png){: loading="lazy" }

From top to bottom, the page is organised into five main areas:

1. Header and tools - the search box, region counter, number of PETs, PET indices, `Clear filters`, and `Reset view`.
2. Phase / region group selector - high-level groups such as `Initialisation`, `Runphase`, `Finalisation`, `Mediator`, `Coupling` and `Framework`.
3. Stack depth selector - depths chips used to decide which depth / PET rows appear on the Y axis.
4. Time axis - with the default time axis, the first event starts at `0` and the X axis is `Elapsed time (s)`.
5. Timeline plot - the actual trace explorer, where each horizontal bar is a timing span plotted against elapsed time.

!!! warning "Stack depth is the trace hierarchy depth"
    Stack depth describes where a timing region sits in the nested ESMF trace hierarchy. It is not a model vertical level and is unrelated to ocean, atmosphere or sea-ice grid depth.


## Selecting phase groups and regions

The **Phase / region group** row controls which timing regions are visible.

By default, `ALL` is selected and every phase/region group is shown. Clicking a group such as **Initialisation** or **Coupling** selects all regions in that group. Group selections are additive, so multiple groups can be shown at the same time.

Hover over a group, or click its drop-down indicator, to open the detailed selector. This allows individual regions to be selected within the group. For example, the **Coupling** menu can contain:

```text
ATM -> MED
ROF -> MED
MED -> OCN
OCN -> MED
MED -> ICE
ICE -> MED
```

The detailed selector also contains a search box. This search only filters the options displayed in that menu; it does not itself change the trace explorer until a region is selected.

!!! tip "Start broad, then refine"
    Start with a phase group such as **Run phase** or **Coupling**, then open the detailed menu only when an individual model component or phase needs to be isolated.


## Filtering by stack depth

In the above screenshot, this is the second selector row, directly above the plot. Its chips decide which `Depth N · PET M` rows appear on the Y axis.

`ALL` displays every available stack depth. Selecting one or more numbered depths displays only those Y-axis rows.

For example:

```yaml
Phase / region group: OCN · RunPhase1
Stack depth: 3
```

shows only matching `OCN · RunPhase1` regions at depth 3.

If the selected phase/region does not exist at the selected depth, no bars are shown and the following message appears:

```
No timing regions match the current phase/component/depth selection.
```

!!! tip "Only the depth selector changes the Y-axis rows"
    Selecting a phase or region does **not** collapse the Y axis to the depths where that region happens to exist. With `Stack depth: ALL`, the complete stack-depth/PET domain remains visible even if the selected region only occurs at one depth.

The Y axis is fixed while zooming. Zooming therefore changes the time range only; stack-depth labels remain visible.


## Zooming and Reset view

Use the `Plotly` zoom controls, drag across the plot, or scroll within the timeline panel to zoom the **X axis**.

`Reset view` has one specific meaning:

- restore the X axis to the **complete original time range** of the generated trace explorer; and
- restore the Y axis to the rows implied by the **current Stack depth selection**.

It does **not** clear the current phase, region, depth or text filters.

For example, if only `OCN · RunPhase1` and depth `3` are selected, clicking `Reset view` shows:

```text
X: complete original trace time range
Y: depth 3 rows only
data: OCN · RunPhase1 only
```

!!! warning "`Reset view` and `Clear filters` are different"
    **Reset view** resets the axes while keeping the current filters. **Clear filters** removes the phase/region, depth and text filters, but does not redefine the meaning of the global time range.

!!! warning "standard plotly autoscale/reset-axis"
    The standard `Plotly` autoscale/reset-axis controls are intentionally not used for this view because their normal behaviour is to fit the currently visible data. That is different from the explorer's definition of returning to the complete trace time domain.


## Searching timing regions

The search box at the top of the explorer filters the plotted regions using:

- the complete hierarchical timing path;
- the phase/component label; and
- the component name.

Search is combined with the current phase/region and stack-depth selections.

For example, searching for:

```text
runphase1
```

can match more than one component if that phase exists in several components. Use the phase menu to distinguish entries such as:

```text
ATM · RunPhase1
OCN · RunPhase1
ICE · RunPhase1
...
```


## Hover information

Hover over a timing span to inspect its full provenance and timing information. The explorer shows:

- the complete `model_component` timing path;
- phase/region group · component · phase/region label;
- PET · stack depth;
- span start time;
- span end time;
- simulation day and coupling timestamp, when available;
- span duration in seconds.

!!!tip "Simulation timing"
    Simulation timing is derived from the recurring coupling cycles using `coupling_timestep_seconds` and `simulation_calendar`. It describes model simulation time rather than the wall-clock time taken to execute the span.


## How filters combine

The explorer applies the active controls together:

```text
phase / region selection
        AND
stack-depth selection
        AND
global text search
```

Within a phase/region selection, multiple selected leaves are combined with `OR`. Multiple selected depths are also combined with `OR`.

For example:

```text
Regions: OCN · RunPhase1 OR ICE · RunPhase1
Depths:  2 OR 3
Search box:  RunPhase
```
shows regions satisfying all three filter dimensions.

Filtering changes region visibility in a single `Plotly` update and does not rebuild the span data or modify the current X-axis zoom.


## Interpreting the trace explorer

The trace explorer is useful for understanding **when** work occurs and how timing regions are nested.

Typical uses include:

- locating expensive or repeated model run phases;
- comparing `ATM`, `OCN`, `ICE`, `MED` and `ROF` activity over the same time interval;
- inspecting directional coupling costs;
- separating initialisation/finalisation overhead from steady-state run work;
- finding mediator preparation, diagnostics or I/O regions;
- comparing the same region across selected PETs; and
- zooming into a short interval while preserving the surrounding stack structure.

The trace explorer should not by itself be interpreted as a scaling statistic. For aggregate timing comparisons across calls, outputs or experiments, use the timeseries and post-summary functionality alongside the trace explorer.


## Library usage

The trace explorer can also be generated directly from a parsed trace DataFrame:

```python
from pathlib import Path
from access.esmf_trace.plotting import plot_flame_graph

fig = plot_flame_graph(
    df,
    pets=[0, 104],
    xaxis_datetime=False,
    coupling_timestep_seconds=900,  # coupling timestep in seconds from nuopc.runseq
    simulation_calendar="gregorian",  # for a no-leap calendar, "no_leap"
    simulation_start_datetime="2000-01-01T00:00:00", # optional
    html_path=Path("trace_flamegraph.html"),
)
```

`plot_flame_graph` returns the underlying `plotly.graph_objects.Figure` and, if `html_path` is provided, writes the interactive Trace Explorer HTML.
