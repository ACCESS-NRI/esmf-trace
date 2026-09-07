# Flame Graph

`esmf-trace` writes an interactive flame graph for every processed outputNNN directory. The flame graph is presented as the `ESMF Trace Explorer`: a phase-first view of the `ESMF/NUOPC` timing hierarchy that is designed to remain usable when a trace contains hundreds of timing regions.

The generated file is written alongside the corresponding timeseries JSON:

```bash
<post_base_path>/postprocessing_<base_prefix>/outputNNN/
<base_prefix>_timeseries.json
<base_prefix>_flamegraph.html
```
The HTML is self-contained, including `Plotly`, so it can be copied elsewhere and opened directly in a browser.


## Flame graph overview

The screenshot below shows the default ESMF Trace Explorer layout.

![flame-graph-demonstration](asset/flame_graph_demonstration.png){: loading="lazy" }

From top to bottom, the page is organised into four main areas:

1. Header and tools - the search box, region counter, `Clear filters`, and `Reset View`.
2. Phase / region group selector - broad lifestyle groups such as `Initialisation`, `RunPhase`, `Finalisation`, `Mediator`, `Coupling` and `Framework`.
3. Stack depth selector - depths chips used to decide which depth / PET rows appear on the Y axis.
4. With the default time axis, the first event starts at `0` and the X axis is `Elapsed time (s)`.
5. Timeline plot - the actual flame graph, where each horizontal bar is a timing span plotted against elapsed time.

!!! warning "Stack depth is the trace hierarchy depth"
    Stack depth describes where a timing region sits in the nested ESMF trace hierarchy. It is not a model vertical level and is unrelated to ocean, atmosphere or sea-ice grid depth.


## Phase / region groups

The explorer organises the full ESMF region paths into a small set of phase-first groups:

| Group | What it contains |
|---|---|
| **Initialisation** | Model and top-level driver `Init` and NUOPC `IPD` (Initialize Phase Definition) phases. |
| **Run phase** | Model and driver `RunPhase*` regions used while advancing the coupled model. |
| **Finalisation** | Model and driver `Finalize*` regions used during shutdown and cleanup. |
| **Mediator** | Mediator preparation, post-processing, diagnostics, history/restart I/O and related mediator work. |
| **Coupling** | Directional component-to-component exchanges such as `MED-TO-OCN` and `OCN-TO-MED`. |
| **Framework** | ESMF/ensemble runtime overhead that is outside the named model lifecycle phases above. |

For lifecycle groups, the detailed menu is further organised by component, for example:

```text
Initialisation
├── ATM
├── OCN
├── ICE
├── MED
├── ROF
└── Driver
```

The top-level NUOPC driver is shown as `Driver` rather than being mixed into generic framework overhead.

### [NUOPC IPD phases](https://earthsystemmodeling.org/docs/release/latest/NUOPC_refdoc/node8.html)

Names such as:

```text
IPDv03p1
IPDv03p6
IPDv05p3
```

are NUOPC **Initialize Phase Definition** labels. The explorer reads `IPDvXXpY` as:

```text
Initialize Phase Definition version XX, phase Y
```

Known model and driver IPD phases have a short description in the detailed selector, for example:

```text
OCN IPDv03p1
Advertise fields and geometry-transfer capability.
```

```text
Driver IPDv05p3
Configure driver connector coupling lists and redistribution.
```

These descriptions are navigation aids for interpreting the trace. The complete ESMF timing path is still retained and is shown when hovering over the plotted region.

## Selecting phase groups and regions

The **Phase / region group** row controls which timing regions are visible.

In the screenshot, this is the first selector row beneath the header. It starts with `ALL`, followed by group chips such as `Initialisation`, `Run phase`, `Finalisation`, `Mediator`, `Coupling` and `Framework`.

By default:

```text
ALL
```

is selected and every phase/region group is shown.

Clicking a group such as **Initialisation** or **Coupling** selects all regions in that group. Group selections are additive, so multiple groups can be shown at the same time.

Hover over a group, or click its drop-down indicator, to open the detailed selector. This allows individual regions to be selected within the group. For example, the Coupling menu can contain:

```text
ATM -> MED
ROF -> MED
MED -> OCN
OCN -> MED
MED -> ICE
ICE -> MED
```

A partially selected group is shown with a partial state and a count of the selected entries.

The detailed selector also contains a search box. This search only filters the options displayed in that menu; it does not itself change the flame graph until a region is selected.

!!! tip "Start broad, then refine"
    Start with a phase group such as **Run phase** or **Coupling**, then open the detailed menu only when an individual model component or phase needs to be isolated.

## Filtering by stack depth

The **Stack depth** row is independent from the phase/region selection.

In the screenshot, this is the second selector row, directly above the plot. Its chips decide which `Depth N · PET M` rows appear on the Y axis.

`ALL` displays every available stack depth. Selecting one or more numbered
depths displays only those Y-axis rows.

For example:

```text
Phase / region group: OCN · IPDv03p1
Stack depth: 3
```

shows only matching `OCN · IPDv03p1` regions at depth 3.

If the selected phase/region does not exist at the selected depth, no bar is shown.

!!! tip "Only the depth selector changes the Y-axis rows"
    Selecting a phase or region does **not** collapse the Y axis to the depths where that region happens to exist. With `Stack depth: ALL`, the complete stack-depth/PET domain remains visible even if the selected region only occurs at one depth.

The Y axis is fixed while zooming. Zooming therefore changes the time range only; stack-depth labels remain visible.

## Zooming and Reset view

Use the `Plotly` zoom controls, drag across the plot, or scroll within the timeline panel to zoom the **X axis**.

`Reset view` has one specific meaning:

- restore the X axis to the **complete original time range** of the generated flame graph; and
- restore the Y axis to the rows implied by the **current Stack depth selection**.

It does **not** clear the current phase, region, depth or text filters.

For example, if only `OCN · IPDv03p1` and depth 3 are selected, clicking `Reset view` shows:

```text
X: complete original trace time range
Y: depth 3 rows only
data: OCN · IPDv03p1 only
```

The X range is never fitted to only the currently visible phase or region.

!!! warning "`Reset view` and `Clear filters` are different"
    **Reset view** resets the axes while keeping the current filters. **Clear filters** removes the phase/region, depth and text filters, but does not redefine the meaning of the global time range.

The standard `Plotly` autoscale/reset-axis controls are intentionally not used for this view because their normal behaviour is to fit the currently visible data. That is different from the explorer's definition of returning to the complete trace time domain.

## Searching timing regions

The search box at the top of the explorer filters the plotted regions using:

- the complete hierarchical timing path;
- the phase/component label; and
- the component name.

Search is combined with the current phase/region and stack-depth selections.

For example, searching for:

```text
IPDv03p1
```

can match more than one component if that phase exists in several components.
Use the phase menu to distinguish entries such as:

```text
OCN · IPDv03p1
MED · IPDv03p1
```

Searching does not change the X-axis range.

## Hover information

Hover over a timing span to inspect its full provenance. The explorer shows:

- the complete `model_component` timing path;
- phase group;
- component;
- phase/region label;
- PET;
- stack depth;
- span start time; and
- span duration in seconds.

The full hierarchical path is kept even when the same phase is presented under
a shorter selector such as `OCN · RunPhase1`.

## Colours and region boundaries

Colours are assigned to the detailed phase/region selectors rather than only to the broad group. Different regions therefore remain distinguishable even when they belong to the same component or phase group.

Related component phases use related hues, while individual detailed selectors receive distinct colours. Timing bars also have a thin dark outline, and stack-depth/PET rows use horizontal dotted separators.

Colour is a navigation aid only. It does not encode timing magnitude, percentage, load balance or any other quantitative value.

## How filters combine

The explorer applies the active controls together:

```text
phase / region selection
        AND
stack-depth selection
        AND
global text search
```

Within a phase/region selection, multiple selected leaves are combined with
`OR`. Multiple selected depths are also combined with `OR`.

For example:

```text
Regions: OCN · RunPhase1 OR ICE · RunPhase1
Depths:  2 OR 3
Search:  RunPhase
```

shows regions satisfying all three filter dimensions.

Filtering changes region visibility in a single Plotly update and does not
rebuild the span data or modify the current X-axis zoom.

## Interpreting the flame graph

The flame graph is useful for understanding **when** work occurs and how timing
regions are nested.

Typical uses include:

- locating expensive or repeated model run phases;
- comparing `ATM`, `OCN`, `ICE`, `MED` and `ROF` activity over the same time interval;
- inspecting directional coupling costs;
- separating initialisation/finalisation overhead from steady-state run work;
- finding mediator preparation, diagnostics or I/O regions;
- comparing the same region across selected PETs; and
- zooming into a short interval while preserving the surrounding stack structure.

The flame graph should not by itself be interpreted as a scaling statistic. For aggregate timing comparisons across calls, outputs or experiments, use the timeseries and post-summary functionality alongside the flame graph.

## Output size and responsiveness

Large ESMF traces can contain tens of thousands of spans and hundreds of logical regions. The explorer is structured to avoid duplicating the complete timing path for every span:

- one `Plotly` trace is retained per logical timing region;
- the complete path is stored once in trace metadata;
- the legend is replaced by the phase-first selector; and
- one Plotly update is used for each filtering interaction.

This keeps the HTML substantially smaller and the interactive filtering more
responsive than rendering hundreds of traditional legend entries.

## Library usage

The flame graph can also be generated directly from a parsed trace DataFrame:

```python
from pathlib import Path

from access.esmf_trace.plotting import plot_flame_graph

fig = plot_flame_graph(
    df,
    pets=[0, 104],
    xaxis_datetime=False,
    html_path=Path("trace_flamegraph.html"),
)
```

`plot_flame_graph` returns the underlying `plotly.graph_objects.Figure` and, if `html_path` is provided, writes the interactive Trace Explorer HTML.
