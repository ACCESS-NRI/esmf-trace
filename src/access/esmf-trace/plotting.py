from pathlib import Path
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from plotly.subplots import make_subplots
from matplotlib import colormaps


def plot_flame_graph(
    df: pd.DataFrame,
    pets: int|list|None = None,
    *,
    as_datetime: bool = False,
    separate_plots: bool = False,
    cmap_name: str = "tab20",
    renderer: str | None = None,
    output_html: Path | None = None
    ):
    """
    Interactive flame graph for one or more pets.
    """

    if "pet" not in df.columns:
        df = df.assign(pet=0)
    
    if pets is None:
        pets = sorted(df["pet"].unique())
    elif isinstance(pets, int):
        pets = [pets]
    
    df = df[df["pet"].isin(pets)].copy()
    if df.empty:
        raise ValueError("no data for the selected pets")
    
    components = sorted(df["component"].unique())
    cmap = colormaps.get_cmap(cmap_name).resampled(len(components))
    colours = {
        c: f"rgb({int(r*255)},{int(g*255)},{int(b*255)})"
        for c, (r, g, b, _) in zip(components, cmap(range(len(components))))
    }

    # yaxis categories
    multi_overlay = (not separate_plots) and (len(pets) > 1)
    yaxis_kwargs = {}
    cat_order = None

    if multi_overlay:
        pet_index = {p: i for i, p in enumerate(sorted(pets))}
        df["y_cat"] = [f"depth{d}_pet_{p}" for d, p in zip(df["depth"], df["pet"])]
        df["__order"] = df["depth"] * len(pets) + df["pet"].map(pet_index)
        cat_order = df.sort_values("__order")["y_cat"].unique().tolist()
        y_col = y_hover_col = "y_cat"
        yaxis_kwargs = dict(categoryorder="array", categoryarray=cat_order)
    else:
        y_col = y_hover_col = "depth"
        yaxis_kwargs = dict(autorange="reversed")

    # xaxis columns
    if as_datetime:
        df["x_start"] = pd.to_datetime(df["start"], unit="ns")
        df["x_end"] = pd.to_datetime(df["end"], unit="ns")
        df["duration_s"] = df["duration"] / 1e9
    else:
        origin_ns = df["start"].min()
        df["x_start"] = (df["start"] - origin_ns) / 1e9
        df["x_end"] = df["x_start"] + df["duration"] / 1e9

    # plot
    if as_datetime:
        fig = px.timeline(
            df,
            x_start="x_start",
            x_end="x_end",
            y=y_col,
            category_orders={y_col: cat_order} if multi_overlay else None,
            color="component",
            color_discrete_map=colours,
            separate_plots_col="pet" if separate_plots and len(pets) > 1 else None,
            separate_plots_col_wrap=2 if separate_plots else None,
            hover_data={
                "component": True,
                "duration_s": ":.6f",
                "pet": True,
                y_hover_col: False,
            },
            title="Flame Graph https://github.com/ACCESS-NRI/esmf-trace",
        )
        fig.update_xaxes(title="Wall-clock time")
    else:
        df["width"] = df["x_end"] - df["x_start"]
        if separate_plots and len(pets) > 1:
            rows = (len(pets) + 1) // 2 if len(pets) > 2 else len(pets)
            cols = 2 if len(pets) > 2 else 1
            fig = make_subplots(
                rows=rows,
                cols=cols,
                subplot_titles=[f"Pet {p}" for p in pets],
                vertical_spacing=0.1 if rows > 1 else 0.15,
                horizontal_spacing=0.1 if cols > 1 else 0.15,
            )
            pet_subplot = {p: divmod(i, cols) for i, p in enumerate(pets)}
        else:
            fig = go.Figure()
            
            for p in pets:
                sub = df[df["pet"] == p]
                for comp, grp in sub.groupby("component", sort=False):
                    bar = go.Bar(
                        y=grp[y_col],
                        x=grp["width"],
                        base=grp["x_start"],
                        orientation="h",
                        name=comp if p == pets[0] else f"{comp} (PET {p})",
                        marker_color=colours[comp],
                        hovertext=[
                            f"{comp}<br>PET {p}<br>"
                            f"{y_hover_col}: {lbl}<br>"
                            f"start = {s:.6f}s<br>"
                            f"end   = {e:.6f}s<br>"
                            f"dur   = {w:.6f}s"
                            for lbl, s, e, w in zip(
                                grp[y_hover_col], grp["x_start"], grp["x_end"], grp["width"]
                            )
                        ],
                        hoverinfo="text",
                        showlegend=bool(p == pets[0]),
                    )
                    if separate_plots and len(pets) > 1:
                        r, c = next(pet_subplot(p))
                        fig.add_trace(bar, row=r+1, col=c+1)
                    else:
                        fig.add_trace(bar)
        
        fig.update_xaxes(
            title="Seconds since first event",
            showgrid=True,
            gridcolor="rgba(0,0,0,0.15)",
        )
        fig.update_layout(
            title="Flame Graph https://github.com/ACCESS-NRI/esmf-trace",
            bargap=0,
            barmode="overlay",
            legend_title_text="Component",
            template="simple_white",
        )

    # yaxis label
    y_title = "Stack Depth" if not multi_overlay else "Stack Depth and PET"
    fig.update_yaxes(title=y_title, **yaxis_kwargs)

    if separate_plots and len(pets) > 1:
        depth_max = df["depth"].max()

        common_range = [depth_max + 0.8, -1]

        for yaxis in fig.select_yaxes():
            yaxis.update(range=common_range, autorange=False)

    if renderer:
        pio.renderers.default = renderer

    if output_html:
        out = Path(output_html)
        out.parent.mkdir(parents=True, exist_ok=True)
        fig.write_html(str(out), include_plotlyjs="cdn")

    fig.show()
    return fig
