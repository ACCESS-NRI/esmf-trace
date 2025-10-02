from pathlib import Path
import pandas as pd
from stats import timeseries_component, stats_by_component, stats_by_component_pet
from plotting import plot_flame_graph, insta_timeseries


def compute_products(
    df: pd.DataFrame,
    model_component: list[str] | None,
    pets: list[int] | None,
    stats_start_index: int | None,
    stats_end_index: int | None,
    save_mode: str, # "full", "sliced", "both"
) -> dict[str, pd.DataFrame]:
    """
    Compute various stats and timeseries extracted from esmf_streams.
    The returned dict will be used for further analysis and plottings.
    Parameters:
    - `df`: input DataFrame.
    - `model_component`: List of component names to filter suchas["OCN", "ICE"], or None for all.
    - `pets`: List of pet indices to filter, or None for all.
    - `stats_start_index`: Optional start index for slicing stats and timeseries.
    - `stats_end_index`: Optional end index for slicing stats and timeseries.
    - `save_mode`: One of "full", "sliced", or "both" to control which products to compute.
    """
    stats_full = save_mode in ("full", "both")
    stats_sliced = save_mode in ("sliced", "both")

    def _timeseries(start_index, end_index):
        return timeseries_component(
            df=df,
            model_component=model_component,
            pets=pets,
            stats_start_index=start_index,
            stats_end_index=end_index,
        )

    def _stats_by_component(start_index, end_index):
        return stats_by_component(
            df=df,
            model_component=model_component,
            pets=pets,
            stats_start_index=start_index,
            stats_end_index=end_index,
        )

    def _stats_by_component_pet(start_index, end_index):
        return stats_by_component_pet(
            df=df,
            model_component=model_component,
            pets=pets,
            stats_start_index=start_index,
            stats_end_index=end_index,
        )

    products = {
        "timeseries_full": _timeseries(None, None) if stats_full else None,
        "stats_full": _stats_by_component(None, None) if stats_full else None,
        "stats_pet_full": _stats_by_component_pet(None, None) if stats_full else None,

        "timeseries_sliced": _timeseries(stats_start_index, stats_end_index) if stats_sliced else None,
        "stats_sliced": _stats_by_component(stats_start_index, stats_end_index) if stats_sliced else None,
        "stats_pet_sliced": _stats_by_component_pet(stats_start_index, stats_end_index) if stats_sliced else None,
    }

    return products

def write_products_to_files(
    outdir: Path,
    products: dict[str, pd.DataFrame | None],
    base_prefix: str
) -> None:
    """
    Write computed products to json files in the specified output directory.
    """
    def _write(name:str, df: pd.DataFrame | None) -> None:
        if df is not None:
            (outdir / f"{base_prefix}_{name}.json").write_text(df.to_json(orient="records", indent=2))

    _write("timeseries_full", products.get("timeseries_full"))
    _write("stats_full", products.get("stats_full"))
    _write("stats_by_component_pet_full", products.get("stats_pet_full"))

    _write("timeseries_sliced", products.get("timeseries_sliced"))
    _write("stats_sliced", products.get("stats_sliced"))
    _write("stats_by_component_pet_sliced", products.get("stats_pet_sliced"))

    print(f"-- Stats written to: {outdir}")

def save_product_plots(
    outdir: Path,
    products: dict[str, pd.DataFrame],
    df: pd.DataFrame,
    pets: list[int] | None,
    xaxis_datetime: bool,
    separate_plots: bool,
    cmap: str,
    renderer: str | None,
    show_html: bool,
    base_prefix: str,
    save_mode: str = "both",  # "full" | "sliced" | "both"
) -> None:

    out_paths = {
        "timeseries_full_png": None,
        "timeseries_sliced_png": None,
        "flame_graph_html": None,
    }

    plot_full = save_mode in ("full", "both")
    plot_sliced = save_mode in ("sliced", "both")

    if plot_full and products.get("timeseries_full") is not None:
        timeseries = products["timeseries_full"]
        out_png = outdir / f"{base_prefix}_timeseries_full.png"
        insta_timeseries(
            timeseries,
            out_png=out_png,
            xaxis_datetime=xaxis_datetime,
        )
        out_paths["timeseries_full_png"] = out_png

    if plot_sliced and products.get("timeseries_sliced") is not None:
        timeseries = products["timeseries_sliced"]
        out_png = outdir / f"{base_prefix}_timeseries_sliced.png"
        insta_timeseries(
            timeseries,
            out_png=out_png,
            xaxis_datetime=xaxis_datetime,
        )
        out_paths["timeseries_sliced_png"] = out_png

    # flame graph for all only
    html_path = outdir / f"{base_prefix}_flame_graph.html"
    plot_flame_graph(
        df=df,
        pets=pets,
        xaxis_datetime=xaxis_datetime,
        separate_plots=separate_plots,
        cmap_name=cmap,
        renderer=renderer,
        show_html=show_html,
        html_path=html_path,
    )
    out_paths["flame_graph_html"] = html_path

    return out_paths