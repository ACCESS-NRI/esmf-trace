import argparse
import json
from pathlib import Path
import pandas as pd
from parsers import (
    parse_stream_pet_indices,
    parse_pets,
    construct_stream_paths,
)
from extract_timing import df_for_selected_streams
from stats import (
    timeseries_component,
    stats_by_component,
    stats_by_component_pet,
)
from plotting import plot_flame_graph, insta_timeseries


def _require_outdir(args: argparse.Namespace) -> Path:
    """
    Create output directory if needed.
    """
    if args.output_dir is None:
        raise ValueError(
            "--output-dir is required when using --collect-stats or --collect-plots"
        )
    outdir = Path(args.output_dir)
    outdir.mkdir(parents=True, exist_ok=True)
    return outdir

def _compute_products(
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
    - `df`: input DataFrame with columns ['component', 'start', 'end', 'duration_s', 'depth', 'pet']
    - `model_component`: List of component names to filter suchas["OCN", "ICE"], or None for all.
    - `pets`: List of pet indices to filter, or None for all.
    - `stats_start_index`: Optional start index for slicing stats and timeseries.
    - `stats_end_index`: Optional end index for slicing stats and timeseries.
    Returns:
    A dictionary with the following keys and DataFrame values:
        - 'rp1_view': DataFrame view filtered to model components.
        - 'stats_full': Full runphase1 stats without slicing.
        - 'stats_sliced': Sliced runphase1 stats if slicing indices are provided, else None.
        - 'stats_by_component_pet': Runphase1 stats grouped by (model, pet) with slicing applied.
        - 'timeseries': timeseries data with slicing applied.
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

def _write_products_to_files(
    outdir: Path,
    products: dict[str, pd.DataFrame | None],
    base_prefix: str
) -> None:
    """
    Write computed products to CSV files in the specified output directory.
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

def _save_plots(
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


def run(args):
    indices = parse_stream_pet_indices(args.pets) if args.pets else []
    stream_paths = construct_stream_paths(args.traceout_path, indices, args.stream_prefix)
    pets = parse_pets(args.pets)

    # skip very short traces
    merge_gap_ns = args.merge_gap_ns if args.merge_adjacent else None

    df = df_for_selected_streams(
        traceout_path=args.traceout_path,
        stream_paths=stream_paths,
        pets=pets,
        merge_adjacent=args.merge_adjacent,
        merge_gap_ns=merge_gap_ns,
        max_depth=args.max_depth,
    )

    if df.empty:
        print("-- No rows parsed (empty DataFrame). Nothing to do.")
        return

    # if None means all components
    model_component = args.model_component.split(",") if args.model_component else None

    # Collect stats/timeseries when requested (also needed if plotting)
    products = None
    if args.collect_stats or args.plotting:
        products = _compute_products(
            df,
            model_component=model_component,
            pets=pets,
            stats_start_index=args.stats_start_index,
            stats_end_index=args.stats_end_index,
            save_mode=args.save_mode
        )
    def _print_product_sizes(tag: str, products: dict[str, pd.DataFrame | None]):
        def _n(x): return "None" if x is None else f"{len(x)} rows"
        print(f"-- {tag}:",
            f"timeseries_full={_n(products.get('timeseries_full'))},",
            f"stats_full={_n(products.get('stats_full'))},",
            f"stats_pet_full={_n(products.get('stats_pet_full'))},",
            f"timeseries_sliced={_n(products.get('timeseries_sliced'))},",
            f"stats_sliced={_n(products.get('stats_sliced'))},",
            f"stats_pet_sliced={_n(products.get('stats_pet_sliced'))}")

    # after computing products:
    _print_product_sizes("products", products)
    # Save stats
    if args.collect_stats and products is not None:
        outdir = _require_outdir(args)
        _write_products_to_files(outdir=outdir, products=products, base_prefix=args.base_prefix)

    # Save plots
    if args.plotting:
        outdir = _require_outdir(args)
        _save_plots(
            outdir=outdir,
            products=products,
            df=df,
            pets=pets,
            xaxis_datetime=args.xaxis_datetime,
            separate_plots=args.separate_plots,
            cmap=args.cmap,
            renderer=args.renderer,
            show_html=args.show_html,
            base_prefix=args.base_prefix,
            save_mode=args.save_mode
        )

def main():
    parser = argparse.ArgumentParser(
        prog="esmf-trace",
        description="ESMF traceout analysis and visualisation."
        )
    # base options
    parser.add_argument(
        "--traceout-path",
        type=Path,
        help="Path to the traceout root directory"
    )
    parser.add_argument(
        "--stream_prefix",
        type=str,
        default="esmf_stream",
        help="Stream file prefix - default is 'esmf_stream'"
    )
    parser.add_argument(
        "--pets",
        type=str,
        default=None,
        help="Comma-separated list of pets or ranges (e.g. '0,3-5,8')"
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=6,
        help="Only keep spans with stack depth up to N (default: 6).",
    )
    parser.add_argument(
        "--merge-adjacent",
        action="store_true",
        help="Merge adjacent spans"
    )
    parser.add_argument(
        "--merge-gap-ns",
        type=int,
        default=1000,
        help="Maximum gap in nano-seconds to merge adjacent spans (default: 1000)"
    )

    # stats options
    parser.add_argument(
        "--model-component",
        type=str,
        default=None,
        help="Comma-separated full hierarchical keys to keep. 'Example: [ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1/[OCN] RunPhase1'"
    )
    parser.add_argument(
        "--stats-start-index",
        type=int,
        default=None,
        help="Start index for stats (default: None)"
    )
    parser.add_argument(
        "--stats-end-index",
        type=int,
        default=None,
        help="End index for stats (default: None)"
    )
    parser.add_argument("--save-mode",
        choices=("full", "sliced", "both"),
        default="full",
        help="Which products to compute/save (default: full)")

    # plotting options
    parser.add_argument(
        "--xaxis-datetime",
        type=bool,
        default=False,
        help="Use wall-clock timestamps on x-axis"
    )
    parser.add_argument(
        "--separate-plots",
        type=bool,
        default=False,
        help="Separate PETs into different panels"
    )
    parser.add_argument(
        "--cmap",
        default="tab20",
        help="Matplotlib colormap name (default: tab20)"
    )
    parser.add_argument(
        "--renderer",
        default="browser", help="Plotly renderer (e.g. 'browser')"
    )
    parser.add_argument(
        "--show-html",
        action="store_true",
        help="Open the interactive figure after cli",
    )

    # stats and plotting options
    parser.add_argument(
        "--collect-stats",
        action="store_true",
        help="Collect and save stats to --output-dir"
    )
    parser.add_argument(
        "--plotting",
        action="store_true",
        help="Generate and save plots to --output-dir"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=None,
        help="Directory to write stats and/or plots. Used when --collect-stats and/or --collect-plots is set."
    )
    parser.add_argument(
        "--base-prefix",
        type=str,
        default="base",
        help="Base file name for output files (default: 'base')"
    )
    parser.set_defaults(func=run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()