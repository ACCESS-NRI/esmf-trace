import argparse
import json
from pathlib import Path
import pandas as pd
from utils import extract_pets, construct_stream_paths
from extract_timing import df_for_selected_streams
from products import compute_products, write_products_to_files, save_product_plots


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

def run(args):
    pets = extract_pets(args.pets)
    stream_paths = construct_stream_paths(args.traceout_path, pets, args.stream_prefix)

    # skip very short traces, the default 1000nano seconds if enabled
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
        products = compute_products(
            df,
            model_component=model_component,
            pets=pets,
            stats_start_index=args.stats_start_index,
            stats_end_index=args.stats_end_index,
            save_mode=args.save_mode
        )

    # Save stats
    if args.collect_stats and products is not None:
        outdir = _require_outdir(args)
        write_products_to_files(outdir=outdir, products=products, base_prefix=args.base_prefix)

    # Save plots
    if args.plotting:
        outdir = _require_outdir(args)
        save_plots(
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
        help="Open the interactive figure after creating it",
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