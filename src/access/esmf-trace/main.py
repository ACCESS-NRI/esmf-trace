import argparse
from pathlib import Path
import pandas as pd
from utils import parse_stream_pet_indices, parse_pets, construct_stream_paths, write_df, plot_html
from extract_timing import df_for_selected_streams


def cmd_run(args):
    indices = parse_stream_pet_indices(args.pets)
    stream_paths = construct_stream_paths(args.traceout_path, indices, args.stream_prefix)
    pets = parse_pets(args.pets)

    if not args.merge_adjacent:
        merge_gap_ns = None
    else:
        merge_gap_ns = args.merge_gap_ns

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

    # write to file
    if args.write_to_file is not None:
        write_df(df, args.write_to_file)

    # save/render plot in html
    if args.write_to_html is not None:
        plot_html(
            df=df,
            pets=pets,
            xaxis_datetime=args.xaxis_datetime,
            separate_plots=args.separate_plots,
            cmap=args.cmap,
            renderer=args.renderer,
            show_html=args.show_html,
            write_to_html=args.write_to_html,
        )

def main():
    parser = argparse.ArgumentParser(
        prog="esmf-trace",
        description="ESMF traceout analysis and visualisation."
        )

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
        "--write-to-file",
        type=Path,
        default=None,
        help="Output CSV file path"
    )
    parser.add_argument(
        "--max-depth",
        type=int,
        default=5,
        help="Only keep spans with stack depth up to N (default: 5).",
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
        help="Maximum gap in ns to merge adjacent spans (default: 1000)"
    )

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
        help="Open the interactive figure",
    )
    parser.add_argument(
        "--write-to-html",
        type=Path,
        default=None,
        help="Write a standalone html file"
    )
    parser.set_defaults(func=cmd_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()