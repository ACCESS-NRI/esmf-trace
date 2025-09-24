import argparse
from pathlib import Path
import pandas as pd
from extract_timing import df_for_selected_streams
from plotting import plot_flame_graph

def _parse_pets(pets_str: str | None) -> int | list[int] | None:
    """
    Input can be 0,3-5,8
    """
    if pets_str is None:
        return None
    pets = []
    for part in pets_str.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            start, end = part.split("-", 1)
            pets.extend(range(int(start), int(end)+1))
        else:
            pets.append(int(part))
    return sorted(set(pets))

def _read_streams_arg(streams_path_input: Path) -> list[str]:
    paths = []
    for p in streams_path_input.parent.glob(streams_path_input.name):
        paths.append(str(p))
    sort_paths = sorted(set(paths))
    if not sort_paths:
        raise ValueError("no stream paths provided!")
    return sort_paths

def _write_df(df: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".csv":
        df.to_csv(output_path, index=False)
    else:
        raise ValueError(f"unsupported output file type: {output_path.suffix}"
)

def _plot_html(
    df: pd.DataFrame,
    pets: int | list[int] | None,
    *,
    as_datetime: bool,
    overlay: bool,
    cmap: str,
    renderer: str | None,
    out_html: Path,
) -> None:
    out_html.parent.mkdir(parents=True, exist_ok=True)
    plot_flame_graph(
        df,
        pets=pets,
        as_datetime=as_datetime,
        separate_plots=not overlay,
        cmap_name=cmap,
        renderer=renderer,
        output_html=out_html,
    )
    print(f"wrote HTML to {out_html}")

def cmd_run(args):
    stream_paths = _read_streams_arg(args.streams_path)
    pets = _parse_pets(args.pets)
    df = df_for_selected_streams(
        trace_root=args.trace_root,
        stream_paths=stream_paths,
        pets=pets,
        merge_adjacent=not args.no_merge,
        merge_gap_ns=args.merge_gap_ns,
    )

    if df.empty:
        print("-- No rows parsed (empty DataFrame). Nothing to do.")
        return

    did_anything = False

    if args.output is not None:
        _write_df(df, args.output)
        did_anything = True

    if args.out_html is not None:
        _plot_html(
            df,
            pets,
            as_datetime=args.as_datetime,
            overlay=args.overlay,
            cmap=args.cmap,
            renderer=args.renderer,
            out_html=args.out_html,
        )
        did_anything = True

def main():
    parser = argparse.ArgumentParser(
        prog="esmf-trace",
        description="ESMF traceout analysis and visualisation."
        )
    subparsers = parser.add_subparsers(dest="command", required=True)

    # df command
    run_parser = subparsers.add_parser(
        "df", 
        help="Extract timing data to a DataFrame (CSV)"
    )
    run_parser.add_argument(
        "trace_root",
        type=Path,
        help="Path to the trace root directory"
    )
    run_parser.add_argument(
        "streams_path",
        type=Path,
        help="Path for stream file paths (e.g. '/path/to/streams/stream*.ctf')"
    )
    run_parser.add_argument(
        "-p",
        "--pets",
        type=str,
        default=None,
        help="Comma-separated list of pets or ranges (e.g. '0,3-5,8')"
    )
    run_parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("timing_data.csv"),
        help="Output CSV file path"
    )
    run_parser.add_argument(
        "--no-merge",
        action="store_true",
        help="Do not merge adjacent spans"
    )
    run_parser.add_argument(
        "--merge-gap-ns",
        type=int,
        default=1000,
        help="Maximum gap in ns to merge adjacent spans (default: 1000)"
    )

    run_parser.add_argument(
        "--as-datetime", action="store_true", help="Use wall-clock timestamps on x-axis"
    )
    run_parser.add_argument(
        "--overlay", action="store_true", help="Overlay PETs in one panel"
    )
    run_parser.add_argument(
        "--cmap", default="tab20", help="Matplotlib colormap name (default: tab20)"
    )
    run_parser.add_argument(
        "--renderer", default=None, help="Plotly renderer (e.g. 'browser')"
    )
    run_parser.add_argument(
        "--out-html", type=Path, default=None, help="Write a standalone html file"
    )

    run_parser.set_defaults(func=cmd_run)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()