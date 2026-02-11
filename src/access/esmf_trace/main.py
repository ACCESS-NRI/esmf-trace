import argparse
from pathlib import Path

from .common_vars import RUN_DEFAULT_FLAG_KEYS, RUN_DEFAULT_KEYS, POST_SUMMARY_DEFAULT_KEYS
from .library import run_from_config, post_summary_from_config


def _add_run_overrides(parser: argparse.ArgumentParser) -> None:
    """
    Optional overrides from command line args to config settings.
    """
    arg = parser.add_argument_group("overrides", "Optional overrides to config settings")

    arg.add_argument(
        "--stream-prefix",
        type=str,
        help="Override the stream file prefix from config (default: esmf_stream)",
    )
    arg.add_argument(
        "--model-component",
        type=str,
        help=(
            "Override the model component filter from config"
            " (default: '[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1')"
        ),
    )
    arg.add_argument(
        "--max-depth",
        type=int,
        help="Override the max depth filter from config (default: 6)",
    )
    arg.add_argument(
        "--merge-adjacent",
        action="store_true",
        help="Override to enable merging of adjacent events from config (default: False)",
    )
    arg.add_argument(
        "--merge-gap-ns",
        type=int,
        help="Override the gap threshold (in ns) for merging adjacent events from config (default: 1000)",
    )
    arg.add_argument(
        "--xaxis-datetime",
        action="store_true",
        help="Override to enable datetime x-axis in flame graph from config (default: False)",
    )
    arg.add_argument(
        "--separate-plots",
        action="store_true",
        help="Override to enable separate flame graph plots per pet from config (default: False)",
    )
    arg.add_argument(
        "--cmap",
        type=str,
        help="Override the matplotlib colormap for flame graph from config (default: tab10)",
    )
    arg.add_argument(
        "--renderer",
        type=str,
        help="Override the plotly renderer for flame graph from config (default: browser)",
    )
    arg.add_argument(
        "--show-html",
        action="store_true",
        help="Override to open the flame graph html in a browser after generation (default: False)",
    )
    arg.add_argument(
        "--max-workers",
        type=int,
        help="Override the maximum number of workers for parallel processing from config (default: number of CPUs)",
    )


def _apply_run_overrides(ns: argparse.Namespace) -> dict:
    """
    Apply any command line overrides to the run defaults.
    """
    overrides = {}

    # booleans only override when True provided
    for flag in RUN_DEFAULT_FLAG_KEYS:
        if getattr(ns, flag, False):
            overrides[flag] = True

    # None means no override
    for f in RUN_DEFAULT_KEYS:
        v = getattr(ns, f, None)
        if v is not None:
            overrides[f] = v

    return overrides


def _add_post_summary_overrides(parser: argparse.ArgumentParser) -> None:
    """
    Add optional override arguments for the post-summary-from-yaml command.
    """
    arg = parser.add_argument_group("overrides", "Optional overrides to config settings")

    arg.add_argument("--model-component", nargs="+", help="Full model_component name(s) to include.")
    arg.add_argument("--pets", nargs="+", type=int, help="PET index(es) to include.")
    arg.add_argument("--stats-start-index", type=int, help="Slice start (iloc) per series.")
    arg.add_argument("--stats-end-index", type=int, help="Slice end (iloc, exclusive) per series.")
    arg.add_argument(
        "--timeseries-suffix", type=str, help="Timeseries filename suffix to match (e.g., _timeseries.json)."
    )
    arg.add_argument("--save-json-path", type=Path, help="Save combined summary JSON to this path.")


def _apply_post_summary_overrides(ns: argparse.Namespace) -> dict:
    overrides = {}

    for f in POST_SUMMARY_DEFAULT_KEYS:
        v = getattr(ns, f, None)
        if v is not None:
            if f == "save_json_path" and isinstance(v, Path):
                v = str(v)
            overrides[f] = v

    return overrides


def _add_run_command(subparsers) -> None:
    """
    run-from-yaml:
      Process multiple traceout directories from a yaml config file
    """
    rs = subparsers.add_parser(
        "run-from-yaml",
        help="Process multiple traceout directories from a yaml config file",
    )

    rs.add_argument(
        "--config",
        type=Path,
        required=True,
        help="Path to the yaml config file defining multiple runs",
    )

    # Optional overrides
    _add_run_overrides(rs)

    rs.set_defaults(func=cli_run_from_yaml)


def _add_post_summary_command(subparsers) -> None:
    """
    post-summary-from-yaml:
      Summarise existing *_timeseries.json files by reading a YAML file that lists:
        - post_base_path
        - cases: [{ name: postprocessing_<case>, output_index: [optional list of ints] }, ...]
    """
    ps = subparsers.add_parser(
        "post-summary-from-yaml",
        help="Summarise *_timeseries.json for cases listed in a YAML file (JSON output).",
    )

    # yaml config for postprocessing summary
    ps.add_argument(
        "--config",
        type=Path,
        required=True,
        help="yaml config file for postprocessing summary",
    )

    # Optional overrides
    _add_post_summary_overrides(ps)

    ps.set_defaults(func=cli_post_summary_from_yaml)


def cli_run_from_yaml(
    ns: argparse.Namespace,
) -> None:
    """
    Run multiple jobs from a yaml config file with optional command line overrides.
    """
    run_from_config(ns.config, run_overrides=_apply_run_overrides(ns))


def cli_post_summary_from_yaml(
    ns: argparse.Namespace,
) -> None:
    """
    Summarise existing e.g. *_timeseries.json files by reading a yaml file that lists:
      - post_base_path
      - cases: [{ name: postprocessing_<case>, output_index: [optional list of ints] }, ...]
    """
    post_summary_from_config(
        ns.config,
        post_overrides=_apply_post_summary_overrides(ns),
        save_json_path=ns.save_json_path,
    )


def build_parser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser.
    """
    parser = argparse.ArgumentParser(
        prog="esmf-trace",
        description="ESMF traceout analysis and visualisation.",
    )
    subparsers = parser.add_subparsers(dest="cmd", required=True)
    _add_run_command(subparsers)
    _add_post_summary_command(subparsers)
    return parser


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
