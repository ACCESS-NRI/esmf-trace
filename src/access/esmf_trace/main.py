import argparse
from pathlib import Path

from .common_vars import POST_SUMMARY_DEFAULT_KEYS, RUN_DEFAULT_FLAG_KEYS, RUN_DEFAULT_KEYS
from .library import post_summary_from_config, run_from_config

EXIT_OK = 0
# At least one job did not produce its outputs. Config and setup problems raise
# instead, so they surface as a traceback rather than this code.
EXIT_JOB_FAILED = 1


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
    arg.add_argument(
        "--force",
        action="store_true",
        help=(
            "Reprocess every job, even where the outputs already exist and were produced"
            " by the same settings (default: False)"
        ),
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
    arg.add_argument(
        "--all-runs-summary-path",
        type=Path,
        help="Write the summary spanning every run to this path (.json; a sibling parquet is written too).",
    )
    arg.add_argument(
        "--include-combined",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Include rows pooled across selected outputs (default: true).",
    )
    arg.add_argument(
        "--include-per-output",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Include one row per selected output (default: true).",
    )


def _apply_post_summary_overrides(ns: argparse.Namespace) -> dict:
    """
    Collect the post-summary override dict from parsed CLI args.

    Only includes a key for each field in POST_SUMMARY_DEFAULT_KEYS (which
    now includes include_combined/include_per_output) that the user actually
    set - the BooleanOptionalAction flags default to None (unset) so a
    caller can tell "not passed" apart from an explicit False. Passed
    straight to parse_post_summary_config()'s default_overrides, which apply
    to every run.
    """
    overrides = {}

    for f in POST_SUMMARY_DEFAULT_KEYS:
        v = getattr(ns, f, None)
        if v is not None:
            if f == "all_runs_summary_path" and isinstance(v, Path):
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
) -> int:
    """
    Run multiple jobs from a yaml config file with optional command line overrides.

    Returns the process exit code: non-zero if any job failed, so a wrapper
    script or CI step does not read a failed batch as a success.
    """
    result = run_from_config(ns.config, run_overrides=_apply_run_overrides(ns))
    return EXIT_OK if result.ok else EXIT_JOB_FAILED


def cli_post_summary_from_yaml(
    ns: argparse.Namespace,
) -> int:
    """
    Summarise existing e.g. *_timeseries.json files by reading a yaml file that lists:
      - post_base_path
      - cases: [{ name: postprocessing_<case>, output_index: [optional list of ints] }, ...]
    """
    post_summary_from_config(
        ns.config,
        post_overrides=_apply_post_summary_overrides(ns),
        all_runs_summary_path=ns.all_runs_summary_path,
    )
    return EXIT_OK


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


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    raise SystemExit(args.func(args))


if __name__ == "__main__":
    main()
