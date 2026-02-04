import argparse
from dataclasses import replace
from pathlib import Path

from .batch_runs import run_batch_jobs
from .config import DefaultSettings, load_config
from .postprocess import run_post_summary_from_yaml
from .tmp_yaml_parser import read_yaml


def _override_run_args(ns: argparse.Namespace) -> None:
    """
    Optional overrides from command line args to config settings.
    """
    arg = ns.add_argument_group("overrides", "Optional overrides to config settings")

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


def _apply_overrides(ns: argparse.Namespace, defaults: DefaultSettings) -> DefaultSettings:
    """
    Apply any command line overrides to the run defaults.
    """
    updates = {}

    # booleans only override when True provided
    if getattr(ns, "merge_adjacent", False):
        updates["merge_adjacent"] = True
    if getattr(ns, "xaxis_datetime", False):
        updates["xaxis_datetime"] = True
    if getattr(ns, "separate_plots", False):
        updates["separate_plots"] = True
    if getattr(ns, "show_html", False):
        updates["show_html"] = True

    # None means no override
    for f in [
        "stream_prefix",
        "model_component",
        "max_depth",
        "merge_gap_ns",
        "cmap",
        "renderer",
        "max_workers",
    ]:
        v = getattr(ns, f, None)
        if v is not None:
            updates[f] = v

    return replace(defaults, **updates) if updates else defaults


def _add_run_from_yaml_subparser(subparsers) -> None:
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
    _override_run_args(rs)

    rs.set_defaults(func=run_from_yaml_config)


def _add_post_summary_from_yaml_subparser(subparsers) -> None:
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

    arg = ps.add_argument_group("overrides", "Optional overrides to config settings")

    # Optional override
    arg.add_argument("--model-component", nargs="+", help="Full model_component name(s) to include.")
    arg.add_argument("--pets", nargs="+", type=int, help="PET index(es) to include.")
    arg.add_argument("--stats-start-index", type=int, help="Slice start (iloc) per series.")
    arg.add_argument(
        "--stats-end-index", type=int, help="Slice end (iloc, exclusive) per series. Default: full length."
    )
    arg.add_argument(
        "--timeseries-suffix",
        type=str,
        default="_timeseries.json",
        help="Timeseries filename suffix to match (default: _timeseries.json).",
    )
    arg.add_argument(
        "--save-json-path", type=Path, help="Save summary to json format file (otherwise prints to stdout)."
    )

    ps.set_defaults(func=run_post_summary_from_yaml)


def run_from_yaml_config(
    ns: argparse.Namespace,
) -> None:
    """
    Run multiple jobs from a yaml config file with optional command line overrides.
    """
    input_config = read_yaml(ns.config)
    defaults, runs = load_config(input_config)
    # overides
    defaults = _apply_overrides(ns, defaults)
    run_batch_jobs(defaults, runs)


def main():
    parser = argparse.ArgumentParser(
        prog="esmf-trace",
        description="ESMF traceout analysis and visualisation.",
    )

    subparsers = parser.add_subparsers(dest="cmd", required=True)

    _add_run_from_yaml_subparser(subparsers)
    _add_post_summary_from_yaml_subparser(subparsers)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
