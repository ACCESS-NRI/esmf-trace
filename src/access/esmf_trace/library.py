from dataclasses import replace
from pathlib import Path

from .batch_runs import run_batch_jobs
from .config import DefaultSettings, PostRunSettings, PostSummarySettings, RunSettings, load_yaml_config
from .postprocess import post_summary_from_yaml


def run_from_config(
    config_path: str | Path | dict,
    run_overrides: dict | None = None,
):
    """
    Either a yaml path or a dict with the same structure.

    run_overrides: optional dict of DefaultSettings field overrides
    e.g. {"stream_prefix": "esmf_stream", "max_workers": 8}
    """

    if isinstance(config_path, (str, Path)):
        defaults, runs = load_yaml_config(Path(config_path), kind="run")
    else:
        defaults = DefaultSettings(**config_path["default_settings"])
        runs = [RunSettings(**r) for r in config_path["runs"]]

    if run_overrides:
        defaults = replace(defaults, **dict(run_overrides))

    run_batch_jobs(defaults, runs)


def post_summary_from_config(
    config_path: str | Path | dict,
    post_overrides: dict | None = None,
    save_json_path: str | Path | None = None,
):
    """
    Either a yaml path or a dict with the same structure.

    post_overrides: optional dict of PostSummarySettings field overrides
    e.g. {"timeseries_suffix": "_timeseries.json", "stats_start_index": 1}
    """

    if isinstance(config_path, (str, Path)):
        defaults, runs = load_yaml_config(Path(config_path), kind="post-summary")
        assert isinstance(defaults, PostSummarySettings)
    else:
        defaults = PostSummarySettings(**config_path["default_settings"])
        runs = [PostRunSettings(**r) for r in config_path["runs"]]

    if post_overrides:
        defaults = replace(defaults, **dict(post_overrides))

    out_path = str(save_json_path) if save_json_path is not None else None
    post_summary_from_yaml(defaults, runs, save_json_path=out_path)
