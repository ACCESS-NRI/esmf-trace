import re
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


class ACCESSRunConfigBuilder:
    """
    Build an esmf-trace run-config dict for ACCESS-style workflows.
    """

    DEFAULT_SETTINGS: dict = {
        "stream_prefix": "esmf_stream",
        "xaxis_datetime": False,
        "separate_plots": False,
        "cmap": "tab10",
        "renderer": "browser",
        "show_html": False,
    }

    def __init__(
        self,
        branches: list[str],
        post_base_path: str | Path,
        exact_paths: list[str],
        model_component: str,
        branch_pattern: re.Pattern[str],
        pets_components: list[str],
        pets_prefix: str = "0",
        max_workers: int = 4,
        default_overwrite: dict | None = None,
    ) -> None:
        """
        Parameters:
        branches: Experiment branch directory names; Each string must match layout["pattern"]
        post_base_path: where esmf-trace writes postprocessed outputs for this config
        exact_paths: list of exact paths for each branch
        model_component: comma-separated esmf component selector string.
        branch_pattern: regex pattern to parse layout values, with capture groups for each layout variable
        pets_components: list[str], keys to include in pets string in order
        pets_prefix: str | None, prefix for pets string (default "0")
        max_workers: number of parallel workers to use for postprocessing default 4 for login nodes
        default_overwrite: Extra keys to merge into default_settings (eg {"timeseries_suffix": "_timeseries.json"}).
        """
        self.branches = branches
        self.post_base_path = Path(post_base_path)

        self.model_component = model_component
        self.max_workers = max_workers

        self.branch_pattern = branch_pattern
        self.pets_components = pets_components
        self.pets_prefix = pets_prefix

        self.exact_paths = exact_paths

        # default_settings
        self.default_settings = dict(self.DEFAULT_SETTINGS)
        if default_overwrite:
            self.default_settings.update(default_overwrite)
        self.default_settings["max_workers"] = self.max_workers

        self._validate()

    def _validate(self) -> None:
        if not self.branches:
            raise ValueError("At least one branch must be provided.")

        if not isinstance(self.model_component, str) or not self.model_component:
            raise ValueError("model_component must be a non-empty string.")

        if not isinstance(self.max_workers, int) or self.max_workers < 1:
            raise ValueError("max_workers must be an int >= 1")

        if self.branch_pattern is None:
            raise ValueError("branch_pattern must be provided with a regex pattern string.")

        if self.pets_components is None:
            raise ValueError("pets_components must be provided, (e.g. ['shared','ocn'])")

    def _parse_layouts(self) -> list[dict[str, int]]:
        """
        Parse per branch layout values

        It returns one dict per branch, with keys from the named capture groups in the regex pattern and int values.
        e.g.,
            branch = "..._shared_26_ocn_78" -> {"shared": 26, "ocn": 78}
        """
        # Collect one dict per branch
        layouts: list[dict[str, int]] = []

        for branch in self.branches:
            match = self.branch_pattern.search(branch)
            if not match:
                raise ValueError(f"Branch name '{branch}' does not match the layout pattern.")

            # layout extracted from this branch
            layout = {name: int(value) for name, value in match.groupdict().items()}
            layouts.append(layout)

        return layouts

    def _pets_for_layout(self, layout: dict[str, int]) -> str:
        """
        Build pets string for a branch from the parsed layout values.

        eg with pets_components = ['shared', 'ocn'] and pets_prefix = "0"
        layout = {"shared": 26, "ocn": 78} -> "0,26,78"
        """
        # first element is the prefix
        parts = [self.pets_prefix]
        parts.extend(str(layout[comp]) for comp in self.pets_components)
        return ",".join(parts)

    def _pet_list(self) -> list[str]:
        """
        Return the per-run PET string aligned with `branches`
        """
        if self.pets_components is None:
            raise ValueError("pets_components must be provided to build pets strings.")

        layouts = self._parse_layouts()
        return [self._pets_for_layout(layout) for layout in layouts]

    def build_config(self) -> dict:
        """
        Build the config dict for esmf-trace from the provided information.

        Output format:
            {
                "default_settings": {..},
                "runs": [
                    {
                        "exact_path": "path/to/traceout",
                        "base_prefix": "branch_name",
                        "pets": "0,26,78",
                    },
                    ...
            }
        """
        paths = self.exact_paths

        # pets are optional; if configured, compute them otherwise leave them out
        pets = self._pet_list() if self.pets_components is not None else None

        runs: list[dict] = []
        for i, branch in enumerate(self.branches):
            run_item: dict = {
                "exact_path": paths[i],
                "base_prefix": branch,
            }
            if pets is not None:
                run_item["pets"] = pets[i]
            runs.append(run_item)

        config = {
            "default_settings": {
                "post_base_path": str(self.post_base_path),
                "model_component": self.model_component,
                **self.default_settings,
            },
            "runs": runs,
        }

        return config
