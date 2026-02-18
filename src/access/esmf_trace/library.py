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
        exact_paths: list[Path],
        model_component: str,
        branch_pattern: re.Pattern[str] | None = None,
        pets_components: list[str] | None = None,
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
        # core run list
        self.branches = branches
        self.exact_paths = [Path(p) for p in exact_paths]

        # defaults
        self.post_base_path = Path(post_base_path)
        self.model_component = model_component
        self.max_workers = max_workers

        # pet configuration
        self.branch_pattern = branch_pattern
        self.pets_components = list(pets_components) if pets_components is not None else None
        self.pets_prefix = pets_prefix

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

        if self.pets_components is not None and self.branch_pattern is None:
            raise ValueError("branch_pattern must be provided if pets_components is provided.")

    def _parse_layouts(self) -> list[dict[str, int]]:
        """
        Parse per branch layout values.

        This is only used if pets_components is provided,
        otherwise pets will be None and esmf-trace will use all pets in the traceout dir.

        It returns one dict per branch, with keys from the named capture groups in the regex pattern and int values.
        e.g.,
            branch_pattern captures: (?P<shared>\\d+), (?P<ocn>\\d+)
            branch: "..._shared_26_ocn_78" -> {"shared": 26, "ocn": 78}
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
        Build a PET string for a branch from a parsed layout value.

        eg with pets_components = ['shared', 'ocn'] and pets_prefix = "0"
        layout = {"shared": 26, "ocn": 78} -> "0,26,78"
        """
        # first element is the prefix
        parts = [self.pets_prefix]
        parts.extend(str(layout[comp]) for comp in self.pets_components)
        return ",".join(parts)

    def _build_pets_list(self) -> list[str]:
        """
        Return PET strings aligned with `branches`
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
        # pets are optional
        # If pets_components is None, pets will be None in the config,
        # and esmf-trace will use all pets in the traceout dir.
        pets_list = self._build_pets_list() if self.pets_components is not None else None

        runs: list[dict] = []
        for i, branch in enumerate(self.branches):
            run_item: dict = {
                "exact_path": str(self.exact_paths[i]),
                "base_prefix": branch,
            }
            if pets_list is not None:
                run_item["pets"] = pets_list[i]
            runs.append(run_item)

        return {
            "default_settings": {
                "post_base_path": str(self.post_base_path),
                "model_component": self.model_component,
                **self.default_settings,
            },
            "runs": runs,
        }


class ACCESSPostSummaryConfigBuilder:
    """
    Build an esmf-trace post-summary config dict for ACCESS-style workflows.
    """

    def __init__(
        self,
        post_base_path: str | Path,
        model_component: str | list[str] | None = None,
        pets: str | list | None = None,
        stats_start_index: int | None = None,
        stats_end_index: int | None = None,
        save_json_path: str | Path | None = None,
        timeseries_suffix: str = "_timeseries.json",
        default_overwrite: dict | None = None,
    ) -> None:
        """
        Same parameters as ACCESSRunConfigBuilder, but builds a config for post-summary instead of run.
        """
        self.post_base_path = Path(post_base_path)
        self.model_component = model_component
        self.pets = pets
        self.stats_start_index = stats_start_index
        self.stats_end_index = stats_end_index
        self.timeseries_suffix = timeseries_suffix
        self.save_json_path = Path(save_json_path) if save_json_path is not None else None
        self.default_overwrite = default_overwrite if default_overwrite is not None else {}

        self._validate()

    def _validate(self) -> None:
        if not str(self.post_base_path):
            raise ValueError("post_base_path must be a non-empty path string.")

    def build_config(self, runs: list[dict]) -> dict:
        """
        Build the post-summary config dict.

        minimum requirement per run:
            - {"name": "branch_name"}
        common fields all optional:
            - pets: "0, 52" or [0, 52]
            - model_component: list[str] or comma-separated str
            - output_index: "1,3-5,6" or [1,3,4,5,6]
            - stats_start_index: int
            - stats_end_index: int
            - save_json_path: str or Path, must end with .json
        """
        if not isinstance(runs, list) or len(runs) == 0:
            raise ValueError("At least one run must be provided.")

        default_settings: dict = {
            "post_base_path": str(self.post_base_path),
            "timeseries_suffix": self.timeseries_suffix,
        }

        if self.model_component is not None:
            default_settings["model_component"] = (
                self.model_component
                if isinstance(self.model_component, list)
                else [s.strip() for s in str(self.model_component).split(",") if s.strip()]
            )
        if self.pets is not None:
            default_settings["pets"] = (
                self.pets
                if isinstance(self.pets, list)
                else [s.strip() for s in str(self.pets).split(",") if s.strip()]
            )
        if self.stats_start_index is not None:
            default_settings["stats_start_index"] = self.stats_start_index
        if self.stats_end_index is not None:
            default_settings["stats_end_index"] = self.stats_end_index
        if self.save_json_path is not None:
            default_settings["save_json_path"] = str(self.save_json_path)

        default_settings.update(self.default_overwrite)

        return {
            "default_settings": default_settings,
            "runs": runs,
        }
