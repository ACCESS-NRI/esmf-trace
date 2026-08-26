import re
from dataclasses import replace
from pathlib import Path

from .batch_runs import run_batch_jobs
from .config import DefaultSettings, RunSettings, load_post_summary_config, load_yaml_config
from .postprocess import post_summary_from_yaml
from .utils import normalise_str_list


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
    all_runs_summary_path: str | Path | None = None,
    include_combined: bool | None = None,
    include_per_output: bool | None = None,
):
    """
    Load a post-summary config and build the combined summary table from it.

    config_path: either a yaml path or a dict with the same structure.
    post_overrides: optional dict of PostSummarySettings field overrides,
        applied to every run (not just where a run itself left the field
        unset) - e.g. {"timeseries_suffix": "_timeseries.json",
        "stats_start_index": 1}. Unrecognised keys raise ConfigError.
    all_runs_summary_path: where to write the summary table spanning every
        run. Must end in ".json"; a sibling "<stem>_table.parquet" is
        written alongside it. Overrides the config's own value if given.
    include_combined: include the rows pooled across selected outputs.
    include_per_output: include one row per selected output.

    Returns the combined summary as a DataFrame (see
    postprocess.post_summary_from_yaml).
    """

    defaults, runs = load_post_summary_config(config_path, default_overrides=post_overrides)
    return post_summary_from_yaml(
        defaults,
        runs,
        all_runs_summary_path=all_runs_summary_path,
        include_combined=include_combined,
        include_per_output=include_per_output,
    )


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
        model_component: str | list[str],
        branch_pattern: re.Pattern[str] | None = None,
        pets_components: list[str] | None = None,
        pets_prefix: str = "0",
        max_workers: int = 4,
        default_overwrite: dict | None = None,
    ) -> None:
        """
        Parameters:
        branches: Experiment branch directory names; each string must match the regex provided in branch_pattern
        post_base_path: where esmf-trace writes postprocessed outputs for this config
        exact_paths: list of exact paths for each branch
        model_component: comma-separated esmf component selector string or list[str] of selectors
        branch_pattern: regex pattern to parse layout values, with capture groups for each layout variable
        pets_components: list[str], keys to include in pets string in order
        pets_prefix: str, prefix for pets string (default "0")
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

        if not normalise_str_list(self.model_component):
            raise ValueError("model_component must be a non-empty string or list[str].")

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
                "model_component": normalise_str_list(self.model_component),
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
        pets: str | list[int] | None = None,
        stats_start_index: int | None = None,
        stats_end_index: int | None = None,
        all_runs_summary_path: str | Path | None = None,
        timeseries_suffix: str = "_timeseries.json",
        include_combined: bool = True,
        include_per_output: bool = True,
        default_overwrite: dict | None = None,
    ) -> None:
        """
        Initialise a builder for esmf-trace post-summary configuration for ACCESS-style workflows.

        post_base_path: root directory containing one subdirectory per case name.
        model_component, pets, stats_start_index, stats_end_index,
            all_runs_summary_path, timeseries_suffix: become the corresponding
            default_settings entries in the built config (see build_config());
            each is applied to every run unless a run dict overrides it
            directly. all_runs_summary_path is the exception - it is the
            across-all-runs destination and has no per-run counterpart to
            inherit; a run opts in separately via its own summary_path.
        include_combined: include the pooled-across-outputs "combine" row.
        include_per_output: include one row per output. At least one of
            include_combined/include_per_output must be true.
        default_overwrite: extra key/value pairs merged into default_settings
            last, so they take precedence over every field above (e.g.
            {"timeseries_suffix": "_custom.json"}).

        Raises ValueError if post_base_path is empty, or if both
        include_combined and include_per_output are false.
        """
        self.post_base_path = Path(post_base_path)
        self.model_component = model_component
        self.pets = pets
        self.stats_start_index = stats_start_index
        self.stats_end_index = stats_end_index
        self.timeseries_suffix = timeseries_suffix
        self.all_runs_summary_path = Path(all_runs_summary_path) if all_runs_summary_path is not None else None
        self.include_combined = include_combined
        self.include_per_output = include_per_output
        self.default_overwrite = default_overwrite if default_overwrite is not None else {}

        self._validate()

    def _validate(self) -> None:
        """Raise ValueError if post_base_path is empty, or both include flags are false."""
        if not str(self.post_base_path):
            raise ValueError("post_base_path must be a non-empty path string.")
        if not self.include_combined and not self.include_per_output:
            raise ValueError("At least one of include_combined or include_per_output must be true.")

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
            - summary_path: str or Path, must end with .json; writes this
              run's own rows, separate from all_runs_summary_path

        The default_settings block always carries include_combined and
        include_per_output (from the constructor args), in addition to
        post_base_path and timeseries_suffix.

        Raises ValueError if `runs` is empty.
        """
        if not isinstance(runs, list) or len(runs) == 0:
            raise ValueError("At least one run must be provided.")

        default_settings: dict = {
            "post_base_path": str(self.post_base_path),
            "timeseries_suffix": self.timeseries_suffix,
            "include_combined": self.include_combined,
            "include_per_output": self.include_per_output,
        }

        default_settings["model_component"] = normalise_str_list(self.model_component)

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
        if self.all_runs_summary_path is not None:
            default_settings["all_runs_summary_path"] = str(self.all_runs_summary_path)

        default_settings.update(self.default_overwrite)

        return {
            "default_settings": default_settings,
            "runs": runs,
        }
