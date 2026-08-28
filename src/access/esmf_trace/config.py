from dataclasses import dataclass, fields
from pathlib import Path
from typing import Literal, overload

from .common_vars import config_kind
from .tmp_yaml_parser import read_yaml
from .utils import extract_index_list_from_str, extract_pets


class ConfigError(Exception):
    pass


@dataclass(frozen=True)
class DefaultSettings:
    """
    The `default_settings:` block of a run config - values shared by every run
    in it. A run may override post_base_path and model_component; everything
    else here applies to the whole batch.

    post_base_path: root directory results are written under. See RunSettings
        for how the tree beneath it is laid out.
    stream_prefix: filename prefix of the per-PET CTF stream files inside each
        traceout dir, e.g. "esmf_stream" for esmf_stream_0000.
    model_component: default component selector(s) to keep in the timeseries,
        as a comma-separated string or a list.
    max_workers: number of worker processes. None falls back to the physical
        core count.
    max_depth: drop trace regions nested deeper than this.
    merge_adjacent, merge_gap_ns: merge consecutive spans of the same component
        separated by no more than merge_gap_ns nanoseconds.
    xaxis_datetime, separate_plots, cmap, renderer, show_html: flame graph
        options, passed straight through to plot_flame_graph.
    """

    post_base_path: str | None = None
    stream_prefix: str = "esmf_stream"
    model_component: str | list[str] = "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1"
    max_workers: int | None = None
    xaxis_datetime: bool = False
    separate_plots: bool = False
    cmap: str = "tab10"
    renderer: str = "browser"
    show_html: bool = False
    max_depth: int = 6
    merge_adjacent: bool = False
    merge_gap_ns: int = 1000


@dataclass(frozen=True)
class RunSettings:
    """
    One entry in a run config's `runs:` list - a single experiment to process.

    A run reads a trace tree and writes a postprocessed tree. The two sides are
    configured independently: nothing about the output location is derived from
    the input path, or the other way round.

        INPUT - the payu archive directory, given either as exact_path or
        assembled from run_base/run_name/branch/archive:

            <archive dir>/
              output000/traceout/esmf_stream_0000, esmf_stream_0001, ...
              output001/traceout/...

        OUTPUT - written under post_base_path and named by base_prefix:

            <post_base_path>/
              postprocessing_<base_prefix>/
                output000/
                  <base_prefix>_timeseries.json
                  <base_prefix>_flamegraph.html
                output001/
                  ...

    One job runs per outputNNN directory, so a run covering three outputs
    becomes three jobs writing three output subdirectories.

    base_prefix: label for this run's output tree - it names both the
        postprocessing_<base_prefix> directory and the files inside it. It
        plays no part in locating the input, so a run still needs it when
        exact_path is set. A post-summary config refers back to this run by the
        full directory name, i.e. its `name:` must be
        "postprocessing_<base_prefix>".
    post_base_path: output root for this run, overriding the config-wide
        DefaultSettings.post_base_path. Most configs set it once in
        default_settings and leave it unset here.
    exact_path: the archive directory to read, given directly.
    run_base, run_name, branch, archive: the alternative to exact_path, joined
        as run_base/run_name/branch/archive. `archive` defaults to "archive"
        and is only consulted in this form.
    pets: PET indices to read, as a range string like "0,13" or "0,3-5". None
        reads every stream file found in the traceout dir.
    model_component: component selector(s) to keep in the timeseries; falls
        back to DefaultSettings.model_component when unset.
    output_index: which outputNNN directories to process, as a range string
        like "0,2-4". None processes all of them.

    Either exact_path or all of run_base/run_name/branch must be set; if both
    forms are given, exact_path wins - see _resolve_exact_paths.
    """

    base_prefix: str | None = None
    post_base_path: str | None = None
    exact_path: Path | None = None
    run_base: Path | None = None
    run_name: str | None = None
    branch: str | None = None
    archive: str = "archive"
    pets: str | None = None
    model_component: str | list[str] | None = None
    output_index: str | None = None

    def _resolve_exact_paths(self) -> Path | None:
        """
        Return this run's INPUT archive directory, or None if underspecified.

        exact_path takes precedence: when it is set, run_base/run_name/branch/
        archive are ignored entirely. Otherwise all three of run_base, run_name
        and branch must be present and the path is joined as
        run_base/run_name/branch/archive.

        None means neither form was fully given. load_yaml_config rejects that
        up front, but the dict form of run_from_config does not, so
        run_batch_jobs checks again before using the result.
        """
        if self.exact_path:
            return Path(self.exact_path).expanduser().resolve()
        if self.run_base and self.run_name and self.branch:
            return Path(self.run_base) / self.run_name / self.branch / self.archive
        return None

    def _effective_post_base_path(self, defaults: DefaultSettings) -> Path:
        """
        Return the OUTPUT root for this run: its own post_base_path if set,
        otherwise the config-wide default. Expanded and resolved, so the result
        is absolute even when the config spelled it "~/x" or "../x".

        Every post_dir for this run hangs off this path, and progress messages
        are shown relative to it. defaults.post_base_path is the raw configured
        value and is not interchangeable with this.
        """
        return Path(self.post_base_path if self.post_base_path else defaults.post_base_path).expanduser().resolve()

    def normalised_model_component(self, defaults: DefaultSettings) -> str:
        """
        Return the component selector as the single comma-separated string
        run.run() expects, preferring this run's value over the config-wide
        default. A list is joined with commas.
        """
        mc = self.model_component if self.model_component is not None else defaults.model_component
        if isinstance(mc, list):
            return ",".join(mc)
        return mc

    def to_job_kwargs(
        self,
        defaults: DefaultSettings,
        traceout_path: Path,
        post_dir: Path,
    ) -> dict:
        """
        Produce the kwargs for one job, i.e. one outputNNN directory.

        traceout_path: the INPUT <outputNNN>/traceout dir holding the
            per-PET stream files.
        post_dir: the OUTPUT dir for this one job,
            <post_base_path>/postprocessing_<base_prefix>/<outputNNN>.

        base_prefix, pets and model_component are the only per-run values a
        worker sees; everything else is taken from the config-wide defaults.
        """
        return {
            "traceout_path": traceout_path,
            "base_prefix": self.base_prefix,
            "post_dir": post_dir,
            "pets": self.pets,
            "model_component": self.normalised_model_component(defaults),
            "merge_adjacent": defaults.merge_adjacent,
            "merge_gap_ns": defaults.merge_gap_ns,
            "max_depth": defaults.max_depth,
            "stream_prefix": defaults.stream_prefix,
            "xaxis_datetime": defaults.xaxis_datetime,
            "separate_plots": defaults.separate_plots,
            "cmap": defaults.cmap,
            "renderer": defaults.renderer,
            "show_html": defaults.show_html,
        }


@dataclass(frozen=True)
class PostSummarySettings:
    """
    Config-wide defaults for a post-summary run, produced by
    parse_post_summary_config() from `default_settings` (plus any CLI/library
    overrides). Individual PostRunSettings fall back to these values when a
    run doesn't set its own.

    post_base_path: root directory containing one subdirectory per case name.
    model_component: full component selector strings to keep; None keeps all.
    pets: PET indices to keep; None keeps all.
    stats_start_index, stats_end_index: iloc[start:end] slice applied per
        (case, output, component, pet) series before aggregating; None on
        both means no slicing.
    timeseries_suffix: filename suffix used to find timeseries JSON files
        under each output directory (e.g. "_timeseries.json").
    all_runs_summary_path: where to write the summary table spanning every
        run. Must end in ".json"; a sibling "<stem>_table.parquet" is
        written alongside it. None means don't save.
    include_combined: include the "combine" row per (case, component),
        pooling that case's outputs. This is a different axis from
        all_runs_summary_path, which stacks whole runs together.
    include_per_output: include one row per (case, output, component).
        At least one of include_combined/include_per_output must be true.
    """

    post_base_path: Path
    model_component: list[str] | None = None
    pets: list[int] | None = None
    stats_start_index: int | None = None
    stats_end_index: int | None = None
    timeseries_suffix: str = "_timeseries.json"
    all_runs_summary_path: Path | None = None
    include_combined: bool = True
    include_per_output: bool = True


@dataclass(frozen=True)
class PostRunSettings:
    """
    Settings for a single case/run within a post-summary config. Any field
    left unset (None) falls back to the corresponding PostSummarySettings
    default, except summary_path, which is opt-in per run and has no default
    to inherit - the config-wide all_runs_summary_path is a different
    destination, holding the table stacked across every run.

    name: case name; must match a subdirectory under post_base_path.
    output_index: which outputNNN directories to include; None means all.
    summary_path: if set, write this run's own summary rows to this path
        (in addition to it contributing to the all-runs table). Must end in
        ".json".
    """

    name: str
    output_index: list[int] | None = None
    model_component: list[str] | None = None
    pets: list[int] | None = None
    stats_start_index: int | None = None
    stats_end_index: int | None = None
    summary_path: Path | None = None


def _as_mapping(x, what: str) -> dict:
    if not isinstance(x, dict):
        raise ConfigError(f"{what} must be a mapping (dict)")
    return x


def _as_list(x, what: str) -> list:
    if not isinstance(x, list):
        raise ConfigError(f"{what} must be a list")
    return x


def _require_keys(d: dict, keys: list[str], where: str) -> None:
    missing = [k for k in keys if k not in d]
    if missing:
        raise ConfigError(f"missing required config key(s) in {where}: {', '.join(missing)}")


def _field_names(cls) -> set[str]:
    """
    Field names of a settings dataclass, i.e. the keys it accepts.
    """
    return {f.name for f in fields(cls)}


def _reject_unknown_keys(provided: dict, known: set[str], where: str) -> None:
    """
    Raise ConfigError for keys that aren't recognised, so typos fail loudly
    instead of being silently dropped.
    """
    unknown = set(provided) - known
    if unknown:
        raise ConfigError(
            f"unknown config key(s) in {where}: {', '.join(sorted(unknown))} (valid keys: {', '.join(sorted(known))})"
        )


def _norm_model_component(v: str | list | tuple | set | None) -> list[str] | None:
    """
    Normalise model_component to a list of strings.
    Accepts a comma-separated str or a list[str].
    """
    if v is None:
        return None

    if isinstance(v, (list, tuple, set)):
        parts = [str(x).strip() for x in v if str(x).strip()]
        return parts or None

    s = str(v).strip()
    if not s:
        return None

    # split on commas
    parts = [p.strip() for p in s.split(",") if p.strip()]
    return parts or None


def _norm_int_or_none(v: int | str | None) -> int | None:
    if v is None or v == "":
        return None
    return int(v)


def _norm_path_or_none(v: str | Path | None) -> Path | None:
    if v is None:
        return None
    return Path(v).expanduser()


def _norm_pets(v: int | str | list[int] | tuple[int, ...] | set[int] | None) -> list[int] | None:
    """
    Normalise a pets value to a sorted list of unique ints, or None (all pets).

    Accepts a single int, a list/tuple/set of ints, or a range string like
    "0,3-5" (delegated to extract_pets). e.g. 3 -> [3]; [3, 1, 1] -> [1, 3];
    "0,3-5" -> [0, 3, 4, 5].
    """
    if v is None:
        return None
    if isinstance(v, int):
        return [v]
    if isinstance(v, (list, tuple, set)):
        return sorted({int(pet) for pet in v})
    return extract_pets(v)


def parse_post_summary_config(
    data: dict,
    default_overrides: dict | None = None,
) -> tuple[PostSummarySettings, list[PostRunSettings]]:
    """
    Validate and normalise a post-summary config (already-loaded dict, e.g.
    from YAML) into a (PostSummarySettings, list[PostRunSettings]) pair.

    data: must contain "default_settings" (mapping) and "runs" (list of
        mappings, each requiring at least a "name").
    default_overrides: field values (e.g. from the CLI or a library caller)
        that take precedence over both the configured defaults and any
        per-run value for that same field - for every run, not just where
        the run itself left the field unset.

    Raises ConfigError if required keys are missing, any unrecognised key is
    supplied (in default_settings, a run entry, or default_overrides),
    post_base_path isn't set, or both include_combined and include_per_output
    are false.
    """
    _require_keys(data, ["default_settings", "runs"], where="config")
    configured_default = dict(_as_mapping(data["default_settings"], what="default_settings"))
    runs = _as_list(data["runs"], what="runs")
    overrides = dict(default_overrides or {})

    _reject_unknown_keys(configured_default, _field_names(PostSummarySettings), "default_settings")
    _reject_unknown_keys(overrides, _field_names(PostSummarySettings), "default_overrides")

    configured_default.update(overrides)

    post_base = configured_default.get("post_base_path")
    if not post_base:
        raise ConfigError("default_settings.post_base_path is required for post-summary config")

    defaults = PostSummarySettings(
        post_base_path=Path(post_base).expanduser(),
        model_component=_norm_model_component(configured_default.get("model_component")),
        pets=_norm_pets(configured_default.get("pets")),
        stats_start_index=_norm_int_or_none(configured_default.get("stats_start_index")),
        stats_end_index=_norm_int_or_none(configured_default.get("stats_end_index")),
        timeseries_suffix=configured_default.get("timeseries_suffix", "_timeseries.json"),
        all_runs_summary_path=_norm_path_or_none(configured_default.get("all_runs_summary_path")),
        include_combined=bool(configured_default.get("include_combined", True)),
        include_per_output=bool(configured_default.get("include_per_output", True)),
    )
    if not defaults.include_combined and not defaults.include_per_output:
        raise ConfigError("At least one of include_combined or include_per_output must be true")

    def selected(item: dict, key: str, default_value):
        # An explicit library/CLI override applies to every run. Otherwise a
        # per-run value takes precedence over the configured default.
        if key in overrides:
            return overrides[key]
        return item.get(key, default_value)

    post_runs: list[PostRunSettings] = []
    for i, raw_item in enumerate(runs):
        item = _as_mapping(raw_item, what=f"runs[{i}]")
        _require_keys(item, ["name"], where=f"runs[{i}]")
        _reject_unknown_keys(item, _field_names(PostRunSettings), f"runs[{i}]")

        oi = item.get("output_index")
        if isinstance(oi, list):
            output_index = [int(x) for x in oi]
        elif isinstance(oi, str):
            output_index = extract_index_list_from_str(oi)
        else:
            output_index = None

        pets_input = selected(item, "pets", defaults.pets)
        pets = _norm_pets(pets_input)

        post_runs.append(
            PostRunSettings(
                name=str(item["name"]),
                output_index=output_index,
                model_component=_norm_model_component(selected(item, "model_component", defaults.model_component)),
                pets=pets,
                stats_start_index=_norm_int_or_none(selected(item, "stats_start_index", defaults.stats_start_index)),
                stats_end_index=_norm_int_or_none(selected(item, "stats_end_index", defaults.stats_end_index)),
                # all_runs_summary_path is a different destination (the table
                # across all runs), so there is nothing to inherit here:
                # per-run output is opt-in and must be declared on that run.
                summary_path=_norm_path_or_none(item.get("summary_path")),
            )
        )

    return defaults, post_runs


def load_post_summary_config(
    config: str | Path | dict,
    default_overrides: dict | None = None,
) -> tuple[PostSummarySettings, list[PostRunSettings]]:
    """
    Load a post-summary config from a YAML file path or an equivalent dict,
    then validate and normalise it via parse_post_summary_config().

    config: path to a YAML file, or a dict with the same
        {"default_settings": ..., "runs": [...]} structure.
    default_overrides: see parse_post_summary_config().
    """
    data = read_yaml(Path(config)) if isinstance(config, (str, Path)) else config
    return parse_post_summary_config(data, default_overrides=default_overrides)


# define overloads for type checking of load_yaml_config
@overload
def load_yaml_config(config_path: Path, kind: Literal["run"]) -> (DefaultSettings, list[RunSettings]): ...


@overload
def load_yaml_config(
    config_path: Path, kind: Literal["post-summary"]
) -> (PostSummarySettings, list[PostRunSettings]): ...


def load_yaml_config(config_path: Path, kind: config_kind):
    """
    Load and validate an esmf-trace yaml configuration file.
    """
    config_path = Path(config_path)
    data = read_yaml(config_path)

    if kind == "post-summary":
        return parse_post_summary_config(data)

    _require_keys(data, ["default_settings", "runs"], where=str(config_path))
    default = _as_mapping(data["default_settings"], what="default_settings")
    runs = _as_list(data["runs"], what="runs")

    if kind == "run":
        defaults = DefaultSettings(
            post_base_path=default.get("post_base_path"),
            stream_prefix=default.get("stream_prefix", "esmf_stream"),
            model_component=default.get("model_component", "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1"),
            max_workers=default.get("max_workers"),
            xaxis_datetime=bool(default.get("xaxis_datetime", False)),
            separate_plots=bool(default.get("separate_plots", False)),
            cmap=default.get("cmap", "tab10"),
            renderer=default.get("renderer", "browser"),
            show_html=bool(default.get("show_html", False)),
            max_depth=int(default.get("max_depth", 6)),
            merge_adjacent=bool(default.get("merge_adjacent", False)),
            merge_gap_ns=int(default.get("merge_gap_ns", 1000)),
        )

        run_settings: list[RunSettings] = []
        for i, item in enumerate(runs):
            item = _as_mapping(item, what=f"runs[{i}]")

            has_exact_path = item.get("exact_path")
            has_other_parts = item.get("run_base") and item.get("run_name") and item.get("branch")
            if not has_exact_path and not has_other_parts:
                raise ConfigError(
                    "Each run must have either 'exact_path' or "
                    f"all of 'run_base', 'run_name', and 'branch' set (error in runs[{i}])"
                )

            run_settings.append(
                RunSettings(
                    base_prefix=item.get("base_prefix"),
                    post_base_path=item.get("post_base_path"),
                    exact_path=_norm_path_or_none(item.get("exact_path") if item.get("exact_path") else None),
                    run_base=_norm_path_or_none(item.get("run_base") if item.get("run_base") else None),
                    run_name=item.get("run_name"),
                    branch=item.get("branch"),
                    archive=item.get("archive", "archive"),
                    pets=item.get("pets"),
                    model_component=item.get("model_component"),
                    output_index=item.get("output_index"),
                )
            )

        return defaults, run_settings

    raise ValueError(f"Invalid config kind: {kind}")
