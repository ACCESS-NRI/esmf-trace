from dataclasses import dataclass
from pathlib import Path


class ConfigError(Exception):
    pass

@dataclass(frozen=True)
class DefaultSettings:
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
        Return the exact dir for this run
            - if exact_path is set, use that.
            - else if run_base, run_name, branch are set, construct the path as:
                run_base / run_name / branch / archive
        """
        if self.exact_path:
            return Path(self.exact_path).expanduser().resolve()
        if self.run_base and self.run_name and self.branch:
            return Path(self.run_base) / self.run_name / self.branch / self.archive
        return None

    def _effective_post_base_path(self, defaults: DefaultSettings) -> Path:
        return Path(self.post_base_path if self.post_base_path else defaults.post_base_path).expanduser().resolve()

    def normalised_model_component(self, defaults: DefaultSettings) -> str:
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
        Produce kwargs for the single run
        """
        return dict(
            traceout_path=traceout_path,
            base_prefix=self.base_prefix,
            post_dir=post_dir,
            pets=self.pets,
            model_component=self.normalised_model_component(defaults),
            merge_adjacent=defaults.merge_adjacent,
            merge_gap_ns=defaults.merge_gap_ns,
            max_depth=defaults.max_depth,
            stream_prefix=defaults.stream_prefix,
            xaxis_datetime=defaults.xaxis_datetime,
            separate_plots=defaults.separate_plots,
            cmap=defaults.cmap,
            renderer=defaults.renderer,
            show_html=defaults.show_html,
        )

def _require_key(d: dict, keys: list[str]) -> str:
    missing = [k for k in keys if k not in d]
    if missing:
        raise ConfigError(f"missing required config key(s): {', '.join(missing)}")

def _parse_defaults(d: dict) -> DefaultSettings:
    return DefaultSettings(
        post_base_path=d.get("post_base_path"),
        stream_prefix=d.get("stream_prefix", "esmf_stream"),
        model_component=d.get("model_component", "[ESMF]/[ensemble] RunPhase1/[ESM0001] RunPhase1"),
        max_workers=d.get("max_workers"),
        xaxis_datetime=bool(d.get("xaxis_datetime", False)),
        separate_plots=bool(d.get("separate_plots", False)),
        cmap=d.get("cmap", "tab10"),
        renderer=d.get("renderer", "browser"),
        show_html=bool(d.get("show_html", False)),
        max_depth=int(d.get("max_depth", 6)),
        merge_adjacent=bool(d.get("merge_adjacent", False)),
        merge_gap_ns=int(d.get("merge_gap_ns", 1000)),
    )

def _parse_runs(lst: list[dict]) -> list[RunSettings]:
    runs = []
    for l in lst:
        if not isinstance(l, dict):
            raise ConfigError("Each run must be a mapping (dict)")

        has_exact_path = l.get("exact_path")
        has_other_parts = l.get("run_base") and l.get("run_name") and l.get("branch")
        if not has_exact_path and not has_other_parts:
            raise ConfigError("Each run must have either 'exact_path' or all of 'run_base', 'run_name', and 'branch' set")

        runs.append(
            RunSettings(
                base_prefix=l.get("base_prefix"),
                post_base_path=l.get("post_base_path"),
                exact_path=Path(l["exact_path"]) if l.get("exact_path") else None,
                run_base=Path(l["run_base"]) if l.get("run_base") else None,
                run_name=l.get("run_name"),
                branch=l.get("branch"),
                pets=l.get("pets"),
                model_component=l.get("model_component"),
                output_index=l.get("output_index"),
            )
        )
    return runs

def load_config(input_config: dict) -> (DefaultSettings, list[RunSettings]):

    _require_key(input_config, ["default_settings", "runs"])

    if not isinstance(input_config["default_settings"], dict):
        raise ConfigError("'default_settings' must be a dict")
    if not isinstance(input_config["runs"], list):
        raise ConfigError("'runs' must be a list")

    defaults = _parse_defaults(input_config["default_settings"])
    runs = _parse_runs(input_config["runs"])
    return defaults, runs
