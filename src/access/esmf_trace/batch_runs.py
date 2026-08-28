import argparse
import json
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path

import psutil

from .config import ConfigError, DefaultSettings, RunSettings
from .run import run as single_run
from .utils import extract_index_list_from_str, output_name_to_index

# Recording the settings a job's outputs were produced with written
# next to them on success.
RUN_SETTINGS_SUFFIX = "_run_settings.json"

# Job kwargs that do not change what lands on disk, so a difference in them is
# no reason to reprocess: post_dir is the location itself, and show_html only
# decides whether a browser is opened afterwards.
_FINGERPRINT_EXCLUDED = frozenset({"post_dir", "show_html"})


@dataclass(frozen=True)
class _Job:
    ns: argparse.Namespace
    label: str
    post_dir: Path
    fingerprint: dict


def _find_traceout_dir(output_dir: Path, stream_prefix: str) -> Path | None:
    """
    Return <outputNNN>/traceout if it exists and holds at least one
    <stream_prefix>_* file, else None. This is the input a single job reads.
    """
    tdir = output_dir / "traceout"
    return tdir if (tdir.is_dir() and any(tdir.glob(f"{stream_prefix}_*"))) else None


def _display_path(post_dir: Path, post_base_path: Path) -> str:
    """
    Short label for a post dir, relative to the base it was built from.

    That base is the run's effective post_base_path, which is resolved and may
    come from the run itself rather than from default_settings - so it is not
    interchangeable with defaults.post_base_path. Fall back to the full path if
    the two ever fail to share a prefix, since this only feeds progress output.
    """
    try:
        return str(post_dir.relative_to(post_base_path))
    except ValueError:
        return str(post_dir)


def _expected_outputs_exist(post_dir: Path, base_prefix: str) -> bool:
    """
    True if both files a job would write are already present in post_dir.
    """
    expected = [
        post_dir / f"{base_prefix}_timeseries.json",
        post_dir / f"{base_prefix}_flamegraph.html",
    ]
    return all(p.exists() for p in expected)


def _output_fingerprint(job_kwargs: dict) -> dict:
    """
    The settings that determine what a job writes, in a JSON-comparable form.

    Derived from the job kwargs rather than an explicit list, so a setting
    added to RunSettings.to_job_kwargs() later is covered by default. That errs
    towards reprocessing when in doubt, which is the safe direction.
    """
    return {
        key: str(value) if isinstance(value, Path) else value
        for key, value in job_kwargs.items()
        if key not in _FINGERPRINT_EXCLUDED
    }


def _settings_record_path(post_dir: Path, base_prefix: str) -> Path:
    return post_dir / f"{base_prefix}{RUN_SETTINGS_SUFFIX}"


def _read_recorded_settings(post_dir: Path, base_prefix: str) -> dict | None:
    """
    The fingerprint recorded beside an existing set of outputs, or None if
    there isn't one or it can't be read - outputs written before this file
    existed, a partial write, hand-edited JSON.
    """
    try:
        recorded = json.loads(_settings_record_path(post_dir, base_prefix).read_text())
    except (OSError, ValueError):
        return None
    return recorded if isinstance(recorded, dict) else None


def _write_recorded_settings(post_dir: Path, base_prefix: str, fingerprint: dict) -> None:
    _settings_record_path(post_dir, base_prefix).write_text(json.dumps(fingerprint, indent=2, sort_keys=True))


def _changed_settings(recorded: dict, current: dict) -> list[str]:
    """
    Names of the settings that differ between two fingerprints.
    keys that exist on only one side or values that differ on both sides.
    The caller can then decide whether to reprocess or not.
    """
    only_one_side = set(recorded) ^ set(current)  # keys that exist on only one side
    differing = {
        key for key in set(recorded) & set(current) if recorded[key] != current[key]
    }  # values differ on both sides
    return sorted(only_one_side | differing)


def _skip_decision(post_dir: Path, base_prefix: str, fingerprint: dict, force: bool) -> tuple[bool, str]:
    """
    Decide whether an existing set of outputs can stand, as (skip, message).

    Outputs are reused only when the settings recorded beside them match the
    ones about to be used. Outputs with no record - anything written before
    this check existed - keep the old behaviour of being reused on sight,
    rather than being invalidated wholesale. Their settings are unknown, so no
    record is backfilled for them either; --force or deleting them is the way
    to regenerate.

    An empty message means there is nothing worth printing.
    """
    if force or not _expected_outputs_exist(post_dir, base_prefix):
        # No outputs or --force, so reprocess regardless of settings.
        return False, ""

    recorded = _read_recorded_settings(post_dir, base_prefix)
    if recorded is None:
        # No record, so the outputs are reused on sight.
        # This is the old behaviour, so we don't warn about it.
        return True, "-- skip postprocessing, expected outputs already exist (settings not recorded)"
    if recorded == fingerprint:
        # Settings match, so the outputs are reused
        return True, "-- skip postprocessing, expected outputs already exist and settings are unchanged"

    changed = ", ".join(_changed_settings(recorded, fingerprint))
    return False, f"-- settings changed since these outputs were written ({changed}), reprocessing"


def _gather_outputs(archive_dir: Path, output_index: str | None) -> list[Path]:
    """
    Return the outputNNN dirs to process, sorted by index.

    archive_dir is a run's resolved input archive dir (RunSettings.exact_path
    or the run_base/run_name/branch/archive form). output_index is a range
    string like "0,2-4"; None means every outputNNN present. Requested indices
    that aren't on disk are warned about, not treated as an error.
    """
    if not archive_dir.is_dir():
        print(f"-- skip not a dir: {archive_dir}")
        return []
    all_outputs = [p for p in archive_dir.glob("output*") if p.is_dir()]
    all_outputs = [p for p in all_outputs if output_name_to_index(p) is not None]
    output_dirs = sorted(all_outputs, key=output_name_to_index)
    selected = extract_index_list_from_str(output_index)
    if selected is not None:
        sel = set(selected)
        present = {output_name_to_index(p) for p in output_dirs}
        missing = sorted(sel - present)
        if missing:
            print(f"-- warning: requested output indices not found: {missing}")
        output_dirs = [p for p in output_dirs if output_name_to_index(p) in sel]
    return output_dirs


def _build_namespace(job_kwargs: dict) -> argparse.Namespace:
    """
    Convert a kwargs dict to an argparse.Namespace which is compatible with run.run()
    """
    return argparse.Namespace(**job_kwargs)


def run_one_job(ns: argparse.Namespace) -> tuple[int, str]:
    try:
        return single_run(ns)
    except Exception as e:
        return (1, f"Failed: {e}")


def run_batch_jobs(defaults: DefaultSettings, runs: list[RunSettings]) -> None:
    """
    Expand every run into one job per outputNNN directory, then process the
    jobs in parallel.

    Per run:
        - resolve the input archive dir (exact_path, or the run_base/run_name/
          branch/archive form)
        - pick the outputNNN dirs to process, honouring output_index
        - locate <outputNNN>/traceout inside each
        - derive the matching output dir,
          <post_base_path>/postprocessing_<base_prefix>/<outputNNN>
        - hand the pair to run.run() in a worker process

    See RunSettings for the full directory layout and what base_prefix names.

    A job is skipped when its outputs already exist AND the settings recorded
    beside them match the ones about to be used, so re-running with a wider
    max_depth or a different model_component regenerates rather than silently
    keeping the old results. Outputs predating that record are still reused on
    sight; defaults.force reprocesses everything regardless.
    """

    max_workers = defaults.max_workers or (psutil.cpu_count(logical=False) or 1)
    print(f"-- Using up to {max_workers} parallel workers")

    jobs = []
    for run in runs:
        exact_path = run._resolve_exact_paths()
        if not exact_path:
            raise ConfigError(f"-- cannot resolve {exact_path}, please check config!")

        base_prefix = run.base_prefix
        post_base_path = run._effective_post_base_path(defaults)
        post_base_path.mkdir(parents=True, exist_ok=True)

        output_dirs = _gather_outputs(exact_path, run.output_index)
        if not output_dirs:
            raise ValueError(f"-- no output* dirs found under {exact_path}")

        for outdir in output_dirs:
            # print(f"-- processing {outdir}")

            traceout_path = _find_traceout_dir(outdir, defaults.stream_prefix)
            if not traceout_path:
                raise ValueError(f"-- no traceout dir found under {outdir}")

            post_dir = post_base_path / f"postprocessing_{base_prefix}" / outdir.name

            label = _display_path(post_dir, post_base_path)

            job_kwargs = run.to_job_kwargs(defaults=defaults, traceout_path=traceout_path, post_dir=post_dir)
            fingerprint = _output_fingerprint(job_kwargs)

            skip, message = _skip_decision(post_dir, base_prefix, fingerprint, defaults.force)
            if message:
                print(f"{message} in {label}")
            if skip:
                continue

            post_dir.mkdir(parents=True, exist_ok=True)

            jobs.append(_Job(_build_namespace(job_kwargs), label, post_dir, fingerprint))

    if not jobs:
        print("-- No jobs to run. All done or nothing to do.")
        return

    print(f"-- Running {len(jobs)} jobs with up to {max_workers} parallel workers...")

    n_ok = 0
    n_fail = 0

    with ProcessPoolExecutor(max_workers=max_workers) as exe:
        tmp_jobs = {exe.submit(run_one_job, job.ns): job for job in jobs}
        for tmp in as_completed(tmp_jobs):
            job = tmp_jobs[tmp]
            try:
                ret, msg = tmp.result()
            except Exception as e:
                n_fail += 1
                print(f"[{job.label}] EXCEPTION: {e}")
            else:
                if ret == 0:
                    n_ok += 1
                    # Only a job that actually wrote its outputs may claim the
                    # settings that produced them.
                    try:
                        _write_recorded_settings(job.post_dir, job.ns.base_prefix, job.fingerprint)
                    except OSError as e:
                        print(f"[{job.label}] warning: could not record run settings: {e}")
                else:
                    n_fail += 1
                print(f"[{job.label}] {msg}")

    print("\n")
    print("=== Summary ===")
    print(f"Successful jobs: {n_ok}")
    print(f"Failed jobs: {n_fail}")
