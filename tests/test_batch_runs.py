import sys
from pathlib import Path

import pytest

from access.esmf_trace import batch_runs
from access.esmf_trace.batch_runs import (
    RUN_SETTINGS_SUFFIX,
    BatchResult,
    JobFailure,
    _changed_settings,
    _display_path,
    _output_fingerprint,
    _read_recorded_settings,
    run_batch_jobs,
)
from access.esmf_trace.config import DefaultSettings, RunSettings
from access.esmf_trace.main import build_parser, cli_run_from_yaml, main
from access.esmf_trace.tmp_yaml_parser import write_yaml


class _SerialFuture:
    """Run the job inline so the tests don't depend on the fork start method."""

    def __init__(self, fn, *args):
        self._exc = None
        try:
            self._result = fn(*args)
        except Exception as e:  # noqa: BLE001 - mirrors what a worker would surface
            self._result = None
            self._exc = e

    def result(self):
        if self._exc is not None:
            raise self._exc
        return self._result


class _SerialExecutor:
    def __init__(self, max_workers=None):
        self.max_workers = max_workers

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def submit(self, fn, *args):
        return _SerialFuture(fn, *args)


@pytest.fixture
def serial_pool(monkeypatch):
    monkeypatch.setattr(batch_runs, "ProcessPoolExecutor", _SerialExecutor)
    monkeypatch.setattr(batch_runs, "as_completed", list)


@pytest.fixture
def stub_run(monkeypatch):
    """Replace the bt2 work with a no-op so only the batch plumbing is exercised."""
    monkeypatch.setattr(batch_runs, "single_run", lambda ns: (0, "success!"))


def _writing_single_run(calls):
    """
    single_run stand-in that writes the two files a real run would, so a
    following batch sees outputs on disk. Appends each namespace it is called
    with to `calls`, i.e. the jobs that were not skipped.
    """

    def _run(ns):
        calls.append(ns)
        post_dir = Path(ns.post_dir)
        post_dir.mkdir(parents=True, exist_ok=True)
        (post_dir / f"{ns.base_prefix}_timeseries.json").write_text("[]")
        (post_dir / f"{ns.base_prefix}_flamegraph.html").write_text("<html></html>")
        return (0, "success!")

    return _run


@pytest.fixture
def writing_run(monkeypatch):
    calls = []
    monkeypatch.setattr(batch_runs, "single_run", _writing_single_run(calls))
    return calls


@pytest.fixture
def archive(tmp_path):
    arch = tmp_path / "archive"
    traceout = arch / "output000" / "traceout"
    traceout.mkdir(parents=True)
    (traceout / "esmf_stream_0000").write_text("x")
    return arch


class TestDisplayPath:
    """
    post_dir is always built by appending to post_base_path, so the fallback
    below cannot be reached through run_batch_jobs. It is still pinned here:
    the helper exists to keep a mismatched base from killing a finished batch,
    and an untested guard is one refactor away from being dropped.
    """

    def test_relative_to_its_own_base(self, tmp_path):
        assert _display_path(tmp_path / "post" / "run" / "output000", tmp_path / "post") == "run/output000"

    def test_falls_back_to_full_path_when_not_under_base(self, tmp_path):
        post_dir = tmp_path / "elsewhere" / "output000"
        assert _display_path(post_dir, tmp_path / "post") == str(post_dir)


class TestRunBatchJobsProgressOutput:
    """
    Regression tests: the progress line used to be built with
    defaults.post_base_path, while post_dir was built from the run's own
    (resolved) effective base. Any mismatch crashed with ValueError/TypeError
    *after* every job had already finished and written its outputs.
    """

    def test_per_run_post_base_path_overriding_the_default(self, tmp_path, archive, serial_pool, stub_run, capsys):
        defaults = DefaultSettings(post_base_path=str(tmp_path / "default_post"))
        runs = [RunSettings(base_prefix="p", exact_path=archive, post_base_path=str(tmp_path / "run_post"))]

        run_batch_jobs(defaults, runs)

        out = capsys.readouterr().out
        assert "[postprocessing_p/output000] success!" in out
        assert "Successful jobs: 1" in out
        assert (tmp_path / "run_post" / "postprocessing_p" / "output000").is_dir()

    def test_relative_post_base_path(self, tmp_path, archive, serial_pool, stub_run, capsys, monkeypatch):
        monkeypatch.chdir(tmp_path)
        defaults = DefaultSettings(post_base_path="relative_post")

        run_batch_jobs(defaults, [RunSettings(base_prefix="p", exact_path=archive)])

        out = capsys.readouterr().out
        assert "[postprocessing_p/output000] success!" in out
        assert "Successful jobs: 1" in out

    def test_user_expanded_post_base_path(self, tmp_path, archive, serial_pool, stub_run, capsys, monkeypatch):
        monkeypatch.setenv("HOME", str(tmp_path))
        defaults = DefaultSettings(post_base_path="~/post")

        run_batch_jobs(defaults, [RunSettings(base_prefix="p", exact_path=archive)])

        out = capsys.readouterr().out
        assert "[postprocessing_p/output000] success!" in out
        assert "Successful jobs: 1" in out

    def test_worker_exception_is_labelled_not_crashed(self, tmp_path, archive, serial_pool, capsys, monkeypatch):
        def boom(ns):
            raise RuntimeError("bt2 blew up")

        monkeypatch.setattr(batch_runs, "single_run", boom)
        defaults = DefaultSettings(post_base_path=str(tmp_path / "default_post"))
        runs = [RunSettings(base_prefix="p", exact_path=archive, post_base_path=str(tmp_path / "run_post"))]

        run_batch_jobs(defaults, runs)

        out = capsys.readouterr().out
        assert "[postprocessing_p/output000] Failed: bt2 blew up" in out
        assert "Failed jobs: 1" in out

    def test_skip_message_uses_the_effective_base(self, tmp_path, archive, serial_pool, stub_run, capsys):
        run_post = tmp_path / "run_post"
        done = run_post / "postprocessing_p" / "output000"
        done.mkdir(parents=True)
        (done / "p_timeseries.json").write_text("[]")
        (done / "p_flamegraph.html").write_text("<html></html>")

        defaults = DefaultSettings(post_base_path=str(tmp_path / "default_post"))
        runs = [RunSettings(base_prefix="p", exact_path=archive, post_base_path=str(run_post))]

        run_batch_jobs(defaults, runs)

        out = capsys.readouterr().out
        assert "(settings not recorded) in postprocessing_p/output000" in out
        assert "No jobs to run" in out

    def test_label_is_relative_to_each_runs_own_base(self, tmp_path, archive, serial_pool, stub_run, capsys):
        defaults = DefaultSettings(post_base_path=str(tmp_path / "shared_post"))
        runs = [
            RunSettings(base_prefix="a", exact_path=archive),
            RunSettings(base_prefix="b", exact_path=archive, post_base_path=str(tmp_path / "other_post")),
        ]

        run_batch_jobs(defaults, runs)

        out = capsys.readouterr().out
        assert "[postprocessing_a/output000] success!" in out
        assert "[postprocessing_b/output000] success!" in out
        assert "Successful jobs: 2" in out
        assert Path(tmp_path / "other_post" / "postprocessing_b" / "output000").is_dir()


class TestFingerprintHelpers:
    def test_excludes_settings_that_do_not_affect_written_output(self):
        fingerprint = _output_fingerprint({"max_depth": 6, "post_dir": Path("/a"), "show_html": True})
        assert fingerprint == {"max_depth": 6}

    def test_paths_are_recorded_as_strings(self):
        assert _output_fingerprint({"traceout_path": Path("/a/traceout")}) == {"traceout_path": "/a/traceout"}

    def test_changed_settings_reports_differing_and_one_sided_keys(self):
        assert _changed_settings({"a": 1, "b": 2}, {"a": 9, "b": 2}) == ["a"]
        assert _changed_settings({"a": 1}, {"a": 1, "b": 2}) == ["b"]
        assert _changed_settings({"a": 1}, {"a": 1}) == []

    def test_unreadable_record_reads_as_absent(self, tmp_path):
        (tmp_path / f"p{RUN_SETTINGS_SUFFIX}").write_text("{not json")
        assert _read_recorded_settings(tmp_path, "p") is None

    def test_non_mapping_record_reads_as_absent(self, tmp_path):
        (tmp_path / f"p{RUN_SETTINGS_SUFFIX}").write_text("[1, 2, 3]")
        assert _read_recorded_settings(tmp_path, "p") is None

    def test_missing_record_reads_as_absent(self, tmp_path):
        assert _read_recorded_settings(tmp_path, "p") is None

    def test_record_name_is_not_picked_up_by_the_post_summary_glob(self):
        """post-summary collects *_timeseries.json; the sidecar must not match."""
        assert not f"p{RUN_SETTINGS_SUFFIX}".endswith("_timeseries.json")


class TestReprocessOnChangedSettings:
    """
    Outputs used to be reused on the strength of their filenames alone, so a
    re-run with a wider max_depth or a different model_component silently kept
    the old results. They are now reused only when the settings recorded beside
    them match.
    """

    def _run(self, tmp_path, archive, **defaults_kwargs):
        defaults = DefaultSettings(post_base_path=str(tmp_path / "post"), **defaults_kwargs)
        run_batch_jobs(defaults, [RunSettings(base_prefix="p", exact_path=archive)])

    def test_first_run_records_the_settings(self, tmp_path, archive, serial_pool, writing_run):
        self._run(tmp_path, archive, max_depth=6)

        recorded = _read_recorded_settings(tmp_path / "post" / "postprocessing_p" / "output000", "p")
        assert recorded is not None
        assert recorded["max_depth"] == 6
        assert "post_dir" not in recorded

    def test_unchanged_settings_skip(self, tmp_path, archive, serial_pool, writing_run, capsys):
        self._run(tmp_path, archive, max_depth=6)
        capsys.readouterr()

        self._run(tmp_path, archive, max_depth=6)

        assert len(writing_run) == 1
        assert "settings are unchanged" in capsys.readouterr().out

    def test_changed_max_depth_reprocesses_and_names_the_setting(
        self, tmp_path, archive, serial_pool, writing_run, capsys
    ):
        self._run(tmp_path, archive, max_depth=6)
        capsys.readouterr()

        self._run(tmp_path, archive, max_depth=20)

        assert len(writing_run) == 2
        out = capsys.readouterr().out
        assert "settings changed since these outputs were written (max_depth), reprocessing" in out
        assert "Successful jobs: 1" in out

    def test_changed_model_component_reprocesses(self, tmp_path, archive, serial_pool, writing_run, capsys):
        self._run(tmp_path, archive, model_component="[OCN] RunPhase1")
        capsys.readouterr()

        self._run(tmp_path, archive, model_component="[ICE] RunPhase1")

        assert len(writing_run) == 2
        assert "(model_component)" in capsys.readouterr().out

    def test_reprocessing_updates_the_record(self, tmp_path, archive, serial_pool, writing_run):
        self._run(tmp_path, archive, max_depth=6)
        self._run(tmp_path, archive, max_depth=20)

        recorded = _read_recorded_settings(tmp_path / "post" / "postprocessing_p" / "output000", "p")
        assert recorded["max_depth"] == 20

    def test_show_html_alone_does_not_reprocess(self, tmp_path, archive, serial_pool, writing_run, capsys):
        self._run(tmp_path, archive, show_html=False)
        capsys.readouterr()

        self._run(tmp_path, archive, show_html=True)

        assert len(writing_run) == 1
        assert "settings are unchanged" in capsys.readouterr().out

    def test_force_reprocesses_unchanged_settings(self, tmp_path, archive, serial_pool, writing_run, capsys):
        self._run(tmp_path, archive, max_depth=6)
        capsys.readouterr()

        self._run(tmp_path, archive, max_depth=6, force=True)

        assert len(writing_run) == 2
        out = capsys.readouterr().out
        assert "skip postprocessing" not in out
        assert "Successful jobs: 1" in out

    def test_outputs_without_a_record_are_still_reused(self, tmp_path, archive, serial_pool, writing_run, capsys):
        post_dir = tmp_path / "post" / "postprocessing_p" / "output000"
        post_dir.mkdir(parents=True)
        (post_dir / "p_timeseries.json").write_text("[]")
        (post_dir / "p_flamegraph.html").write_text("<html></html>")

        self._run(tmp_path, archive, max_depth=20)

        assert writing_run == []
        assert "(settings not recorded)" in capsys.readouterr().out

    def test_no_record_is_backfilled_for_outputs_of_unknown_provenance(
        self, tmp_path, archive, serial_pool, writing_run
    ):
        """Their real settings are unknown, so claiming the current ones would be a lie."""
        post_dir = tmp_path / "post" / "postprocessing_p" / "output000"
        post_dir.mkdir(parents=True)
        (post_dir / "p_timeseries.json").write_text("[]")
        (post_dir / "p_flamegraph.html").write_text("<html></html>")

        self._run(tmp_path, archive, max_depth=20)

        assert _read_recorded_settings(post_dir, "p") is None

    def test_corrupt_record_falls_back_to_reuse(self, tmp_path, archive, serial_pool, writing_run, capsys):
        self._run(tmp_path, archive, max_depth=6)
        record = tmp_path / "post" / "postprocessing_p" / "output000" / f"p{RUN_SETTINGS_SUFFIX}"
        record.write_text("{truncated")
        capsys.readouterr()

        self._run(tmp_path, archive, max_depth=20)

        assert len(writing_run) == 1
        assert "(settings not recorded)" in capsys.readouterr().out

    def test_failed_job_records_nothing(self, tmp_path, archive, serial_pool, monkeypatch, capsys):
        monkeypatch.setattr(batch_runs, "single_run", lambda ns: (1, "Failed: boom"))

        self._run(tmp_path, archive, max_depth=6)

        post_dir = tmp_path / "post" / "postprocessing_p" / "output000"
        assert _read_recorded_settings(post_dir, "p") is None
        assert "Failed jobs: 1" in capsys.readouterr().out

    def test_a_failed_job_is_retried_next_time(self, tmp_path, archive, serial_pool, monkeypatch):
        monkeypatch.setattr(batch_runs, "single_run", lambda ns: (1, "Failed: boom"))
        self._run(tmp_path, archive, max_depth=6)

        calls = []
        monkeypatch.setattr(batch_runs, "single_run", _writing_single_run(calls))
        self._run(tmp_path, archive, max_depth=6)

        assert len(calls) == 1


class TestBatchResult:
    """
    run_batch_jobs used to return None, so a total failure was indistinguishable
    from success to anything but a human reading stdout.
    """

    def _run(self, tmp_path, archive, n_runs=1, **defaults_kwargs):
        defaults = DefaultSettings(post_base_path=str(tmp_path / "post"), **defaults_kwargs)
        runs = [RunSettings(base_prefix=f"p{i}", exact_path=archive) for i in range(n_runs)]
        return run_batch_jobs(defaults, runs)

    def test_all_succeed(self, tmp_path, archive, serial_pool, writing_run):
        result = self._run(tmp_path, archive, n_runs=2)

        assert result == BatchResult(n_ok=2, n_fail=0, failures=[])
        assert result.ok

    def test_all_fail(self, tmp_path, archive, serial_pool, monkeypatch):
        monkeypatch.setattr(batch_runs, "single_run", lambda ns: (1, "Failed: bt2 blew up"))

        result = self._run(tmp_path, archive, n_runs=2)

        assert result.n_ok == 0
        assert result.n_fail == 2
        assert not result.ok
        assert sorted(f.label for f in result.failures) == [
            "postprocessing_p0/output000",
            "postprocessing_p1/output000",
        ]
        assert all(f.message == "Failed: bt2 blew up" for f in result.failures)

    def test_partial_failure_is_not_ok(self, tmp_path, archive, serial_pool, monkeypatch):
        def flaky(ns):
            if ns.base_prefix == "p1":
                return (1, "Failed: boom")
            return _writing_single_run([])(ns)

        monkeypatch.setattr(batch_runs, "single_run", flaky)

        result = self._run(tmp_path, archive, n_runs=2)

        assert (result.n_ok, result.n_fail) == (1, 1)
        assert not result.ok
        assert result.failures == [JobFailure("postprocessing_p1/output000", "Failed: boom")]

    def test_worker_exception_is_recorded_as_a_failure(self, tmp_path, archive, serial_pool, monkeypatch):
        def boom(ns):
            raise RuntimeError("segfault in bt2")

        monkeypatch.setattr(batch_runs, "single_run", boom)

        result = self._run(tmp_path, archive)

        assert result.n_fail == 1
        assert result.failures[0].message.startswith("Failed: segfault in bt2")

    def test_nothing_to_do_is_ok(self, tmp_path, archive, serial_pool, writing_run):
        self._run(tmp_path, archive)
        result = self._run(tmp_path, archive)

        assert result == BatchResult(n_ok=0, n_fail=0, failures=[])
        assert result.ok

    def test_failures_are_listed_in_the_summary(self, tmp_path, archive, serial_pool, monkeypatch, capsys):
        monkeypatch.setattr(batch_runs, "single_run", lambda ns: (1, "Failed: boom"))

        self._run(tmp_path, archive)

        out = capsys.readouterr().out
        assert "Failed jobs: 1" in out
        assert "  postprocessing_p0/output000: Failed: boom" in out


class TestExitCode:
    def test_job_failure_exits_non_zero(self, tmp_path, archive, serial_pool, monkeypatch):
        monkeypatch.setattr(batch_runs, "single_run", lambda ns: (1, "Failed: boom"))
        cfg = _write_run_config(tmp_path, archive)

        assert cli_run_from_yaml(build_parser().parse_args(["run-from-yaml", "--config", str(cfg)])) == 1

    def test_success_exits_zero(self, tmp_path, archive, serial_pool, writing_run):
        cfg = _write_run_config(tmp_path, archive)

        assert cli_run_from_yaml(build_parser().parse_args(["run-from-yaml", "--config", str(cfg)])) == 0

    def test_main_propagates_the_code(self, tmp_path, archive, serial_pool, monkeypatch):
        monkeypatch.setattr(batch_runs, "single_run", lambda ns: (1, "Failed: boom"))
        cfg = _write_run_config(tmp_path, archive)
        monkeypatch.setattr(sys, "argv", ["esmf-trace", "run-from-yaml", "--config", str(cfg)])

        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 1

    def test_main_exits_zero_on_success(self, tmp_path, archive, serial_pool, writing_run, monkeypatch):
        cfg = _write_run_config(tmp_path, archive)
        monkeypatch.setattr(sys, "argv", ["esmf-trace", "run-from-yaml", "--config", str(cfg)])

        with pytest.raises(SystemExit) as excinfo:
            main()

        assert excinfo.value.code == 0


def _write_run_config(tmp_path, archive):
    cfg = tmp_path / "run.yaml"
    write_yaml(
        {
            "default_settings": {"post_base_path": str(tmp_path / "post"), "max_workers": 1},
            "runs": [{"base_prefix": "p0", "exact_path": str(archive)}],
        },
        cfg,
    )
    return cfg
