from pathlib import Path

import pytest

from access.esmf_trace import batch_runs
from access.esmf_trace.batch_runs import _display_path, run_batch_jobs
from access.esmf_trace.config import DefaultSettings, RunSettings


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


@pytest.fixture
def archive(tmp_path):
    arch = tmp_path / "archive"
    traceout = arch / "output000" / "traceout"
    traceout.mkdir(parents=True)
    (traceout / "esmf_stream_0000").write_text("x")
    return arch


class TestDisplayPath:
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
        assert "already exist in postprocessing_p/output000" in out
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
