import json
from pathlib import Path

import pandas as pd
import pytest

from access.esmf_trace.config import PostRunSettings, PostSummarySettings
from access.esmf_trace.postprocess import (
    PUBLIC_COLUMNS,
    _prepare_summary_path,
    _select_summary_rows,
    _slice_per_series_iloc,
    _summarise_case,
    _to_public_table,
    post_summary_from_yaml,
)


def _write_case_output(post_base_path, case_name, output_index, rows, suffix="_timeseries.json"):
    """
    Write one output*/*_timeseries.json file for a case, matching the layout
    <post_base_path>/<case_name>/output<NNN>/<prefix>_timeseries.json expected by
    _collect_case_jsons / _load_timeseries_json.
    """
    out_dir = post_base_path / case_name / f"output{output_index:03d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"foo{suffix}"
    path.write_text(json.dumps(rows))
    return path


def _row(component, pet, duration_s, start=0):
    return {"model_component": component, "pet": pet, "duration_s": duration_s, "start": start, "end": start + 1}


class TestSlicePerSeriesIloc:
    GROUP_COLS = ["case"]
    ORDER_COLS = ["order"]

    def _df(self):
        return pd.DataFrame(
            {
                "case": ["a", "a", "a", "b", "b"],
                "order": [0, 1, 2, 0, 1],
                "value": [10, 11, 12, 20, 21],
            }
        )

    def test_both_none_returns_full_frame_unchanged(self):
        df = self._df()
        out = _slice_per_series_iloc(df, self.GROUP_COLS, self.ORDER_COLS, None, None)
        assert out is df

    def test_start_only_slices_from_start_to_end(self):
        out = _slice_per_series_iloc(self._df(), self.GROUP_COLS, self.ORDER_COLS, 1, None)
        assert sorted(out["value"].tolist()) == [11, 12, 21]

    def test_end_only_slices_from_beginning_to_end(self):
        out = _slice_per_series_iloc(self._df(), self.GROUP_COLS, self.ORDER_COLS, None, 1)
        assert sorted(out["value"].tolist()) == [10, 20]

    def test_start_and_end_slices_each_group_independently(self):
        out = _slice_per_series_iloc(self._df(), self.GROUP_COLS, self.ORDER_COLS, 0, 2)
        assert sorted(out["value"].tolist()) == [10, 11, 20, 21]

    def test_empty_dataframe_returns_empty_copy_without_error(self):
        df = self._df().iloc[0:0]
        out = _slice_per_series_iloc(df, self.GROUP_COLS, self.ORDER_COLS, 0, 1)
        assert out.empty
        assert out is not df

    def _df_with_nan_keys(self):
        return pd.DataFrame(
            {
                "case": ["a", "a", None, None],
                "order": [0, 1, 0, 1],
                "value": [10, 11, 20, 21],
            }
        )

    def test_rows_with_a_nan_group_key_are_not_dropped(self):
        # pandas' groupby default (dropna=True) would silently discard the
        # two NaN-keyed rows, so enabling slicing would change which rows
        # count, not just how many samples each series contributes.
        out = _slice_per_series_iloc(self._df_with_nan_keys(), self.GROUP_COLS, self.ORDER_COLS, 0, 2)
        assert sorted(out["value"].tolist()) == [10, 11, 20, 21]

    def test_nan_keyed_rows_form_their_own_series(self):
        out = _slice_per_series_iloc(self._df_with_nan_keys(), self.GROUP_COLS, self.ORDER_COLS, 0, 1)
        # one row kept from the 'a' series and one from the NaN series
        assert sorted(out["value"].tolist()) == [10, 20]

    def test_slicing_preserves_row_count_when_slice_covers_everything(self):
        df = self._df_with_nan_keys()
        unsliced = _slice_per_series_iloc(df, self.GROUP_COLS, self.ORDER_COLS, None, None)
        sliced = _slice_per_series_iloc(df, self.GROUP_COLS, self.ORDER_COLS, 0, None)
        assert len(sliced) == len(unsliced) == 4

    def test_all_nan_keys_still_slices(self):
        df = pd.DataFrame({"case": [None, None], "order": [0, 1], "value": [1, 2]})
        out = _slice_per_series_iloc(df, self.GROUP_COLS, self.ORDER_COLS, 0, 1)
        assert out["value"].tolist() == [1]


class TestSelectSummaryRows:
    def _summary(self):
        return pd.DataFrame(
            {
                "__output_name": ["output000", "output001", "combine"],
                "value": [1, 2, 3],
            }
        )

    def test_both_true_returns_all_rows(self):
        out = _select_summary_rows(self._summary(), include_combined=True, include_per_output=True)
        assert sorted(out["value"].tolist()) == [1, 2, 3]

    def test_per_output_only_excludes_combine_row(self):
        out = _select_summary_rows(self._summary(), include_combined=False, include_per_output=True)
        assert sorted(out["value"].tolist()) == [1, 2]
        assert "combine" not in out["__output_name"].tolist()

    def test_combined_only_keeps_only_combine_row(self):
        out = _select_summary_rows(self._summary(), include_combined=True, include_per_output=False)
        assert out["__output_name"].tolist() == ["combine"]

    def test_both_false_raises(self):
        with pytest.raises(ValueError, match="include_combined"):
            _select_summary_rows(self._summary(), include_combined=False, include_per_output=False)


class TestPrepareSummaryPath:
    def test_none_returns_none(self):
        # callers rely on this instead of guarding at the call site
        assert _prepare_summary_path(None) is None

    def test_none_does_not_touch_the_filesystem(self, monkeypatch):
        called = []
        monkeypatch.setattr(Path, "mkdir", lambda self, *a, **kw: called.append(self))
        assert _prepare_summary_path(None) is None
        assert called == []

    def test_non_json_suffix_raises(self, tmp_path):
        with pytest.raises(ValueError, match="must explicitly end with"):
            _prepare_summary_path(tmp_path / "out.txt")

    def test_creates_parent_directory(self, tmp_path):
        target = tmp_path / "nested" / "out.json"
        resolved = _prepare_summary_path(target)
        assert resolved == target
        assert target.parent.is_dir()


class TestSummariseCaseCombinedStats:
    """
    Regression tests for the fix that makes the 'combine' row aggregate
    directly from pooled raw duration_s samples, rather than averaging the
    already-aggregated per-output statistics (which was the pre-fix bug:
    e.g. summing hits became a mean-of-means, and ncpus became an average of
    per-output PET counts instead of the true distinct-PET count).
    """

    def _paths_for_two_outputs(self, tmp_path):
        # component "A": output000 has pet 0 with samples [1, 3]; output001 has pet 1 with [5, 7].
        p0 = _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0, start=0), _row("A", 0, 3.0, start=1)])
        p1 = _write_case_output(tmp_path, "case_a", 1, [_row("A", 1, 5.0, start=0), _row("A", 1, 7.0, start=1)])
        return [p0, p1]

    def test_combine_row_pools_raw_samples_not_per_output_stats(self, tmp_path):
        json_paths = self._paths_for_two_outputs(tmp_path)
        summary = _summarise_case(
            json_paths=json_paths,
            model_component=None,
            pets=None,
            stats_start_index=None,
            stats_end_index=None,
        )

        combine = summary[summary["__output_name"] == "combine"].iloc[0]

        # pooled duration_s = [1, 3, 5, 7]
        assert combine["hits"] == 4  # NOT mean(2, 2) == 2
        assert combine["tmin"] == 1.0
        assert combine["tmax"] == 7.0
        assert combine["tavg"] == pytest.approx(4.0)
        assert combine["tmedian"] == pytest.approx(4.0)
        # sample std of [1, 3, 5, 7], ddof=1
        assert combine["tstd"] == pytest.approx(2.581988897)
        assert combine["ncpus"] == 2  # NOT mean(1, 1) == 1 -- two distinct PETs pooled
        assert combine["pemin"] == 0
        assert combine["pemax"] == 1

    def test_per_output_rows_are_still_present_and_unpooled(self, tmp_path):
        json_paths = self._paths_for_two_outputs(tmp_path)
        summary = _summarise_case(
            json_paths=json_paths,
            model_component=None,
            pets=None,
            stats_start_index=None,
            stats_end_index=None,
        )
        per_output = summary[summary["__output_name"] != "combine"].sort_values("__output_name")
        assert per_output["hits"].tolist() == [2, 2]
        assert per_output["ncpus"].tolist() == [1, 1]

    def test_model_component_filter_applied_before_aggregation(self, tmp_path):
        p0 = _write_case_output(
            tmp_path,
            "case_b",
            0,
            [_row("A", 0, 1.0), _row("B", 0, 100.0)],
        )
        summary = _summarise_case(
            json_paths=[p0],
            model_component=["A"],
            pets=None,
            stats_start_index=None,
            stats_end_index=None,
        )
        assert set(summary["model_component"].unique()) == {"A"}

    def test_pets_filter_applied_before_aggregation(self, tmp_path):
        p0 = _write_case_output(
            tmp_path,
            "case_c",
            0,
            [_row("A", 0, 1.0), _row("A", 1, 100.0)],
        )
        summary = _summarise_case(
            json_paths=[p0],
            model_component=None,
            pets=[0],
            stats_start_index=None,
            stats_end_index=None,
        )
        combine = summary[summary["__output_name"] == "combine"].iloc[0]
        assert combine["tmax"] == 1.0

    def test_no_json_paths_returns_empty_frame_with_expected_columns(self):
        summary = _summarise_case(
            json_paths=[],
            model_component=None,
            pets=None,
            stats_start_index=None,
            stats_end_index=None,
        )
        assert summary.empty
        assert "__row_label" in summary.columns

    def test_null_model_component_survives_slicing(self, tmp_path):
        # _summarise_case aggregates with dropna=False, so the slicer must
        # keep null-keyed rows too; otherwise turning on stats_start_index
        # silently changes which rows are summarised.
        rows = [
            {"model_component": None, "pet": 0, "duration_s": 1.0, "start": 0},
            {"model_component": None, "pet": 0, "duration_s": 3.0, "start": 1},
            _row("A", 0, 5.0, start=0),
            _row("A", 0, 7.0, start=1),
        ]
        p0 = _write_case_output(tmp_path, "case_nan", 0, rows)

        unsliced = _summarise_case(
            json_paths=[p0],
            model_component=None,
            pets=None,
            stats_start_index=None,
            stats_end_index=None,
        )
        sliced = _summarise_case(
            json_paths=[p0],
            model_component=None,
            pets=None,
            stats_start_index=0,
            stats_end_index=2,
        )
        # the same set of components is summarised either way
        assert set(sliced["model_component"].isna()) == set(unsliced["model_component"].isna())
        assert sliced["hits"].sum() == unsliced["hits"].sum()

    def test_stats_slicing_restricts_pooled_samples(self, tmp_path):
        # 3 samples per (output, pet) series; keep only the first sample (iloc[0:1]) of each.
        p0 = _write_case_output(
            tmp_path,
            "case_d",
            0,
            [_row("A", 0, 1.0, start=0), _row("A", 0, 2.0, start=1), _row("A", 0, 3.0, start=2)],
        )
        summary = _summarise_case(
            json_paths=[p0],
            model_component=None,
            pets=None,
            stats_start_index=0,
            stats_end_index=1,
        )
        combine = summary[summary["__output_name"] == "combine"].iloc[0]
        assert combine["hits"] == 1
        assert combine["tavg"] == pytest.approx(1.0)


class TestPostSummaryFromYaml:
    def _settings(self, post_base_path, **overrides):
        defaults = PostSummarySettings(post_base_path=post_base_path, **overrides)
        runs = [PostRunSettings(name="case_a")]
        return defaults, runs

    def test_raises_value_error_when_no_rows_produced(self, tmp_path):
        defaults, runs = self._settings(tmp_path)  # no case dirs exist on disk
        with pytest.raises(ValueError, match="No rows produced"):
            post_summary_from_yaml(defaults, runs)

    def test_returns_dataframe_indexed_by_name(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        defaults, runs = self._settings(tmp_path)
        out = post_summary_from_yaml(defaults, runs)
        assert isinstance(out, pd.DataFrame)
        assert out.index.name == "name"
        assert "tavg" in out.columns

    def test_combined_table_column_order(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        defaults, runs = self._settings(tmp_path)
        out = post_summary_from_yaml(defaults, runs)
        assert out.index.name == "name"
        assert list(out.columns) == PUBLIC_COLUMNS[1:]

    def test_returned_table_exposes_no_internal_columns(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        defaults, runs = self._settings(tmp_path)
        out = post_summary_from_yaml(defaults, runs)
        assert not [c for c in out.columns if c.startswith("__")]
        assert not out.index.name.startswith("__")

    def test_ncpus_reaches_the_combined_table(self, tmp_path):
        # the pooled 'combine' row spans two PETs; without ncpus in the
        # combined output that distinct-PET count is invisible.
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        _write_case_output(tmp_path, "case_a", 1, [_row("A", 1, 5.0), _row("A", 1, 7.0)])
        defaults, runs = self._settings(tmp_path)
        out = post_summary_from_yaml(defaults, runs)
        assert "ncpus" in out.columns
        assert out.loc["case_a/combine/A", "ncpus"] == 2
        assert out.loc["case_a/output000/A", "ncpus"] == 1
        assert out.loc["case_a/output001/A", "ncpus"] == 1

    def test_ncpus_reaches_the_combined_json_and_parquet(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        _write_case_output(tmp_path, "case_a", 1, [_row("A", 1, 5.0)])
        defaults, runs = self._settings(tmp_path)
        save_path = tmp_path / "out" / "combined.json"
        post_summary_from_yaml(defaults, runs, all_runs_summary_path=save_path)

        saved = json.loads(save_path.read_text())
        assert all("ncpus" in row for row in saved)
        combine_row = next(r for r in saved if "/combine/" in r["name"])
        assert combine_row["ncpus"] == 2

        parquet = pd.read_parquet(tmp_path / "out" / "combined_table.parquet")
        assert "ncpus" in parquet.columns
        assert parquet.loc["case_a/combine/A", "ncpus"] == 2

    def test_include_per_output_false_keeps_only_combine_rows(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        _write_case_output(tmp_path, "case_a", 1, [_row("A", 1, 5.0), _row("A", 1, 7.0)])
        defaults, runs = self._settings(tmp_path)
        out = post_summary_from_yaml(defaults, runs, include_combined=True, include_per_output=False)
        assert all("/combine/" in name for name in out.index)

    def test_explicit_flags_override_defaults(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        defaults, runs = self._settings(tmp_path, include_combined=False, include_per_output=True)
        out = post_summary_from_yaml(defaults, runs, include_combined=True, include_per_output=False)
        assert all("/combine/" in name for name in out.index)

    def test_both_flags_false_raises(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        defaults, runs = self._settings(tmp_path)
        with pytest.raises(ValueError, match="include_combined"):
            post_summary_from_yaml(defaults, runs, include_combined=False, include_per_output=False)

    def test_saves_combined_json_and_parquet(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        defaults, runs = self._settings(tmp_path)
        save_path = tmp_path / "out" / "combined.json"
        post_summary_from_yaml(defaults, runs, all_runs_summary_path=save_path)
        assert save_path.is_file()
        assert (tmp_path / "out" / "combined_table.parquet").is_file()

    def test_per_run_summary_path_writes_only_selected_rows(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        run_save = tmp_path / "per_run.json"
        defaults = PostSummarySettings(post_base_path=tmp_path)
        runs = [PostRunSettings(name="case_a", summary_path=run_save)]
        post_summary_from_yaml(defaults, runs, include_combined=True, include_per_output=False)
        assert run_save.is_file()
        saved = json.loads(run_save.read_text())
        assert all(r["output_name"] == "combine" for r in saved)

    def test_run_without_summary_path_writes_nothing(self, tmp_path, capsys):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        defaults = PostSummarySettings(post_base_path=tmp_path)
        runs = [PostRunSettings(name="case_a")]  # no summary_path
        post_summary_from_yaml(defaults, runs)
        assert "saved per-run summary JSON" not in capsys.readouterr().out
        assert not list(tmp_path.glob("*.json"))

    def test_mixed_runs_only_save_the_ones_that_asked(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        _write_case_output(tmp_path, "case_b", 0, [_row("A", 0, 2.0)])
        run_save = tmp_path / "only_b.json"
        defaults = PostSummarySettings(post_base_path=tmp_path)
        runs = [
            PostRunSettings(name="case_a"),
            PostRunSettings(name="case_b", summary_path=run_save),
        ]
        post_summary_from_yaml(defaults, runs)
        assert run_save.is_file()
        # case_b contributes an output row and a combine row, and case_a
        # must not appear in a file it never asked for
        assert {r["case_name"] for r in json.loads(run_save.read_text())} == {"case_b"}

    def test_missing_case_directory_is_skipped_not_fatal(self, tmp_path, capsys):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        defaults = PostSummarySettings(post_base_path=tmp_path)
        runs = [PostRunSettings(name="case_a"), PostRunSettings(name="does_not_exist")]
        out = post_summary_from_yaml(defaults, runs)
        assert not out.empty
        assert "warning: case dir not found" in capsys.readouterr().out


class TestToPublicTable:
    def test_renames_internal_columns(self):
        df = pd.DataFrame(
            {
                "__row_label": ["case_a/output000/A"],
                "__case_name": ["case_a"],
                "__output_name": ["output000"],
                "model_component": ["A"],
                "hits": [2],
            }
        )
        out = _to_public_table(df)
        assert list(out.columns) == ["name", "case_name", "output_name", "model_component", "hits"]

    def test_drops_columns_outside_the_public_schema(self):
        df = pd.DataFrame({"__row_label": ["x"], "__src_path": ["/tmp/x.json"], "hits": [1]})
        out = _to_public_table(df)
        assert "__src_path" not in out.columns

    def test_orders_columns_as_public_columns(self):
        df = pd.DataFrame(
            {c: [0] for c in ["pemax", "hits", "__row_label", "ncpus", "model_component"]},
        )
        out = _to_public_table(df)
        assert list(out.columns) == ["name", "model_component", "ncpus", "hits", "pemax"]


class TestOutputSchemaParity:
    """
    The per-run JSON used to expose raw '__row_label'/'__case_name'/
    '__output_name' keys while the combined JSON used 'name' and dropped
    model_component, so one command produced two differently shaped files.
    """

    def _run(self, tmp_path):
        _write_case_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        _write_case_output(tmp_path, "case_a", 1, [_row("A", 1, 5.0), _row("A", 1, 7.0)])
        run_save = tmp_path / "per_run.json"
        combined_save = tmp_path / "combined.json"
        defaults = PostSummarySettings(post_base_path=tmp_path)
        runs = [PostRunSettings(name="case_a", summary_path=run_save)]
        table = post_summary_from_yaml(defaults, runs, all_runs_summary_path=combined_save)
        return json.loads(run_save.read_text()), json.loads(combined_save.read_text()), table

    def test_both_json_files_share_one_schema(self, tmp_path):
        per_run, combined, _ = self._run(tmp_path)
        assert sorted(per_run[0].keys()) == sorted(combined[0].keys())

    def test_neither_file_leaks_internal_columns(self, tmp_path):
        per_run, combined, _ = self._run(tmp_path)
        for rows in (per_run, combined):
            assert not [k for k in rows[0] if k.startswith("__")]

    def test_both_files_carry_the_public_columns(self, tmp_path):
        per_run, combined, _ = self._run(tmp_path)
        for rows in (per_run, combined):
            assert sorted(rows[0].keys()) == sorted(PUBLIC_COLUMNS)

    def test_model_component_survives_into_the_combined_file(self, tmp_path):
        _, combined, _ = self._run(tmp_path)
        assert all(row["model_component"] == "A" for row in combined)

    def test_returned_table_matches_the_file_schema(self, tmp_path):
        _, combined, table = self._run(tmp_path)
        assert sorted([table.index.name, *table.columns]) == sorted(combined[0].keys())

    def test_parquet_matches_the_file_schema(self, tmp_path):
        _, combined, _ = self._run(tmp_path)
        parquet = pd.read_parquet(tmp_path / "combined_table.parquet")
        assert sorted([parquet.index.name, *parquet.columns]) == sorted(combined[0].keys())

    def test_printed_table_is_narrowed_but_files_are_not(self, tmp_path, capsys):
        _, combined, _ = self._run(tmp_path)
        printed = capsys.readouterr().out
        # the long, repeated columns are omitted from the terminal view only;
        # pandas may still elide middle columns, so check the outer ones.
        assert "model_component" not in printed
        assert "case_name" not in printed
        assert "ncpus" in printed
        assert "pemax" in printed
        assert all("model_component" in row for row in combined)
