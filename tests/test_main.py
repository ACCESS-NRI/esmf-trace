from pathlib import Path

from access.esmf_trace.main import (
    _apply_post_summary_overrides,
    _apply_run_overrides,
    build_parser,
)


class TestPostSummaryCli:
    def _parse(self, argv):
        return build_parser().parse_args(argv)

    def test_include_combined_defaults_to_none_when_unset(self, tmp_path):
        ns = self._parse(["post-summary-from-yaml", "--config", str(tmp_path / "c.yaml")])
        assert ns.include_combined is None
        assert ns.include_per_output is None

    def test_include_combined_flag_sets_true(self, tmp_path):
        ns = self._parse(["post-summary-from-yaml", "--config", str(tmp_path / "c.yaml"), "--include-combined"])
        assert ns.include_combined is True

    def test_no_include_combined_flag_sets_false(self, tmp_path):
        ns = self._parse(["post-summary-from-yaml", "--config", str(tmp_path / "c.yaml"), "--no-include-combined"])
        assert ns.include_combined is False

    def test_apply_overrides_only_includes_set_values(self, tmp_path):
        ns = self._parse(
            [
                "post-summary-from-yaml",
                "--config",
                str(tmp_path / "c.yaml"),
                "--include-per-output",
                "--stats-start-index",
                "2",
            ]
        )
        overrides = _apply_post_summary_overrides(ns)
        assert overrides["include_per_output"] is True
        assert overrides["stats_start_index"] == 2
        assert "include_combined" not in overrides
        assert "save_json_path" not in overrides

    def test_apply_overrides_converts_save_json_path_to_str(self, tmp_path):
        target = tmp_path / "out.json"
        ns = self._parse(
            ["post-summary-from-yaml", "--config", str(tmp_path / "c.yaml"), "--save-json-path", str(target)]
        )
        overrides = _apply_post_summary_overrides(ns)
        assert overrides["save_json_path"] == str(target)
        assert isinstance(overrides["save_json_path"], str)


class TestRunCli:
    def _parse(self, argv):
        return build_parser().parse_args(argv)

    def test_boolean_flags_only_set_when_true(self, tmp_path):
        ns = self._parse(["run-from-yaml", "--config", str(tmp_path / "c.yaml"), "--merge-adjacent"])
        overrides = _apply_run_overrides(ns)
        assert overrides["merge_adjacent"] is True
        assert "xaxis_datetime" not in overrides

    def test_none_valued_overrides_excluded(self, tmp_path):
        ns = self._parse(["run-from-yaml", "--config", str(tmp_path / "c.yaml")])
        overrides = _apply_run_overrides(ns)
        assert overrides == {}

    def test_scalar_overrides_captured(self, tmp_path):
        ns = self._parse(["run-from-yaml", "--config", str(tmp_path / "c.yaml"), "--max-depth", "9"])
        overrides = _apply_run_overrides(ns)
        assert overrides["max_depth"] == 9

    def test_config_path_parsed_as_path(self, tmp_path):
        cfg = tmp_path / "c.yaml"
        ns = self._parse(["run-from-yaml", "--config", str(cfg)])
        assert ns.config == cfg
        assert isinstance(ns.config, Path)


def test_missing_subcommand_is_required():
    parser = build_parser()
    assert parser.parse_args(["run-from-yaml", "--config", "x.yaml"]).cmd == "run-from-yaml"
