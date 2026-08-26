import json
import re

import pytest

from access.esmf_trace.config import ConfigError, parse_post_summary_config
from access.esmf_trace.library import (
    ACCESSPostSummaryConfigBuilder,
    ACCESSRunConfigBuilder,
    post_summary_from_config,
)


def _write_output(post_base_path, case_name, output_index, rows):
    out_dir = post_base_path / case_name / f"output{output_index:03d}"
    out_dir.mkdir(parents=True, exist_ok=True)
    (out_dir / "foo_timeseries.json").write_text(json.dumps(rows))


def _row(component, pet, duration_s):
    return {"model_component": component, "pet": pet, "duration_s": duration_s, "start": 0}


class TestACCESSRunConfigBuilder:
    def _builder(self, **kwargs):
        defaults = {
            "branches": ["branch_1"],
            "post_base_path": "/post/base",
            "exact_paths": ["/traceout/branch_1"],
            "model_component": "compA",
        }
        defaults.update(kwargs)
        return ACCESSRunConfigBuilder(**defaults)

    def test_requires_at_least_one_branch(self):
        with pytest.raises(ValueError, match="branch"):
            self._builder(branches=[])

    def test_requires_non_empty_model_component(self):
        with pytest.raises(ValueError, match="model_component"):
            self._builder(model_component="")

    def test_max_workers_must_be_positive_int(self):
        with pytest.raises(ValueError, match="max_workers"):
            self._builder(max_workers=0)

    def test_pets_components_requires_branch_pattern(self):
        with pytest.raises(ValueError, match="branch_pattern"):
            self._builder(pets_components=["shared"])

    def test_build_config_without_pets(self):
        builder = self._builder()
        config = builder.build_config()
        assert config["default_settings"]["post_base_path"] == "/post/base"
        assert config["default_settings"]["model_component"] == ["compA"]
        assert config["runs"] == [{"exact_path": "/traceout/branch_1", "base_prefix": "branch_1"}]

    def test_build_config_with_pets_from_branch_pattern(self):
        builder = self._builder(
            branches=["run_shared_26_ocn_78"],
            exact_paths=["/traceout/run_shared_26_ocn_78"],
            branch_pattern=re.compile(r"shared_(?P<shared>\d+)_ocn_(?P<ocn>\d+)"),
            pets_components=["shared", "ocn"],
            pets_prefix="0",
        )
        config = builder.build_config()
        assert config["runs"][0]["pets"] == "0,26,78"

    def test_branch_not_matching_pattern_raises(self):
        builder = self._builder(
            branches=["no_match_here"],
            branch_pattern=re.compile(r"shared_(?P<shared>\d+)"),
            pets_components=["shared"],
        )
        with pytest.raises(ValueError, match="layout pattern"):
            builder.build_config()


class TestACCESSPostSummaryConfigBuilder:
    def _builder(self, **kwargs):
        defaults = {"post_base_path": "/post/base"}
        defaults.update(kwargs)
        return ACCESSPostSummaryConfigBuilder(**defaults)

    def test_both_include_flags_false_raises(self):
        with pytest.raises(ValueError, match="include_combined"):
            self._builder(include_combined=False, include_per_output=False)

    def test_build_config_requires_at_least_one_run(self):
        builder = self._builder()
        with pytest.raises(ValueError, match="At least one run"):
            builder.build_config([])

    def test_build_config_default_settings_shape(self):
        builder = self._builder(
            model_component=["a", "b"],
            pets="0,3",
            stats_start_index=1,
            stats_end_index=5,
            all_runs_summary_path="/out/all_runs.json",
            include_combined=False,
        )
        config = builder.build_config([{"name": "case_a"}])
        ds = config["default_settings"]
        assert ds["model_component"] == ["a", "b"]
        assert ds["pets"] == [0, 3]
        assert ds["stats_start_index"] == 1
        assert ds["stats_end_index"] == 5
        assert ds["all_runs_summary_path"] == "/out/all_runs.json"
        assert ds["include_combined"] is False
        assert ds["include_per_output"] is True
        assert config["runs"] == [{"name": "case_a"}]

    @pytest.mark.parametrize(
        "pets, expected",
        [
            ("0,13", [0, 13]),
            ("0,3-5", [0, 3, 4, 5]),  # ranges must expand, not survive as "3-5"
            ("0,3-5,8", [0, 3, 4, 5, 8]),
            ("  0 , 13 ", [0, 13]),
            ([13, 0, 0], [0, 13]),  # sorted and deduped
            (7, [7]),
        ],
    )
    def test_pets_normalised_to_sorted_ints(self, pets, expected):
        ds = self._builder(pets=pets).build_config([{"name": "case_a"}])["default_settings"]
        assert ds["pets"] == expected
        assert all(isinstance(p, int) for p in ds["pets"])

    @pytest.mark.parametrize("pets", ["0,13", "0,3-5", "0,3-5,8"])
    def test_built_config_parses_and_matches_the_yaml_path(self, pets):
        # the builder must never emit a pets value the parser would reject;
        # "0,3-5" used to survive as ["0", "3-5"] and blow up on int()
        built = self._builder(pets=pets).build_config([{"name": "case_a"}])
        via_builder, _ = parse_post_summary_config(built)
        via_yaml, _ = parse_post_summary_config(
            {"default_settings": {"post_base_path": "/post/base", "pets": pets}, "runs": [{"name": "case_a"}]}
        )
        assert via_builder.pets == via_yaml.pets

    def test_default_overwrite_applied_last(self):
        builder = self._builder(default_overwrite={"timeseries_suffix": "_custom.json"})
        config = builder.build_config([{"name": "case_a"}])
        assert config["default_settings"]["timeseries_suffix"] == "_custom.json"


class TestPostSummaryFromConfigWiring:
    def test_end_to_end_with_dict_config_and_overrides(self, tmp_path):
        _write_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        config = {
            "default_settings": {"post_base_path": str(tmp_path), "stats_start_index": 5},
            "runs": [{"name": "case_a"}],
        }
        # a library override should win over the (irrelevant, out-of-range) default
        out = post_summary_from_config(config, post_overrides={"stats_start_index": None})
        assert not out.empty

    def test_all_runs_summary_path_argument_writes_output(self, tmp_path):
        _write_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0)])
        config = {
            "default_settings": {"post_base_path": str(tmp_path)},
            "runs": [{"name": "case_a"}],
        }
        save_path = tmp_path / "all_runs.json"
        post_summary_from_config(config, all_runs_summary_path=save_path)
        assert save_path.is_file()


class TestPostSummaryFromConfigIncludeFlags:
    """
    include_combined/include_per_output are first-class parameters, so a
    caller can pick which slice of the summary they want without going
    through the stringly-typed post_overrides dict.
    """

    def _config(self, tmp_path, **default_settings):
        _write_output(tmp_path, "case_a", 0, [_row("A", 0, 1.0), _row("A", 0, 3.0)])
        _write_output(tmp_path, "case_a", 1, [_row("A", 1, 5.0), _row("A", 1, 7.0)])
        settings = {"post_base_path": str(tmp_path)}
        settings.update(default_settings)
        return {"default_settings": settings, "runs": [{"name": "case_a"}]}

    def test_default_returns_both_row_kinds(self, tmp_path):
        out = post_summary_from_config(self._config(tmp_path))
        assert any("/combine/" in n for n in out.index)
        assert any("/output00" in n for n in out.index)

    def test_combined_only(self, tmp_path):
        out = post_summary_from_config(self._config(tmp_path), include_per_output=False)
        assert all("/combine/" in n for n in out.index)
        assert len(out) == 1

    def test_per_output_only(self, tmp_path):
        out = post_summary_from_config(self._config(tmp_path), include_combined=False)
        assert not any("/combine/" in n for n in out.index)
        assert len(out) == 2

    def test_parameter_takes_precedence_over_post_overrides(self, tmp_path):
        out = post_summary_from_config(
            self._config(tmp_path),
            post_overrides={"include_per_output": True},
            include_per_output=False,
        )
        assert all("/combine/" in n for n in out.index)

    def test_parameter_takes_precedence_over_config_default_settings(self, tmp_path):
        out = post_summary_from_config(
            self._config(tmp_path, include_per_output=False),
            include_per_output=True,
            include_combined=False,
        )
        assert not any("/combine/" in n for n in out.index)

    def test_none_leaves_config_value_untouched(self, tmp_path):
        out = post_summary_from_config(
            self._config(tmp_path, include_per_output=False),
            include_per_output=None,
        )
        assert all("/combine/" in n for n in out.index)

    def test_both_false_raises(self, tmp_path):
        with pytest.raises(ValueError, match="include_combined"):
            post_summary_from_config(
                self._config(tmp_path),
                include_combined=False,
                include_per_output=False,
            )

    def test_post_overrides_still_works_as_a_channel(self, tmp_path):
        out = post_summary_from_config(self._config(tmp_path), post_overrides={"include_per_output": False})
        assert all("/combine/" in n for n in out.index)

    def test_typo_in_post_overrides_is_rejected_not_ignored(self, tmp_path):
        with pytest.raises(ConfigError, match="include_combine"):
            post_summary_from_config(self._config(tmp_path), post_overrides={"include_combine": False})
