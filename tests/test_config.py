from pathlib import Path

import pytest

from access.esmf_trace.config import (
    ConfigError,
    _norm_pets,
    load_post_summary_config,
    load_yaml_config,
    parse_post_summary_config,
)
from access.esmf_trace.tmp_yaml_parser import write_yaml


def _base_post_summary_data(**default_overrides):
    default_settings = {"post_base_path": "/some/base"}
    default_settings.update(default_overrides)
    return {
        "default_settings": default_settings,
        "runs": [{"name": "case_a"}],
    }


class TestNormPets:
    def test_none(self):
        assert _norm_pets(None) is None

    def test_int(self):
        assert _norm_pets(3) == [3]

    def test_list_sorted_and_deduped(self):
        assert _norm_pets([3, 1, 1, 2]) == [1, 2, 3]

    def test_tuple_and_set(self):
        assert _norm_pets((2, 1)) == [1, 2]
        assert _norm_pets({4, 2}) == [2, 4]

    def test_string_delegates_to_extract_pets(self):
        assert _norm_pets("0,3-5") == [0, 3, 4, 5]


class TestParsePostSummaryConfigValidation:
    def test_missing_required_top_level_keys_raises(self):
        with pytest.raises(ConfigError, match="default_settings"):
            parse_post_summary_config({"runs": []})

    def test_missing_post_base_path_raises(self):
        with pytest.raises(ConfigError, match="post_base_path"):
            parse_post_summary_config({"default_settings": {}, "runs": []})

    def test_missing_run_name_raises(self):
        data = _base_post_summary_data()
        data["runs"] = [{}]
        with pytest.raises(ConfigError, match="name"):
            parse_post_summary_config(data)

    def test_both_include_flags_false_raises(self):
        data = _base_post_summary_data(include_combined=False, include_per_output=False)
        with pytest.raises(ConfigError, match="include_combined.*include_per_output"):
            parse_post_summary_config(data)

    def test_runs_must_be_a_list(self):
        data = _base_post_summary_data()
        data["runs"] = {"name": "case_a"}
        with pytest.raises(ConfigError, match="runs"):
            parse_post_summary_config(data)


class TestUnknownKeyRejection:
    """
    A mistyped key used to be silently dropped, leaving the setting at its
    default with no indication anything was wrong.
    """

    def test_typo_in_default_settings_raises(self):
        data = _base_post_summary_data()
        data["default_settings"]["inclde_combined"] = False  # typo
        with pytest.raises(ConfigError, match="inclde_combined"):
            parse_post_summary_config(data)

    def test_typo_in_default_overrides_raises(self):
        with pytest.raises(ConfigError, match="include_combine"):
            parse_post_summary_config(
                _base_post_summary_data(),
                default_overrides={"include_combine": False},  # missing trailing 'd'
            )

    def test_typo_in_run_entry_raises(self):
        data = _base_post_summary_data()
        data["runs"] = [{"name": "case_a", "stats_start_idx": 2}]
        with pytest.raises(ConfigError, match="stats_start_idx"):
            parse_post_summary_config(data)

    def test_error_message_lists_valid_keys(self):
        data = _base_post_summary_data()
        data["default_settings"]["bogus"] = 1
        with pytest.raises(ConfigError, match="valid keys:.*include_combined"):
            parse_post_summary_config(data)

    @pytest.mark.parametrize("key", ["stream_prefix", "cmap", "max_depth", "renderer"])
    def test_run_config_keys_are_rejected_not_silently_ignored(self, key):
        # these mean something in a run config but nothing here; accepting
        # them would imply they have an effect.
        data = _base_post_summary_data(**{key: "whatever"})
        with pytest.raises(ConfigError, match=key):
            parse_post_summary_config(data)

    def test_run_config_keys_are_rejected_in_overrides(self):
        with pytest.raises(ConfigError, match="stream_prefix"):
            parse_post_summary_config(
                _base_post_summary_data(),
                default_overrides={"stream_prefix": "esmf_stream"},
            )

    def test_valid_config_parses_silently(self, capsys):
        parse_post_summary_config(_base_post_summary_data(pets="0,1", include_combined=False))
        assert capsys.readouterr().out == ""

    def test_every_post_summary_field_is_accepted(self, tmp_path):
        # guards against the known-key set drifting from the dataclass
        data = _base_post_summary_data(
            model_component="a",
            pets="0,1",
            stats_start_index=1,
            stats_end_index=5,
            timeseries_suffix="_ts.json",
            all_runs_summary_path=str(tmp_path / "out.json"),
            include_combined=True,
            include_per_output=True,
        )
        data["runs"] = [
            {
                "name": "case_a",
                "output_index": "0-2",
                "model_component": "a",
                "pets": [0],
                "stats_start_index": 1,
                "stats_end_index": 5,
                "summary_path": str(tmp_path / "run.json"),
            }
        ]
        defaults, runs = parse_post_summary_config(data)
        assert defaults.timeseries_suffix == "_ts.json"
        assert runs[0].output_index == [0, 1, 2]


class TestParsePostSummaryConfigDefaults:
    def test_defaults_fall_back_to_true_when_unset(self):
        defaults, _ = parse_post_summary_config(_base_post_summary_data())
        assert defaults.include_combined is True
        assert defaults.include_per_output is True
        assert defaults.timeseries_suffix == "_timeseries.json"
        assert defaults.post_base_path == Path("/some/base")

    def test_explicit_include_flags_respected(self):
        defaults, _ = parse_post_summary_config(
            _base_post_summary_data(include_combined=False, include_per_output=True)
        )
        assert defaults.include_combined is False
        assert defaults.include_per_output is True

    def test_default_pets_and_model_component_normalised(self):
        data = _base_post_summary_data(pets="0,3-4", model_component="a, b")
        defaults, _ = parse_post_summary_config(data)
        assert defaults.pets == [0, 3, 4]
        assert defaults.model_component == ["a", "b"]


class TestParsePostSummaryConfigPerRunInheritance:
    def test_run_inherits_defaults_when_unset(self):
        data = _base_post_summary_data(pets="0,1", model_component="comp_a", stats_start_index=2)
        _, runs = parse_post_summary_config(data)
        (run,) = runs
        assert run.pets == [0, 1]
        assert run.model_component == ["comp_a"]
        assert run.stats_start_index == 2

    def test_run_level_value_overrides_default_when_no_global_override(self):
        data = _base_post_summary_data(pets="0,1")
        data["runs"] = [{"name": "case_a", "pets": [9, 9, 2]}]
        _, runs = parse_post_summary_config(data)
        (run,) = runs
        assert run.pets == [2, 9]

    def test_default_overrides_apply_to_every_run_even_when_run_sets_its_own_value(self):
        data = _base_post_summary_data(pets="0,1")
        data["runs"] = [
            {"name": "case_a", "pets": [9]},
            {"name": "case_b"},
        ]
        _, runs = parse_post_summary_config(data, default_overrides={"pets": [5]})
        assert [r.pets for r in runs] == [[5], [5]]

    def test_output_index_parsed_from_string(self):
        data = _base_post_summary_data()
        data["runs"] = [{"name": "case_a", "output_index": "0,2-3"}]
        _, runs = parse_post_summary_config(data)
        assert runs[0].output_index == [0, 2, 3]

    def test_output_index_parsed_from_list_of_mixed_types(self):
        data = _base_post_summary_data()
        data["runs"] = [{"name": "case_a", "output_index": ["1", 2]}]
        _, runs = parse_post_summary_config(data)
        assert runs[0].output_index == [1, 2]

    def test_output_index_absent_is_none(self):
        _, runs = parse_post_summary_config(_base_post_summary_data())
        assert runs[0].output_index is None

    def test_per_run_summary_path_does_not_inherit_default(self, tmp_path):
        save_path = tmp_path / "all_runs.json"
        data = _base_post_summary_data(all_runs_summary_path=str(save_path))
        data["runs"] = [{"name": "case_a"}]
        _, runs = parse_post_summary_config(data)
        assert runs[0].summary_path is None

    def test_per_run_summary_path_honoured_when_declared_on_the_run(self, tmp_path):
        run_save_path = tmp_path / "per_run.json"
        data = _base_post_summary_data()
        data["runs"] = [{"name": "case_a", "summary_path": str(run_save_path)}]
        _, runs = parse_post_summary_config(data)
        assert runs[0].summary_path == run_save_path


class TestLoadPostSummaryConfig:
    def test_accepts_a_plain_dict(self):
        defaults, runs = load_post_summary_config(_base_post_summary_data())
        assert defaults.post_base_path == Path("/some/base")
        assert len(runs) == 1

    def test_accepts_a_yaml_file_path(self, tmp_path):
        yaml_path = tmp_path / "post_summary.yaml"
        write_yaml(_base_post_summary_data(), yaml_path)
        defaults, runs = load_post_summary_config(yaml_path)
        assert defaults.post_base_path == Path("/some/base")
        assert runs[0].name == "case_a"


class TestLoadYamlConfigDispatch:
    def test_post_summary_kind_delegates(self, tmp_path):
        yaml_path = tmp_path / "post_summary.yaml"
        write_yaml(_base_post_summary_data(), yaml_path)
        defaults, runs = load_yaml_config(yaml_path, kind="post-summary")
        assert defaults.post_base_path == Path("/some/base")
        assert len(runs) == 1

    def test_run_kind_requires_exact_path_or_full_triplet(self, tmp_path):
        yaml_path = tmp_path / "run.yaml"
        write_yaml(
            {
                "default_settings": {},
                "runs": [{"run_base": "/x"}],
            },
            yaml_path,
        )
        with pytest.raises(ConfigError, match="exact_path"):
            load_yaml_config(yaml_path, kind="run")

    def test_run_kind_builds_settings_with_defaults(self, tmp_path):
        yaml_path = tmp_path / "run.yaml"
        write_yaml(
            {
                "default_settings": {"post_base_path": "/base", "max_depth": 3},
                "runs": [{"exact_path": "/some/exact/path"}],
            },
            yaml_path,
        )
        defaults, runs = load_yaml_config(yaml_path, kind="run")
        assert defaults.max_depth == 3
        assert defaults.merge_gap_ns == 1000
        assert runs[0].exact_path == Path("/some/exact/path")

    def test_invalid_kind_raises_value_error(self, tmp_path):
        yaml_path = tmp_path / "run.yaml"
        write_yaml({"default_settings": {}, "runs": []}, yaml_path)
        with pytest.raises(ValueError, match="Invalid config kind"):
            load_yaml_config(yaml_path, kind="bogus")
