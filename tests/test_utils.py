from pathlib import Path

import pytest

from access.esmf_trace.utils import (
    construct_stream_paths,
    discover_pet_indices,
    extract_index_list_from_str,
    extract_pets,
    normalise_str_list,
    output_dir_to_index,
    output_name_to_index,
)


class TestOutputNameToIndex:
    def test_parses_zero_padded_name(self):
        assert output_name_to_index("output003") == 3

    def test_parses_path(self):
        assert output_name_to_index(Path("output010")) == 10

    def test_non_matching_prefix_returns_none(self):
        assert output_name_to_index("foo003") is None

    def test_non_numeric_suffix_returns_none(self):
        assert output_name_to_index("outputABC") is None

    def test_output_dir_to_index_delegates(self, tmp_path):
        d = tmp_path / "output007"
        d.mkdir()
        assert output_dir_to_index(d) == 7


class TestExtractIndexListFromStr:
    def test_none_returns_none(self):
        assert extract_index_list_from_str(None) is None

    def test_empty_string_returns_none(self):
        assert extract_index_list_from_str("") is None

    def test_single_value(self):
        assert extract_index_list_from_str("5") == [5]

    def test_mixed_ranges_and_singletons_sorted_and_deduped(self):
        assert extract_index_list_from_str("0,2-4,9,3") == [0, 2, 3, 4, 9]

    def test_whitespace_is_tolerated(self):
        assert extract_index_list_from_str(" 0 , 2 - 4 ") == [0, 2, 3, 4]


class TestExtractPets:
    def test_none_returns_none(self):
        assert extract_pets(None) is None

    def test_whitespace_only_returns_none(self):
        assert extract_pets("   ") is None

    def test_ranges_and_singletons_sorted_and_deduped(self):
        assert extract_pets("0,3-5,8,3") == [0, 3, 4, 5, 8]


class TestNormaliseStrList:
    def test_none_returns_none(self):
        assert normalise_str_list(None) is None

    def test_list_input_strips_and_drops_blanks(self):
        assert normalise_str_list([" a ", "", "b"]) == ["a", "b"]

    def test_comma_separated_string(self):
        assert normalise_str_list("a, b ,c") == ["a", "b", "c"]

    def test_non_string_list_elements_are_stringified(self):
        assert normalise_str_list([1, 2]) == ["1", "2"]


class TestDiscoverPetIndices:
    def test_discovers_and_sorts_pets(self, tmp_path):
        for pet in (2, 0, 10):
            (tmp_path / f"esmf_stream_{pet:04d}").touch()
        (tmp_path / "unrelated_file").touch()
        assert discover_pet_indices(tmp_path, "esmf_stream") == [0, 2, 10]

    def test_no_matches_returns_empty_list(self, tmp_path):
        assert discover_pet_indices(tmp_path, "esmf_stream") == []


class TestConstructStreamPaths:
    def test_builds_zero_padded_paths(self, tmp_path):
        paths = construct_stream_paths(tmp_path, [0, 12], prefix="esmf_stream")
        assert paths == [
            tmp_path.resolve() / "esmf_stream_0000",
            tmp_path.resolve() / "esmf_stream_0012",
        ]


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("0", [0]),
        ("3-5", [3, 4, 5]),
    ],
)
def test_extract_pets_parametrised(raw, expected):
    assert extract_pets(raw) == expected
