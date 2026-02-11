from typing import Literal


seconds_to_nanoseconds = 1e9

# For now, two config kinds: "run" and "post-summary" are included.
# This might be extended if we want to support more config kinds.
config_kind = Literal["run", "post-summary"]

# Common keys for both run and post-summary configs
RUN_DEFAULT_FLAG_KEYS = [
    "merge_adjacent",
    "xaxis_datetime",
    "separate_plots",
    "show_html",
]

RUN_DEFAULT_KEYS = [
    "stream_prefix",
    "model_component",
    "max_depth",
    "merge_gap_ns",
    "cmap",
    "renderer",
    "max_workers",
]

POST_SUMMARY_DEFAULT_KEYS = [
    "timeseries_suffix",
    "save_json_path",
    "stats_start_index",
    "stats_end_index",
    "pets",
    "model_component",
]
