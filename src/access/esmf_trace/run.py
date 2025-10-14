from pathlib import Path
import argparse
import pandas as pd
from .utils import extract_pets, discover_pet_indices, construct_stream_paths
from .ctf_parser import df_for_selected_streams
from .timeseries import timeseries_component
from .plotting import plot_flame_graph

def run(ns: argparse.Namespace) -> tuple[int, str]:
    """
    Parse CTF streams -> dataframe
    compute timeseries -> save it to json file
    write flame graph to html file
    """

    traceout_path = Path(ns.traceout_path).expanduser().resolve()
    post_dir = Path(ns.post_dir).expanduser().resolve()
    post_dir.mkdir(parents=True, exist_ok=True)

    pet_list = extract_pets(ns.pets)
    if pet_list is None:
        pet_list = discover_pet_indices(traceout_path, prefix=ns.stream_prefix)
        if not pet_list:
            raise ValueError(f"-- No stream files discovered in {traceout_path} with prefix {ns.stream_prefix}")

    stream_paths = construct_stream_paths(traceout_path, pet_list, ns.stream_prefix)

    df = df_for_selected_streams(
        traceout_path=traceout_path,
        stream_paths=stream_paths,
        pets=pet_list,
        merge_adjacent=ns.merge_adjacent,
        merge_gap_ns=ns.merge_gap_ns,
        max_depth=ns.max_depth,
    )

    model_component = None
    if ns.model_component:
        model_component = [s.strip() for s in str(ns.model_component).split(",") if s.strip()]

    ts = timeseries_component(
        df=df,
        model_component=model_component,
        pets=pet_list,
    )

    # save json timeseries
    json_path = post_dir / f"{ns.base_prefix}_timeseries.json"
    json_path.write_text(ts.to_json(orient="records", indent=2))
    # print(f"-- Saved timeseries json to: {json_path}")

    # save flame graph
    html_path = post_dir / f"{ns.base_prefix}_flamegraph.html"
    plot_flame_graph(
        df=df,
        pets=pet_list,
        xaxis_datetime=ns.xaxis_datetime,
        separate_plots=ns.separate_plots,
        cmap_name=ns.cmap,
        renderer=ns.renderer,
        show_html=ns.show_html,
        html_path=html_path,
    )
    # print(f"-- Saved flame graph html to: {html_path}")
    return (0, "success!")
