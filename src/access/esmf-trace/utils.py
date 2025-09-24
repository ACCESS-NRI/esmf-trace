from pathlib import Path
import pandas as pd
from plotting import plot_flame_graph

def parse_stream_pet_indices(stream_pet_str: str) -> int:
    """
    Parse indices like '0,3-5,8' which gives [0,3,4,5,8]
    """
    pets = []
    for part in stream_pet_str.split(","):
        if "-" in part:
            start, end = part.split("-", 1)
            pets.extend(range(int(start), int(end)+1))
        else:
            pets.append(int(part))
    return sorted(set(pets))

def parse_pets(pets_str: str | None) -> int | list[int] | None:
    """
    Parse indices like '0,3-5,8' which gives [0,3,4,5,8]
    Or None means all pets - not recommanded for large traces - very very very slow!
    """
    return None if pets_str is None else sorted(set(parse_stream_pet_indices(pets_str)))

def construct_stream_paths(
    traceout_path: Path,
    pet_indices: list[int],
    prefix: str="esmf_stream"
    ) -> list[str]:

    traceout_path = Path(traceout_path).expanduser().resolve()
    return [traceout_path / f"{prefix}_{p:04d}" for p in pet_indices]

def write_df(df: pd.DataFrame, output_path: Path) -> None:
    # TODO: currently only CSV supported, maybe adding others?
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.suffix.lower() == ".csv":
        df.to_csv(output_path, index=False)
        print(f"wrote {output_path} to file!")
    else:
        raise ValueError(f"unsupported output file type: {output_path.suffix}"
)

def plot_html(
    df: pd.DataFrame,
    pets: list[int] | None,
    *,
    xaxis_datetime: bool,
    separate_plots: bool,
    cmap: str,
    renderer: str | None,
    show_html: bool,
    write_to_html: Path,
) -> None:
    write_to_html.parent.mkdir(parents=True, exist_ok=True)
    plot_flame_graph(
        df,
        pets=pets,
        xaxis_datetime=xaxis_datetime,
        separate_plots=separate_plots,
        cmap_name=cmap,
        renderer=renderer,
        show_html=show_html,
        write_to_html=write_to_html,
    )
    print(f"wrote {write_to_html} to file!")
