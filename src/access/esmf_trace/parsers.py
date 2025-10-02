from pathlib import Path
import pandas as pd


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
    Parse indices like '0,3-5,8' which gives [0,3,4,5,8];
    Or None means all pets - not recommanded for large traces - very very very slow!
    """
    return None if pets_str is None else sorted(set(parse_stream_pet_indices(pets_str)))

def construct_stream_paths(
    traceout_path: Path,
    pet_indices: list[int],
    prefix: str="esmf_stream"
    ) -> list[str]:
    """
    Build stream paths from traceout path and pet indices.
    """
    traceout_path = Path(traceout_path).expanduser().resolve()
    return [traceout_path / f"{prefix}_{p:04d}" for p in pet_indices]

# The raw output may not be useful, so commenting out for now.
# def write_df(df: pd.DataFrame, output_path: Path) -> None:
#     # TODO: currently only CSV supported, maybe adding others?
#     output_path.parent.mkdir(parents=True, exist_ok=True)
#     if output_path.suffix.lower() == ".csv":
#         df.to_csv(output_path, index=False)
#         print(f"wrote {output_path} to file!")
#     else:
#         raise ValueError(f"unsupported output file type: {output_path.suffix}"
# )
