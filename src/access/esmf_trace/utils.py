from pathlib import Path
import pandas as pd

def _expand_from_str_to_list(str_of_ints) -> list[int]:
    """
    Expand a str of int(s) like '5' or '3-7'into a list of ints.
    """
    str_of_ints = str_of_ints.strip()
    if not str_of_ints:
        return []
    
    if "-" in str_of_ints:
        start_s, end_s = str_of_ints.split("-", 1)
        start = int(start_s.strip())
        end = int(end_s.strip())
        return list(range(start, end + 1))
    return [int(str_of_ints)]

def extract_pets(pets_str: str | None) -> int | list[int] | None:
    """
    Extract pet like '0,3-5,8' -> [0,3,4,5,8].
    If pets_str is None or empty/whitespace, return None (meaning: all pets).
    """
    if pets_str is None or not pets_str.strip():
        return None

    parts = pets_str.split(",")
    out: list[int] = []
    for part in parts:
        out.extend(_expand_from_str_to_list(part))
    return sorted(set(out))

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
