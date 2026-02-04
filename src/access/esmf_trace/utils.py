from contextlib import suppress
from pathlib import Path


def output_name_to_index(p: str | Path) -> int | None:
    """
    'output003' -> 3
    """
    name = p.name if isinstance(p, Path) else str(p)
    if name.startswith("output"):
        try:
            return int(name.replace("output", ""))
        except ValueError:
            return None
    return None


def output_dir_to_index(p: Path) -> int | None:
    return output_name_to_index(p.name)


def extract_index_list(s: str | None) -> list[int] | None:
    """
    Parse '0,2-4,9' -> [0,2,3,4,9]
    """
    if not s:
        return None
    out = set()
    for part in s.split(","):
        part = part.strip()
        if not part:
            continue
        if "-" in part:
            a, b = part.split("-", 1)
            start = int(a.strip())
            end = int(b.strip())
            out.update(range(start, end + 1))
        else:
            out.add(int(part))
    return sorted(out)


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


def discover_pet_indices(traceout_path: Path, prefix: str) -> list[int]:
    """
    Discover pet indices from traceout directory.
    """
    traceout_path = Path(traceout_path).expanduser().resolve()
    pets = []
    for p in traceout_path.glob(f"{prefix}_*"):
        with suppress(ValueError):
            pets.append(int(p.name.split("_")[-1]))
    return sorted(set(pets))


def construct_stream_paths(traceout_path: Path, pet_indices: list[int], prefix: str = "esmf_stream") -> list[Path]:
    """
    Build stream paths from traceout path and pet indices.
    """
    traceout_path = Path(traceout_path).expanduser().resolve()
    return [traceout_path / f"{prefix}_{p:04d}" for p in pet_indices]
