import tarfile
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


def extract_index_list_from_str(s: str | None) -> list[int] | None:
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


def stream_file_archive_path(
    traceout_path: Path,
    prefix: str = "esmf_stream",
) -> Path:
    """
    Return the expected tar archive path
    """
    traceout_path = Path(traceout_path).expanduser().resolve()
    return traceout_path / f"{prefix}.tar"


def _stream_pet_index(name: str, prefix: str) -> int | None:
    """
    Extract a PET index from a stream file name like 'esmf_stream_0003' -> 3
    """
    if not name.startswith(f"{prefix}_"):
        return None

    try:
        return int(name.rsplit("_", 1)[-1])
    except ValueError:
        return None


def discover_pet_indices(traceout_path: Path, prefix: str) -> list[int]:
    """
    Discover pet indices from loose stream files and/or a stream tar archive
    """
    traceout_path = Path(traceout_path).expanduser().resolve()
    pets = set()

    # support loose stream files
    for path in traceout_path.glob(f"{prefix}_*"):
        pet = _stream_pet_index(path.name, prefix)
        if pet is not None:
            pets.add(pet)

    # support stream files consolidated by om3-scripts/archive_esmf_streams.py
    archive_path = stream_file_archive_path(traceout_path, prefix)
    if archive_path.is_file():
        with tarfile.open(archive_path, "r") as archive:
            for member in archive.getmembers():
                if not member.isfile():
                    continue

                pet = _stream_pet_index(member.name, prefix)
                if pet is not None:
                    pets.add(pet)

    return sorted(pets)


def construct_stream_paths(traceout_path: Path, pet_indices: list[int], prefix: str = "esmf_stream") -> list[Path]:
    """
    Build stream paths from traceout path and pet indices.
    """
    traceout_path = Path(traceout_path).expanduser().resolve()
    return [traceout_path / f"{prefix}_{p:04d}" for p in pet_indices]


def normalise_str_list(value: str | list[str] | None) -> list[str] | None:
    if value is None:
        return None
    if isinstance(value, list):
        return [str(v).strip() for v in value if str(v).strip()]
    return [s.strip() for s in str(value).split(",") if s.strip()]
