import shutil
import tarfile
import tempfile
from collections import defaultdict
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

from .bt2_utils import _import_bt2, event_ts_ns, is_event, parse_define_region, parse_region_transition
from .utils import stream_file_archive_path


def rows_from_bt2_iterator(it: iter, *, pet_whitelist: set[int] | None = None) -> list:
    """
    Take a bt2 message iterator and produce span rows.
    """
    region_maps = defaultdict(dict)  # per-pet region_id -> name
    global_map = {}  # global fallback mapping from region_id -> region_name
    active = defaultdict(dict)
    stacks = defaultdict(list)  # per-pet call stack of component names in order with hierarchical
    depth = defaultdict(int)
    out = []

    for msg in it:
        if not is_event(msg):
            continue

        event = msg.event
        pet_id = event.stream.id  # stream.id is the pet id

        if pet_whitelist is not None and pet_id not in pet_whitelist:
            continue

        ts = event_ts_ns(msg)
        if ts is None:
            continue

        name = event.name

        # region management
        if name == "define_region":
            region_id, region_name = parse_define_region(event)
            region_maps[pet_id][region_id] = region_name
            global_map.setdefault(region_id, region_name)
            continue
        if name in ("regionid_enter", "regionid_exit"):
            region_id = parse_region_transition(event)
            component = region_maps[pet_id].get(region_id, global_map.get(region_id, f"region_{region_id}"))
            name = f"{component}_{'enter' if name.endswith('enter') else 'exit'}"
        # only keep enter/exit events
        if not (name.endswith("_enter") or name.endswith("_exit")):
            continue

        component = name.rsplit("_", 1)[0]

        if name.endswith("enter"):
            # push on stack
            stacks[pet_id].append(component)
            active[pet_id][component] = {
                "component": component,
                "start": ts,
                "depth": depth[pet_id],
                "pet": pet_id,
                "model_component": "/".join(stacks[pet_id]),
            }
            depth[pet_id] += 1
        else:
            frame = active[pet_id].pop(component, None)
            depth[pet_id] = max(0, depth[pet_id] - 1)
            if frame:
                frame["end"] = ts
                frame["duration_s"] = ts - frame["start"]
                out.append(frame)

            if stacks[pet_id] and stacks[pet_id][-1] == component:
                stacks[pet_id].pop()
    return out


def _archive_members(archive: tarfile.TarFile) -> dict[str, tarfile.TarInfo]:
    """
    Return archive members indexed by filename.
    """
    return {member.name: member for member in archive.getmembers() if member.isfile()}


def _read_streams(
    traceout_path: Path,
    stream_paths: list[Path],
    pet_whitelist: set[int] | None,
    stream_archive: tarfile.TarFile | None = None,
    archive_members: dict[str, tarfile.TarInfo] | None = None,
) -> list:
    all_rows = []

    for sp in stream_paths:
        label = _suffix_int_from_stream_path(sp)

        if pet_whitelist is not None and label not in pet_whitelist:
            raise ValueError(
                f"stream path {sp} has pet index {label} which is not in the pet whitelist {pet_whitelist}!"
            )

        with open_selected_streams(
            traceout_path,
            [sp],
            stream_archive=stream_archive,
            archive_members=archive_members,
        ) as it:
            rows = rows_from_bt2_iterator(
                it,
                pet_whitelist=None,
            )

            for row in rows:
                row["pet"] = label

            all_rows.extend(rows)

    return all_rows


@contextmanager
def open_selected_streams(
    traceout_path: Path,
    stream_paths: iter,
    stream_archive: tarfile.TarFile | None = None,
    archive_members: dict[str, tarfile.TarInfo] | None = None,
):
    """
    Open a temporary CTF bundle containing:
        - the original metadata
        - selected stream files

    Loose stream files are symlinked. If a loose stream is not present,
    it is extracted from the stream archive.
    """
    bt2 = _import_bt2()
    traceout_path = Path(traceout_path).expanduser().resolve()
    metadata = traceout_path / "metadata"
    if not metadata.is_file():
        raise FileNotFoundError(f"traceout metadata not found at: {metadata}")
    streams = [Path(s).expanduser().resolve() for s in stream_paths]
    if not streams:
        raise ValueError("no stream paths provided!")

    tmpdir = Path(tempfile.mkdtemp(prefix="ctf_stage_")).resolve()
    try:
        (tmpdir / "metadata").symlink_to(metadata)

        for stream in streams:
            target = tmpdir / stream.name

            # existing loose stream files
            if stream.is_file():
                target.symlink_to(stream)
                continue

            # otherwise read it from the already-open tar archive
            if stream_archive is None or archive_members is None:
                raise FileNotFoundError(f"stream file not found at: {stream}")

            member = archive_members.get(stream.name)

            if member is None:
                raise FileNotFoundError(f"{stream.name} not found in {stream_archive.name}")

            source = stream_archive.extractfile(member)

            if source is None:
                raise FileNotFoundError(f"could not read {stream.name} from {stream_archive.name}")

            with source, target.open("wb") as destination:
                shutil.copyfileobj(source, destination)

        yield bt2.TraceCollectionMessageIterator(str(tmpdir))

    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)


def _suffix_int_from_stream_path(sp: Path) -> int | None:
    """
    Extract the pet index from a stream path like 'esmf_stream_0000'
    """
    name = Path(sp).name
    try:
        return int(name.split("_")[-1])
    except ValueError:
        return None


def df_for_selected_streams(
    traceout_path: Path,
    stream_paths: list[Path],
    pets: int | list | None = None,
    merge_adjacent: bool = False,
    merge_gap_ns: int = 1000,
    max_depth: int | None = None,
    stream_prefix: str = "esmf_stream",
) -> pd.DataFrame:
    """
    cols = ["model_component", "start", "end", "duration_s", "depth", "pet"]
    """
    if pets is None:
        pet_whitelist = None
    elif isinstance(pets, int):
        pet_whitelist = {pets}
    else:
        pet_whitelist = set(pets)

    archive_path = stream_file_archive_path(
        traceout_path,
        stream_prefix,
    )

    if any(not stream.is_file() for stream in stream_paths) and archive_path.is_file():
        with tarfile.open(archive_path, "r") as stream_archive:
            archive_members = _archive_members(stream_archive)

            all_rows = _read_streams(
                traceout_path,
                stream_paths,
                pet_whitelist,
                stream_archive,
                archive_members,
            )
    else:
        all_rows = _read_streams(
            traceout_path,
            stream_paths,
            pet_whitelist,
        )

    cols = ["model_component", "start", "end", "duration_s", "depth", "pet"]
    df = pd.DataFrame(all_rows, columns=cols)

    if df.empty:
        raise ValueError(f"-- No events parsed from {len(stream_paths)} stream(s) under {traceout_path}.")

    # print("-- pets:", sorted(df["pet"].unique().tolist()))

    if max_depth is not None:
        # keep only rows up to max_depth
        df = df[df["depth"] <= max_depth].reset_index(drop=True)
        if df.empty:
            raise ValueError(
                f"-- All events were filtered out by max_depth={max_depth}, "
                "try increasing max_depth or removing the filter."
            )

    if not merge_adjacent:
        return df

    # merge adjacent spans on the same pet/component/depth
    df = df.sort_values(["pet", "model_component", "depth", "start"]).reset_index(drop=True)
    merged = []
    current = None

    for r in df.itertuples(index=False):
        if (
            current is not None
            and r.pet == current["pet"]
            and r.model_component == current["model_component"]
            and r.depth == current["depth"]
            and r.start - current["end"] <= merge_gap_ns
        ):
            current["end"] = r.end
            current["duration_s"] += r.duration_s
        else:
            if current is not None:
                merged.append(current)
            current = r._asdict()
    if current is not None:
        merged.append(current)
    return pd.DataFrame(merged, columns=cols)
