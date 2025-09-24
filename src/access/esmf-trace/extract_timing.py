from pathlib import Path
from collections import defaultdict
import bt2
from staging import open_selected_streams
from utils import is_event, event_ts_ns, event_field
import pandas as pd


def rows_from_bt2_iterator(it: iter, *, pet_whitelist: set[int] | None = None) -> list:
    """
    Take a bt2 message iterator and produce span rows.
    """
    region_maps = defaultdict(dict) # per-pet region_id -> name
    global_map = {}
    active = defaultdict(dict)
    depth = defaultdict(int)
    out = []

    for msg in it:
        if not is_event(msg):
            continue

        event = msg.event
        pet_id = event.stream.id # stream id is the pet id

        if pet_whitelist is not None and pet_id not in pet_whitelist:
            continue

        ts = event_ts_ns(msg)
        if ts is None:
            continue

        name = event.name

        # region management
        if name == "define_region":
            region_id = event_field(event, "regionid", "region_id", "id", default=-1)
            region_name = event_field(event, "name", "region_name", default=f"region_{region_id}")
            region_maps[pet_id][region_id] = region_name
            global_map.setdefault(region_id, region_name)
            continue
        if name in ("regionid_enter", "regionid_exit"):
            region_id = event_field(event, "regionid", "region_id", "id", default=-1)
            component = region_maps[pet_id].get(region_id, global_map.get(region_id, f"region_{region_id}"))
            name = f"{component}_{'enter' if name.endswith('enter') else 'exit'}"
        # only keep enter/exit events
        if not(name.endswith("_enter") or name.endswith("_exit")):
            continue

        component = name.rsplit("_", 1)[0]

        # pop depth stack
        if name.endswith("enter"):
            active[pet_id][component] = {
                "component": component,
                "start": ts,
                "depth": depth[pet_id],
                "pet": pet_id,
            }
            depth[pet_id] += 1
        else:
            frame = active[pet_id].pop(component, None)
            if frame:
                depth[pet_id] = max(0, depth[pet_id]-1)
                frame["end"] = ts
                frame["duration"] = ts - frame["start"]
                out.append(frame)
    return out

def df_for_selected_streams(
    trace_root: Path, 
    stream_paths: iter, 
    pets: int|list|None = None, 
    *,
    merge_adjacent: bool = True, 
    merge_gap_ns: int = 1000
    ) -> pd.DataFrame:
    """
    Columns: ['component', 'start', 'end', 'duration', 'depth', 'pet']
    """
    if pets is None:
        pet_whitelist = None
    elif isinstance(pets, int):
        pet_whitelist = {pets}
    else:
        pet_whitelist = set(pets)
    
    # parse from the subset iterator
    with open_selected_streams(trace_root, stream_paths) as it:
        rows = rows_from_bt2_iterator(it, pet_whitelist=pet_whitelist)

    df = pd.DataFrame(rows, columns=['component', 'start', 'end', 'duration', 'depth', 'pet'])
    if df.empty:
        return df

    if not merge_adjacent:
        return df.sort_values(by=['pet', 'start']).reset_index(drop=True)

    # merge adjacent spans on the same pet/component/depth
    df = df.sort_values(["pet", "start"]).reset_index(drop=True)
    merged = []
    current = None

    for r in df.itertuples(index=False):
        if (
            current is not None and
            r.pet == current["pet"] and
            r.component == current["component"] and
            r.depth == current["depth"] and
            r.start - current["end"] <= merge_gap_ns
        ):
            current["end"] = r.end
            current["duration"] += r.duration
        else:
            if current is not None:
                merged.append(current)
            current = r._asdict()
    if current is not None:
        merged.append(current)
    return pd.DataFrame(merged, columns=['component', 'start', 'end', 'duration', 'depth', 'pet'])
