import bt2

# bt2 utils
def is_event(msg):
    """
    Check if a bt2 message is an event message.
    """
    return isinstance(msg, bt2._EventMessageConst)

def event_ts_ns(msg: bt2._MessageConst) -> int | None:
    """
    Timestamp (ns_from_origin) for event messages
    """
    if not is_event(msg):
        return None
    cs = msg.default_clock_snapshot
    return None if cs is None else cs.ns_from_origin

def _payload(event):
    """
    Get the payload_field mapping from an event.
    """
    pf = getattr(event, "payload_field", None)
    if pf is None:
        raise KeyError(f"{event} has no payload_field")
    return pf

def parse_define_region(event) -> tuple[int, str]:
    """
    Parse a `define_region` event.
    """
    pf = _payload(event)
    region_id = int(pf["id"])
    region_name = str(pf["name"])
    return region_id, region_name

def parse_region_transition(event) -> int:
    """
    Parse a `regionid_enter` or `regionid_exit` event.
    """
    pf = _payload(event)
    return int(pf["regionid"])