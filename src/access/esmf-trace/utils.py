import bt2


def is_event(msg):
    return isinstance(msg, bt2._EventMessageConst)

def event_ts_ns(msg: bt2._MessageConst) -> int | None:
    """
    Timestamp (ns_from_origin) for event messages
    """
    if not is_event(msg):
        return None
    cs = msg.default_clock_snapshot
    return None if cs is None else cs.ns_from_origin

def event_field(event, *names: str, default=None):
    """
    Lookup a field on the event: payload first, then event context.
    """
    pf = getattr(event, "payload_field", None)
    if pf is not None:
        for n in names:
            if n in pf:
                return pf[n]
    cf = getattr(event, "context_field", None)
    if cf is not None:
        for n in names:
            if n in cf:
                return cf[n]
    return default
