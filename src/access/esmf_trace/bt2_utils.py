from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import bt2


def _import_bt2():
    """
    Import and return the bt2 module, deferred to call time rather than
    module load time. bt2 (Babeltrace2) is a native binding that isn't
    pip-installable, so importing it eagerly at module scope would make the
    rest of the package - which has nothing to do with CTF parsing -
    unimportable wherever bt2 isn't present (e.g. in most test environments).

    Raises RuntimeError with setup instructions if bt2 can't be imported.
    """
    try:
        import bt2
    except Exception as e:
        raise RuntimeError(
            "Failed to import 'bt2'. Install Babeltrace2 with Python bindings first.\n"
            " - On Gadi: module use /g/data/vk83/modules && module load model-tools/babeltrace2/2.1.2\n"
        ) from e
    return bt2


def is_event(msg):
    """
    Check if a bt2 message is an event message.
    """
    bt2 = _import_bt2()
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
