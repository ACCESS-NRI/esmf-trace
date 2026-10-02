import io
import tarfile
from pathlib import Path

from access.esmf_trace import ctf_parser
from access.esmf_trace.ctf_parser import open_selected_streams


class _FakeIterator:
    def __init__(self, path):
        self.path = Path(path)


class _FakeBt2:
    TraceCollectionMessageIterator = _FakeIterator


def _make_stream_archive(
    traceout_path: Path,
    streams: dict[str, bytes],
) -> None:
    with tarfile.open(
        traceout_path / "esmf_stream.tar",
        "w",
    ) as archive:
        for name, content in streams.items():
            member = tarfile.TarInfo(name)
            member.size = len(content)

            archive.addfile(
                member,
                io.BytesIO(content),
            )


def test_open_loose_stream(tmp_path, monkeypatch):
    traceout = tmp_path / "traceout"
    traceout.mkdir()

    (traceout / "metadata").write_text("metadata")

    stream = traceout / "esmf_stream_0000"
    stream.write_bytes(b"loose stream")

    monkeypatch.setattr(
        ctf_parser,
        "_import_bt2",
        lambda: _FakeBt2,
    )

    with open_selected_streams(
        traceout,
        [stream],
    ) as iterator:
        assert (iterator.path / stream.name).read_bytes() == b"loose stream"


def test_open_archived_stream(tmp_path, monkeypatch):
    traceout = tmp_path / "traceout"
    traceout.mkdir()

    (traceout / "metadata").write_text("metadata")

    _make_stream_archive(
        traceout,
        {
            "esmf_stream_0000": b"archived stream",
        },
    )

    monkeypatch.setattr(
        ctf_parser,
        "_import_bt2",
        lambda: _FakeBt2,
    )

    stream = traceout / "esmf_stream_0000"

    with open_selected_streams(
        traceout,
        [stream],
    ) as iterator:
        assert (iterator.path / stream.name).read_bytes() == b"archived stream"


def test_loose_stream_takes_precedence(tmp_path, monkeypatch):
    traceout = tmp_path / "traceout"
    traceout.mkdir()

    (traceout / "metadata").write_text("metadata")

    _make_stream_archive(
        traceout,
        {
            "esmf_stream_0000": b"archived stream",
        },
    )

    stream = traceout / "esmf_stream_0000"
    stream.write_bytes(b"loose stream")

    monkeypatch.setattr(
        ctf_parser,
        "_import_bt2",
        lambda: _FakeBt2,
    )

    with open_selected_streams(
        traceout,
        [stream],
    ) as iterator:
        assert (iterator.path / stream.name).read_bytes() == b"loose stream"
