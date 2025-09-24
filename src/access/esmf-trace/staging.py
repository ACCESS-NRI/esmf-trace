import os
from contextlib import contextmanager
from pathlib import Path
import shutil
import tempfile
import bt2


@contextmanager
def open_selected_streams(trace_root: Path, stream_paths: iter):
    """
    Context manager to open a temporary bundle that includes:
      - the original 'metadata'
      - the selected stream files (symlinked by basename)
    """
    trace_root = Path(trace_root).expanduser().resolve()
    meta = trace_root / "metadata"
    if not meta.is_file():
        raise FileNotFoundError(f"traceout metadata not found at: {meta}")
    streams = [Path(s).expanduser().resolve() for s in stream_paths]
    if not streams:
        raise ValueError("no stream paths provided!")
    for s in streams:
        if not s.is_file():
            raise FileNotFoundError(f"stream file not found at: {s}")

    tmpdir = Path(tempfile.mkdtemp(prefix="ctf_stage_")).resolve()
    try:
        # link metadata and the selected streams into the temp bundle
        os.symlink(meta, tmpdir / "metadata", target_is_directory=False)
        for s in streams:
            os.symlink(s, tmpdir / s.name, target_is_directory=False)

        yield bt2.TraceCollectionMessageIterator(str(tmpdir))
    finally:
        shutil.rmtree(tmpdir, ignore_errors=True)
