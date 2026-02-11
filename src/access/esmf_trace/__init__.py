"""
esmf-trace package.
"""

from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version

with suppress(PackageNotFoundError):
    __version__ = version("esmf_trace")

from access.esmf_trace.library import (
    post_summary_from_config,
    run_from_config,
)

__all__ = [
    "run_from_config",
    "post_summary_from_config",
]
