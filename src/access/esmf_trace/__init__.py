"""
esmf-trace package.
"""

from contextlib import suppress
from importlib.metadata import PackageNotFoundError, version

with suppress(PackageNotFoundError):
    __version__ = version("esmf_trace")
