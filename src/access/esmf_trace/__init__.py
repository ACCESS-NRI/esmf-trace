"""
esmf-trace package.
"""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("esmf_trace")
except PackageNotFoundError:
    # package is not installed
    pass