"""posdel — a tiny position-based lake-table engine (from scratch).

This project is deliberately *not* compatible with Iceberg / Delta / Hudi or
any other lakehouse protocol.  It implements one small idea honestly:

  * data lives in immutable Parquet files;
  * deletions are stored separately as sets of *row positions* (0-based
    ordinal within a data file);
  * a query at snapshot S applies exactly the data files and deletion sets
    that were visible at S — nothing newer.

The on-disk format is described in ``docs/FORMAT.md``.
"""

from .errors import (
    Conflict,
    CorruptState,
    NotFound,
    PosdelError,
    RecoveryRequired,
    ValidationError,
)
from .store import Store, Snapshot

__all__ = [
    "Store",
    "Snapshot",
    "PosdelError",
    "Conflict",
    "NotFound",
    "CorruptState",
    "ValidationError",
    "RecoveryRequired",
]
__version__ = "0.1.0"
