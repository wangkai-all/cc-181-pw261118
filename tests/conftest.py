import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from posdel.schema import parse_schema  # noqa: E402
from posdel.store import Store  # noqa: E402


SCHEMA_FIELDS = [
    {"name": "id", "type": "int64"},
    {"name": "name", "type": "string"},
    {"name": "score", "type": "float64"},
    {"name": "active", "type": "bool"},
]


@pytest.fixture
def schema():
    return parse_schema(SCHEMA_FIELDS)


def make_rows(n, start=0):
    return [
        {"id": i, "name": f"row-{i}", "score": i * 1.25, "active": i % 2 == 0}
        for i in range(start, start + n)
    ]


@pytest.fixture
def store(tmp_path, schema):
    s = Store(str(tmp_path / "lake"))
    s.init(schema, make_rows(12), chunk_rows=4)
    return s


def visible_ids(result):
    return [r["data"]["id"] for r in result["rows"]]


def visible_rows(store, sid=None, **kw):
    out = []
    offset = 0
    while True:
        page = store.query(sid, mode="visible", offset=offset, limit=1000, **kw)
        out.extend(page["rows"])
        offset += len(page["rows"])
        if offset >= page["total"]:
            break
    return out
