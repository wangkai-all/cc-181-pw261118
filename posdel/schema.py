"""Schema definition, row validation and predicate evaluation.

Supported column types are deliberately tiny: ``int64``, ``float64``,
``string`` and ``bool``.  Rows are plain JSON objects.  There is no schema
evolution in this project (see docs/FORMAT.md).
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from typing import Any, Iterable, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .errors import ValidationError
from .fsutil import canonical_json, sha256_text

SUPPORTED_TYPES = ("int64", "float64", "string", "bool")
ARROW_TYPES = {
    "int64": pa.int64(),
    "float64": pa.float64(),
    "string": pa.string(),
    "bool": pa.bool_(),
}

_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,63}$")
MAX_STRING_LEN = 4096


@dataclass(frozen=True)
class Field:
    name: str
    type: str

    def arrow(self) -> pa.DataType:
        return ARROW_TYPES[self.type]


@dataclass(frozen=True)
class Schema:
    fields: tuple[Field, ...]

    @property
    def hash(self) -> str:
        return sha256_text(canonical_json({"fields": [{"name": f.name, "type": f.type} for f in self.fields]}))

    @property
    def names(self) -> list[str]:
        return [f.name for f in self.fields]

    def field(self, name: str) -> Field:
        for f in self.fields:
            if f.name == name:
                return f
        raise ValidationError(f"未知列: {name!r}", column=name)

    def arrow_schema(self) -> pa.Schema:
        return pa.schema([pa.field(f.name, f.arrow(), nullable=True) for f in self.fields])

    def to_dict(self) -> dict:
        return {"fields": [{"name": f.name, "type": f.type} for f in self.fields], "hash": self.hash}


def parse_schema(raw_fields: Sequence[dict] | Sequence[Field]) -> Schema:
    if not raw_fields:
        raise ValidationError("schema 至少需要一个字段")
    fields: list[Field] = []
    seen: set[str] = set()
    for item in raw_fields:
        if isinstance(item, Field):
            name, typ = item.name, item.type
        elif isinstance(item, dict):
            name, typ = item.get("name"), item.get("type")
        else:
            raise ValidationError("字段定义必须是对象 {name,type}")
        if not isinstance(name, str) or not _NAME_RE.match(name):
            raise ValidationError(f"非法字段名: {name!r}（仅允许字母、数字、下划线，且不以数字开头）")
        if name in seen:
            raise ValidationError(f"字段重复: {name}")
        if typ not in SUPPORTED_TYPES:
            raise ValidationError(f"字段 {name} 的类型 {typ!r} 不受支持，可选: {', '.join(SUPPORTED_TYPES)}")
        seen.add(name)
        fields.append(Field(name, typ))
    return Schema(tuple(fields))


def cast_value(field: Field, value: Any) -> Any:
    """Validate/coerce one JSON value for a field.  Returns a Python value."""
    if value is None:
        return None
    typ = field.type
    if typ == "int64":
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValidationError(f"列 {field.name} 需要 int64 整数，收到 {value!r}")
        if not (-(2**63) <= value < 2**63):
            raise ValidationError(f"列 {field.name} 的整数超出 int64 范围")
        return value
    if typ == "float64":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValidationError(f"列 {field.name} 需要 float64 数值，收到 {value!r}")
        f = float(value)
        if not math.isfinite(f):
            raise ValidationError(f"列 {field.name} 不允许 NaN/Infinity")
        return f
    if typ == "string":
        if not isinstance(value, str):
            raise ValidationError(f"列 {field.name} 需要字符串，收到 {value!r}")
        if len(value) > MAX_STRING_LEN:
            raise ValidationError(f"列 {field.name} 字符串超长（上限 {MAX_STRING_LEN}）")
        if "\x00" in value:
            raise ValidationError(f"列 {field.name} 不允许 NUL 字符")
        return value
    if typ == "bool":
        if not isinstance(value, bool):
            raise ValidationError(f"列 {field.name} 需要布尔值，收到 {value!r}")
        return value
    raise ValidationError(f"未知类型 {typ}")


def normalize_rows(schema: Schema, rows: Iterable[dict]) -> list[dict]:
    if not isinstance(rows, list):
        raise ValidationError("rows 必须是数组")
    if not rows:
        raise ValidationError("rows 不能为空")
    if len(rows) > 10000:
        raise ValidationError("单次追加最多 10000 行")
    out: list[dict] = []
    expected = set(schema.names)
    for i, row in enumerate(rows):
        if not isinstance(row, dict):
            raise ValidationError(f"第 {i + 1} 行必须是对象")
        extra = set(row) - expected
        missing = expected - set(row)
        if extra:
            raise ValidationError(f"第 {i + 1} 行包含未知列: {', '.join(sorted(extra))}", row=i)
        if missing:
            raise ValidationError(f"第 {i + 1} 行缺少列: {', '.join(sorted(missing))}", row=i)
        out.append({f.name: cast_value(f, row[f.name]) for f in schema.fields})
    return out


def rows_to_table(schema: Schema, rows: Sequence[dict]) -> pa.Table:
    columns = {f.name: [r[f.name] for r in rows] for f in schema.fields}
    return pa.Table.from_pydict(columns, schema=schema.arrow_schema())


def read_parquet_schema(path: str) -> pa.Schema:
    return pq.read_schema(path)


# ---------------------------------------------------------------------------
# Predicates
# ---------------------------------------------------------------------------

NO_VALUE_OPS = {"is_null", "not_null"}
SCALAR_OPS = {"eq", "ne", "lt", "lte", "gt", "gte", "contains", "starts_with", "ends_with"}
LIST_OPS = {"in", "not_in"}
ALL_OPS = NO_VALUE_OPS | SCALAR_OPS | LIST_OPS | {"between"}
ORDERED_TYPES = {"int64", "float64", "string"}
TEXT_OPS = {"contains", "starts_with", "ends_with"}


@dataclass(frozen=True)
class Condition:
    column: str
    op: str
    value: Any = None
    second: Any = None

    def describe(self) -> str:
        if self.op == "is_null":
            return f"{self.column} 为空"
        if self.op == "not_null":
            return f"{self.column} 非空"
        if self.op == "between":
            return f"{self.column} 在 [{json_scalar(self.value)}, {json_scalar(self.second)}] 之间"
        if self.op in ("in", "not_in"):
            inside = ", ".join(json_scalar(v) for v in self.value)
            return f"{self.column} {'∈' if self.op == 'in' else '∉'} {{{inside}}}"
        symbols = {"eq": "=", "ne": "≠", "lt": "<", "lte": "≤", "gt": ">", "gte": "≥",
                   "contains": "包含", "starts_with": "前缀", "ends_with": "后缀"}
        return f"{self.column} {symbols.get(self.op, self.op)} {json_scalar(self.value)}"

    def to_dict(self) -> dict:
        d = {"column": self.column, "op": self.op}
        if self.op == "between":
            d["value"] = self.value
            d["to"] = self.second
        elif self.op not in NO_VALUE_OPS:
            d["value"] = self.value
        return d


def json_scalar(v: Any) -> str:
    if isinstance(v, str):
        return f"'{v}'"
    if isinstance(v, bool):
        return "true" if v else "false"
    return str(v)


def build_condition(schema: Schema, raw: dict) -> Condition:
    if not isinstance(raw, dict):
        raise ValidationError("条件必须是对象")
    col = raw.get("column")
    op = raw.get("op")
    if not isinstance(col, str):
        raise ValidationError("条件缺少 column")
    field = schema.field(col)
    if op not in ALL_OPS:
        raise ValidationError(f"不支持的比较操作: {op!r}", supported=sorted(ALL_OPS))

    if op in NO_VALUE_OPS:
        return Condition(col, op)

    if op == "between":
        if "value" not in raw or "to" not in raw:
            raise ValidationError("between 需要 value 和 to")
        low = cast_value(field, raw["value"])
        high = cast_value(field, raw["to"])
        if low is None or high is None:
            raise ValidationError("between 的边界不能为 null")
        if field.type not in ORDERED_TYPES:
            raise ValidationError("between 仅支持 int64/float64/string")
        return Condition(col, op, low, high)

    if op in LIST_OPS:
        values = raw.get("value")
        if not isinstance(values, list) or not values:
            raise ValidationError(f"{op} 需要非空 value 数组")
        if len(values) > 1000:
            raise ValidationError("条件值数组过大（上限 1000）")
        if field.type == "bool" and op == "in":
            pass
        casted = [cast_value(field, v) for v in values]
        if any(v is None for v in casted):
            raise ValidationError(f"{op} 的值不能为 null")
        return Condition(col, op, casted)

    if op in TEXT_OPS:
        if field.type != "string":
            raise ValidationError(f"{op} 仅适用于 string 列")
        if not isinstance(raw.get("value"), str):
            raise ValidationError(f"{op} 需要字符串 value")
        return Condition(col, op, raw["value"])

    if field.type == "bool" and op not in ("eq", "ne"):
        raise ValidationError(f"bool 列只支持 eq/ne/is_null/not_null/in/not_in，不支持 {op}")
    if field.type not in ORDERED_TYPES and op in ("lt", "lte", "gt", "gte"):
        raise ValidationError(f"{op} 仅适用于 int64/float64/string")
    if "value" not in raw:
        raise ValidationError(f"{op} 缺少 value")
    value = cast_value(field, raw["value"])
    if value is None:
        raise ValidationError(f"{op} 的 value 不能为 null（判空请用 is_null）")
    return Condition(col, op, value)


def build_conditions(schema: Schema, raws: Sequence[dict]) -> list[Condition]:
    if raws is None:
        return []
    if not isinstance(raws, list):
        raise ValidationError("conditions 必须是数组")
    if len(raws) > 32:
        raise ValidationError("条件过多（上限 32）")
    return [build_condition(schema, r) for r in raws]


def _scalar(field: Field, value: Any) -> pa.Scalar:
    return pa.scalar(value, type=field.arrow())


def _array(values: Sequence[Any], field: Field) -> pa.Array:
    return pa.array(list(values), type=field.arrow())


def condition_mask(table: pa.Table, condition: Condition, schema: Schema) -> pa.Array:
    field = schema.field(condition.column)
    arr = table.column(condition.column)
    op = condition.op
    if op == "is_null":
        mask = pc.is_null(arr)
    elif op == "not_null":
        mask = pc.is_valid(arr)
    elif op == "eq":
        mask = pc.equal(arr, _scalar(field, condition.value))
    elif op == "ne":
        mask = pc.not_equal(arr, _scalar(field, condition.value))
    elif op == "lt":
        mask = pc.less(arr, _scalar(field, condition.value))
    elif op == "lte":
        mask = pc.less_equal(arr, _scalar(field, condition.value))
    elif op == "gt":
        mask = pc.greater(arr, _scalar(field, condition.value))
    elif op == "gte":
        mask = pc.greater_equal(arr, _scalar(field, condition.value))
    elif op == "between":
        lo = pc.greater_equal(arr, _scalar(field, condition.value))
        hi = pc.less_equal(arr, _scalar(field, condition.second))
        mask = pc.and_(lo, hi)
    elif op == "in":
        mask = pc.is_in(arr, value_set=_array(condition.value, field))
    elif op == "not_in":
        mask = pc.invert(pc.is_in(arr, value_set=_array(condition.value, field)))
    elif op == "contains":
        mask = pc.match_substring(arr, condition.value)
    elif op == "starts_with":
        mask = pc.starts_with(arr, pattern=condition.value)
    elif op == "ends_with":
        mask = pc.ends_with(arr, pattern=condition.value)
    else:  # pragma: no cover - guarded by build_condition
        raise ValidationError(f"未知比较操作 {op}")
    return pc.fill_null(mask, False)


def predicate_mask(table: pa.Table, conditions: Sequence[Condition], match: str, schema: Schema) -> pa.Array:
    if not conditions:
        return pa.array([True] * table.num_rows, type=pa.bool_())
    if match not in ("all", "any"):
        raise ValidationError("match 只能是 all 或 any")
    masks = [condition_mask(table, c, schema) for c in conditions]
    if len(masks) == 1:
        return masks[0]
    return pc.and_(*masks) if match == "all" else pc.or_(*masks)


def describe_predicate(conditions: Sequence[Condition], match: str) -> str:
    if not conditions:
        return "全部行"
    joiner = " 且 " if match == "all" else " 或 "
    return joiner.join(c.describe() for c in conditions)
