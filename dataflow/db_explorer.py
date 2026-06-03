"""Database introspection and safe SQL query utilities for the DB Explorer."""
from collections import OrderedDict

from django.db import connection


APP_TABLE_PREFIXES = [
    "doctors_", "listings_", "contacts_", "accounts_", "pages_",
]
SYSTEM_TABLE_PREFIXES = [
    "auth_", "django_",
]
DATAFLOW_TABLE_PREFIXES = [
    "dataflow_",
]

TYPE_CATEGORY = {
    "BigIntegerField": "numeric",
    "SmallIntegerField": "numeric",
    "IntegerField": "numeric",
    "FloatField": "numeric",
    "DecimalField": "numeric",
    "CharField": "text",
    "TextField": "text",
    "DateField": "datetime",
    "TimeField": "datetime",
    "DateTimeField": "datetime",
    "DurationField": "datetime",
    "BooleanField": "boolean",
    "JSONField": "json",
    "UUIDField": "text",
    "BinaryField": "other",
    "GenericIPAddressField": "text",
}


def _field_type_name(type_code: int) -> str:
    mapping = connection.introspection.data_types_reverse
    try:
        return mapping[type_code]
    except KeyError:
        return f"unknown({type_code})"


def _categorize(table_name: str) -> str:
    for prefix in APP_TABLE_PREFIXES:
        if table_name.startswith(prefix):
            return "application"
    for prefix in SYSTEM_TABLE_PREFIXES:
        if table_name.startswith(prefix):
            return "system"
    for prefix in DATAFLOW_TABLE_PREFIXES:
        if table_name.startswith(prefix):
            return "dataflow"
    return "third-party"


def _category_label(cat: str) -> str:
    return {
        "application": "Application",
        "dataflow": "Dataflow",
        "system": "System",
        "third-party": "Third-Party",
    }.get(cat, cat)


def _type_category(pg_type: str) -> str:
    return TYPE_CATEGORY.get(pg_type, "other")


def get_all_tables():
    """Return metadata for every user-visible table in the database."""
    all_names = connection.introspection.table_names()
    visible = list(all_names)

    tables = []
    with connection.cursor() as cursor:
        for name in sorted(visible):
            quoted = connection.ops.quote_name(name)
            cursor.execute(f"SELECT COUNT(*) FROM {quoted}")
            row_count = cursor.fetchone()[0]

            cols = connection.introspection.get_table_description(cursor, name)
            col_count = len(cols)

            tables.append({
                "name": name,
                "row_count": row_count,
                "col_count": col_count,
                "category": _categorize(name),
                "category_label": _category_label(_categorize(name)),
            })

    return tables


def get_table_schema(table_name: str):
    """Return column metadata for a table."""
    with connection.cursor() as cursor:
        raw_cols = connection.introspection.get_table_description(cursor, table_name)
        pk_name = connection.introspection.get_primary_key_column(cursor, table_name)

    columns = []
    for col in raw_cols:
        type_name = _field_type_name(col.type_code)
        columns.append({
            "name": col.name,
            "type": type_name,
            "type_category": _type_category(type_name),
            "nullable": col.null_ok,
            "is_pk": col.name == pk_name,
        })
    return columns


def get_table_data(table_name: str, page: int = 1, page_size: int = 50):
    """Return paginated rows, total count, column names, and primary key column.

    Returns (rows, total_count, column_names, pk_column).
    """
    with connection.cursor() as cursor:
        raw_cols = connection.introspection.get_table_description(cursor, table_name)
        pk_name = connection.introspection.get_primary_key_column(cursor, table_name)

    col_names = [col.name for col in raw_cols]

    if pk_name and pk_name in col_names:
        order_col = pk_name
    else:
        order_col = col_names[0] if col_names else None

    quoted_table = connection.ops.quote_name(table_name)
    quoted_cols = [connection.ops.quote_name(c) for c in col_names]
    select_list = ", ".join(quoted_cols) if quoted_cols else "*"

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT COUNT(*) FROM {quoted_table}")
        total = cursor.fetchone()[0]

        if order_col:
            quoted_order = connection.ops.quote_name(order_col)
            cursor.execute(
                f"SELECT {select_list} FROM {quoted_table} ORDER BY {quoted_order} LIMIT %s OFFSET %s",
                [page_size, (page - 1) * page_size],
            )
        else:
            cursor.execute(
                f"SELECT {select_list} FROM {quoted_table} LIMIT %s OFFSET %s",
                [page_size, (page - 1) * page_size],
            )

        rows = cursor.fetchall()

    result = []
    for row in rows:
        d = OrderedDict()
        for i, col in enumerate(col_names):
            val = row[i]
            d[col] = str(val) if val is not None else None
        result.append(d)

    return result, total, col_names, pk_name
