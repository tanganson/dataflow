"""Django views for Dataflow Manager web UI."""
import os
import json
import tempfile

from django.shortcuts import render, redirect
from django.http import Http404, HttpResponse
from django.contrib import messages
from django.core.serializers.json import DjangoJSONEncoder
from django.core.paginator import Paginator, EmptyPage
from django.db import connection, transaction
from django.views.decorators.http import require_POST

from dataflow.db_explorer import (
    is_create_missing_table_allowed,
    is_row_replace_allowed,
    is_schema_drop_allowed,
)

PAGE_SIZE = 50
PAGE_SIZE_OPTIONS = [50, 100, 200]
SUPPORTED_UPLOAD_EXTENSIONS = ('.csv', '.xlsx', '.xls', '.json', '.parquet', '.feather')


def _save_uploaded_file(uploaded_file, suffix):
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        for chunk in uploaded_file.chunks():
            tmp.write(chunk)
        tmp_path = tmp.name
    if hasattr(uploaded_file, 'seek'):
        uploaded_file.seek(0)
    return tmp_path



def _db_field_type_name(column):
    try:
        return connection.introspection.data_types_reverse[column.type_code]
    except KeyError:
        return ''


def _is_empty_import_value(value):
    if value is None:
        return True
    try:
        import pandas as pd
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        pass
    return isinstance(value, str) and value.strip() == ''


def _fallback_for_column(column):
    type_name = _db_field_type_name(column)
    if type_name in (
        'CharField',
        'TextField',
        'SlugField',
        'EmailField',
        'URLField',
        'FileField',
        'ImageField',
    ):
        return ''
    if type_name == 'BooleanField':
        return False
    if type_name in (
        'IntegerField',
        'BigIntegerField',
        'SmallIntegerField',
        'PositiveIntegerField',
        'PositiveSmallIntegerField',
        'FloatField',
        'DecimalField',
    ):
        return 0
    if type_name == 'JSONField':
        return {}
    return None


def _normalize_db_value_for_column(value, column):
    if not _is_empty_import_value(value):
        return value
    if column.null_ok:
        return None
    return _fallback_for_column(column)


def _reset_table_sequence(table_name, pk_column):
    if connection.vendor != 'postgresql' or not pk_column:
        return

    quoted_table = connection.ops.quote_name(table_name)
    quoted_pk = connection.ops.quote_name(pk_column)
    with connection.cursor() as cursor:
        cursor.execute('SELECT pg_get_serial_sequence(%s, %s)', [table_name, pk_column])
        sequence_name = cursor.fetchone()[0]
        if not sequence_name:
            return
        cursor.execute(
            """
            SELECT setval(
                %s,
                COALESCE((SELECT MAX({quoted_pk}) FROM {quoted_table}), 1),
                (SELECT MAX({quoted_pk}) FROM {quoted_table}) IS NOT NULL
            )
            """.format(quoted_pk=quoted_pk, quoted_table=quoted_table),
            [sequence_name],
        )


def _import_uploaded_csv_to_table(uploaded_file, table_name, replace=False, dry_run=False):
    ext = os.path.splitext(uploaded_file.name)[1].lower()
    if ext not in ('.csv', '.json'):
        raise ValueError('Only CSV and JSON files can be imported into existing DB tables.')

    table_name = table_name.strip()
    if not table_name:
        raise ValueError('Target table name is required.')
    if table_name not in connection.introspection.table_names():
        raise ValueError(f'Target table "{table_name}" does not exist.')
    if replace and not is_row_replace_allowed(table_name):
        raise ValueError(f'Table "{table_name}" cannot be row-replaced by Dataflow.')

    tmp_path = _save_uploaded_file(uploaded_file, ext)
    try:
        import pandas as pd

        df = pd.read_csv(tmp_path) if ext == '.csv' else pd.read_json(tmp_path)
        records = df.to_dict(orient='records')

        with connection.cursor() as cursor:
            raw_cols = connection.introspection.get_table_description(cursor, table_name)
            pk_column = connection.introspection.get_primary_key_column(cursor, table_name)

        table_columns = [col.name for col in raw_cols]
        column_map = {col.name: col for col in raw_cols}
        csv_columns = [str(col) for col in df.columns]
        insert_columns = [col for col in csv_columns if col in table_columns]
        skipped_columns = [col for col in csv_columns if col not in table_columns]

        if pk_column in insert_columns:
            pk_values = [row.get(pk_column) for row in records]
            if all(_is_empty_import_value(value) for value in pk_values):
                insert_columns.remove(pk_column)

        for col in raw_cols:
            if col.name in insert_columns or col.name == pk_column:
                continue
            if not col.null_ok and _fallback_for_column(col) is not None:
                insert_columns.append(col.name)

        if records and not insert_columns:
            raise ValueError(f'No CSV columns match columns in table "{table_name}".')

        result = {
            'table': table_name,
            'rows': len(records),
            'columns': len(insert_columns),
            'skipped_columns': skipped_columns,
            'dry_run': dry_run,
        }

        if dry_run:
            return result

        quoted_table = connection.ops.quote_name(table_name)
        quoted_cols = [connection.ops.quote_name(col) for col in insert_columns]
        placeholders = ', '.join(['%s'] * len(insert_columns))
        insert_sql = (
            f'INSERT INTO {quoted_table} ({", ".join(quoted_cols)}) '
            f'VALUES ({placeholders})'
        )
        values = [
            [_normalize_db_value_for_column(row.get(col), column_map[col]) for col in insert_columns]
            for row in records
        ]

        with transaction.atomic():
            with connection.cursor() as cursor:
                if replace:
                    cursor.execute(f'DELETE FROM {quoted_table}')
                if values:
                    cursor.executemany(insert_sql, values)
            _reset_table_sequence(table_name, pk_column)

        return result
    finally:
        os.unlink(tmp_path)


def _drop_db_table(table_name, cascade=False):
    if not is_schema_drop_allowed(table_name):
        raise ValueError(f'Table "{table_name}" is protected from schema drop.')
    quoted = connection.ops.quote_name(table_name)
    cascade_sql = ' CASCADE' if cascade and connection.vendor == 'postgresql' else ''
    with connection.cursor() as cursor:
        cursor.execute(f'DROP TABLE IF EXISTS {quoted}{cascade_sql}')


def _extract_sql_table_name(stmt, verb):
    import re
    match = re.search(
        rf'^\s*{verb}\s+TABLE\s+(?:IF\s+(?:NOT\s+)?EXISTS\s+)?["\']?([A-Za-z_][A-Za-z0-9_]*)["\']?',
        stmt,
        re.IGNORECASE,
    )
    return match.group(1) if match else None


def _validate_sql_statement_safety(stmt):
    for verb in ('DROP', 'ALTER', 'TRUNCATE'):
        table_name = _extract_sql_table_name(stmt, verb)
        if table_name and not is_schema_drop_allowed(table_name):
            raise ValueError(f'SQL {verb} TABLE is not allowed for protected table "{table_name}".')

    table_name = _extract_sql_table_name(stmt, 'CREATE')
    if table_name and not is_create_missing_table_allowed(table_name):
        raise ValueError(f'SQL CREATE TABLE is not allowed for protected table "{table_name}".')


def _sql_type_for_series(series):
    import pandas as pd

    if pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    if pd.api.types.is_integer_dtype(series):
        return 'BIGINT'
    if pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION' if connection.vendor == 'postgresql' else 'REAL'
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMPTZ'
    if series.dtype == object:
        sample = series.dropna().head(10)
        if len(sample) > 0 and sample.astype(str).str.match(
            r'^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}'
        ).all():
            return 'TIMESTAMPTZ'
    return 'TEXT'


def _create_table_from_csv(uploaded_file, table_name):
    ext = os.path.splitext(uploaded_file.name)[1].lower()
    if ext not in ('.csv', '.json'):
        raise ValueError('Only CSV and JSON files can create missing DB tables.')

    from dataflow.db_explorer import SYSTEM_TABLE_PREFIXES, DATAFLOW_CORE_TABLE_PREFIXES
    for prefix in SYSTEM_TABLE_PREFIXES + DATAFLOW_CORE_TABLE_PREFIXES:
        if table_name.startswith(prefix):
            raise ValueError(f'Table name "{table_name}" is protected and cannot be created.')

    import pandas as pd
    tmp_path = _save_uploaded_file(uploaded_file, ext)
    try:
        df = pd.read_csv(tmp_path, nrows=200) if ext == '.csv' else pd.read_json(tmp_path).head(200)
        columns = [str(col) for col in df.columns]
        if not columns:
            raise ValueError(f'Cannot create table "{table_name}" from a file with no columns.')

        col_defs = []
        used = set()
        for col_name in columns:
            if col_name in used:
                continue
            used.add(col_name)
            quoted_col = connection.ops.quote_name(col_name)
            sql_type = _sql_type_for_series(df[col_name])
            pk_sql = ' PRIMARY KEY' if col_name == 'id' else ''
            col_defs.append(f'{quoted_col} {sql_type}{pk_sql}')

        quoted_table = connection.ops.quote_name(table_name)
        with connection.cursor() as cursor:
            cursor.execute(f'CREATE TABLE {quoted_table} ({", ".join(col_defs)})')
    finally:
        os.unlink(tmp_path)


def _table_dependencies(table_names):
    selected = set(table_names)
    dependencies = {table_name: set() for table_name in table_names}
    with connection.cursor() as cursor:
        for table_name in table_names:
            constraints = connection.introspection.get_constraints(cursor, table_name)
            for constraint in constraints.values():
                foreign_key = constraint.get('foreign_key')
                if foreign_key:
                    parent_table = foreign_key[0]
                    if parent_table in selected:
                        dependencies[table_name].add(parent_table)
    return dependencies


def _sort_tables_for_import(table_names):
    dependencies = _table_dependencies(table_names)
    remaining = list(table_names)
    ordered = []

    while remaining:
        ready = [
            table_name for table_name in remaining
            if dependencies[table_name].isdisjoint(remaining)
        ]
        if not ready:
            ordered.extend(remaining)
            break
        for table_name in ready:
            ordered.append(table_name)
            remaining.remove(table_name)

    return ordered


def _delete_table_rows_for_replace(table_names):
    with transaction.atomic():
        with connection.cursor() as cursor:
            for table_name in reversed(table_names):
                if not is_row_replace_allowed(table_name):
                    raise ValueError(f'Table "{table_name}" cannot be row-replaced by Dataflow.')
                quoted_table = connection.ops.quote_name(table_name)
                cursor.execute(f'DELETE FROM {quoted_table}')


def _get_page_size(request):
    raw = request.GET.get('page_size', str(PAGE_SIZE)).lower()
    if raw == 'all':
        return None, 'all'
    try:
        page_size = int(raw)
    except ValueError:
        return PAGE_SIZE, str(PAGE_SIZE)
    if page_size not in PAGE_SIZE_OPTIONS:
        return PAGE_SIZE, str(PAGE_SIZE)
    return page_size, str(page_size)


def _page_size_context(page_size_param):
    return {
        'page_size': page_size_param,
        'page_size_options': PAGE_SIZE_OPTIONS,
        'showing_all': page_size_param == 'all',
    }


def _prepare_dataframe_for_excel(df):
    """Convert timezone-aware datetimes and non-Excel-safe types for Excel export."""
    import pandas as pd
    from datetime import datetime, time
    from decimal import Decimal

    if df.empty:
        return df

    df = df.copy()
    for column in df.columns:
        series = df[column]
        if pd.api.types.is_datetime64tz_dtype(series):
            df[column] = series.dt.tz_convert(None)
            continue

        if series.dtype == 'object':
            def _safe(value):
                if isinstance(value, datetime) and value.tzinfo is not None:
                    return value.replace(tzinfo=None)
                if isinstance(value, time):
                    return value.strftime('%H:%M:%S')
                if isinstance(value, Decimal):
                    return float(value)
                return value
            df[column] = series.map(_safe)
    return df


def _dataframe_records_for_json(df):
    """Return JSON-safe records without pandas datetime serialization pitfalls."""
    import datetime
    from decimal import Decimal
    if df.empty:
        return []
    clean = df.astype(object).where(df.notna(), None)
    records = clean.to_dict(orient='records')
    for row in records:
        for k, v in row.items():
            if isinstance(v, (datetime.date, datetime.datetime, datetime.time)):
                row[k] = v.isoformat()
            elif isinstance(v, Decimal):
                row[k] = str(v)
    return records


# ── Upload & Run ──
def upload(request):
    if request.method == 'POST':
        file = request.FILES.get('file')
        folder_files = request.FILES.getlist('folder_files')
        name = request.POST.get('name', '').strip()
        target_mode = request.POST.get('target_mode')
        rules_file = request.FILES.get('rules_file')
        replace = request.POST.get('replace') == 'on'
        dry_run = request.POST.get('dry_run') == 'on'
        create_missing_tables = request.POST.get('create_missing_tables', 'on') == 'on'

        if not file and not folder_files:
            messages.error(request, 'Please select a file or a folder.')
            return render(request, 'dataflow/upload.html')

        target_mode = 'table'

        rules_path = None
        if rules_file:
            rules_path = _save_uploaded_file(rules_file, '.json')

        if folder_files:
            importable_exts = {'.csv', '.json'}
            csv_files = [
                uploaded_file for uploaded_file in folder_files
                if os.path.splitext(uploaded_file.name)[1].lower() in importable_exts
            ]

            if not csv_files:
                if rules_path:
                    os.unlink(rules_path)
                messages.error(request, 'No CSV or JSON files were found in the selected folder.')
                return render(request, 'dataflow/upload.html')

            imported = []
            failed = []
            skipped_missing = []
            created_missing = []
            try:
                if target_mode == 'table':
                    existing_tables = set(connection.introspection.table_names())
                    table_files = {}
                    for uploaded_file in csv_files:
                        table_name = os.path.splitext(os.path.basename(uploaded_file.name))[0]
                        if table_name not in existing_tables:
                            if create_missing_tables and not dry_run:
                                try:
                                    _create_table_from_csv(uploaded_file, table_name)
                                    existing_tables.add(table_name)
                                    created_missing.append(table_name)
                                except Exception as exc:
                                    failed.append(f'{uploaded_file.name}: Could not create table "{table_name}": {exc}')
                                    continue
                            else:
                                skipped_missing.append(f'{uploaded_file.name}: "{table_name}"')
                                continue
                        table_files[table_name] = uploaded_file

                    ordered_tables = _sort_tables_for_import(list(table_files.keys()))
                    if replace and ordered_tables and not dry_run:
                        try:
                            _delete_table_rows_for_replace(ordered_tables)
                        except Exception as exc:
                            failed.append(f'Replace selected tables: {exc}')
                            ordered_tables = []

                    for table_name in ordered_tables:
                        uploaded_file = table_files[table_name]
                        try:
                            imported.append(_import_uploaded_csv_to_table(
                                uploaded_file,
                                table_name,
                                replace=False,
                                dry_run=dry_run,
                            ))
                        except Exception as exc:
                            failed.append(f'{uploaded_file.name}: {exc}')
            finally:
                if rules_path:
                    os.unlink(rules_path)

            if imported:
                action = 'Dry-run checked' if dry_run else 'Imported'
                total_rows = sum(item['rows'] for item in imported)
                messages.success(
                    request,
                    f'{action} {len(imported)} existing DB table(s) from folder: {total_rows} rows.'
                )

            if failed:
                sample = '; '.join(failed[:5])
                extra = f' ({len(failed) - 5} more)' if len(failed) > 5 else ''
                messages.warning(request, f'{len(failed)} file(s) failed: {sample}{extra}')

            if created_missing:
                sample = ', '.join(created_missing[:5])
                extra = f' ({len(created_missing) - 5} more)' if len(created_missing) > 5 else ''
                messages.info(request, f'Created {len(created_missing)} missing DB table(s): {sample}{extra}')

            if skipped_missing:
                sample = '; '.join(skipped_missing[:5])
                extra = f' ({len(skipped_missing) - 5} more)' if len(skipped_missing) > 5 else ''
                messages.info(request, f'Skipped {len(skipped_missing)} CSV file(s) with no matching DB table: {sample}{extra}')

            return redirect('dataflow:db_explorer')

        ext = os.path.splitext(file.name)[1].lower() or '.csv'

        # ── SQL file import: execute directly against database ──
        if ext == '.sql':
            if rules_path:
                os.unlink(rules_path)
            sql_content = file.read().decode('utf-8')
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]

            # Extract table name from CREATE TABLE for replace logic
            table_name = None
            for stmt in statements:
                if stmt.upper().startswith('CREATE TABLE'):
                    table_name = _extract_sql_table_name(stmt, 'CREATE')
                    break

            try:
                for stmt in statements:
                    _validate_sql_statement_safety(stmt)
            except ValueError as exc:
                messages.error(request, str(exc))
                return redirect('dataflow:upload')

            if replace and table_name:
                try:
                    _drop_db_table(table_name, cascade=True)
                except ValueError as exc:
                    messages.error(request, str(exc))
                    return redirect('dataflow:upload')

            executed = 0
            errors = []
            with connection.cursor() as cursor:
                for stmt in statements:
                    if not stmt or stmt.startswith('--'):
                        continue
                    try:
                        cursor.execute(stmt)
                        executed += 1
                    except Exception as e:
                        errors.append(str(e))

            if errors:
                hint = ''
                if any('already exists' in e for e in errors):
                    hint = ' Check "Replace existing" and try again to drop the table first.'
                messages.warning(request,
                    f'Executed {executed} SQL statement(s) with {len(errors)} error(s): {"; ".join(errors[:3])}' + hint)
            else:
                messages.success(request,
                    f'Executed {executed} SQL statement(s) from "{file.name}". '
                    f'Table restored to DB Explorer.')
            return redirect('dataflow:db_explorer')

        if ext not in SUPPORTED_UPLOAD_EXTENSIONS:
            if rules_path:
                os.unlink(rules_path)
            messages.error(request, f'Unsupported file format: {ext}')
            return render(request, 'dataflow/upload.html')

        try:
            table_name = name or os.path.splitext(os.path.basename(file.name))[0]
            created_table = False
            if table_name not in connection.introspection.table_names():
                if create_missing_tables and not dry_run:
                    _create_table_from_csv(file, table_name)
                    created_table = True
                else:
                    raise ValueError(f'Target table "{table_name}" does not exist.')

            result = _import_uploaded_csv_to_table(file, table_name, replace=replace, dry_run=dry_run)
            action = 'Dry-run checked' if dry_run else 'Imported'
            skipped = ''
            if result['skipped_columns']:
                skipped = f' Skipped columns not in table: {", ".join(result["skipped_columns"][:5])}.'
            created = f' Created missing table "{table_name}".' if created_table else ''
            messages.success(request,
                f'{action} "{file.name}" into table "{result["table"]}": '
                f'{result["rows"]} rows, {result["columns"]} matched columns.{created}{skipped}')
            if dry_run:
                return redirect('dataflow:db_explorer')
            return redirect('dataflow:db_explorer_table', table_name=result['table'])
        except Exception as exc:
            messages.error(request, str(exc))
        finally:
            if rules_path:
                os.unlink(rules_path)

    return render(request, 'dataflow/upload.html')


# ── Database Explorer ──

def db_explorer(request):
    from .db_explorer import get_all_tables

    tables = get_all_tables()

    stats = {
        'total': len(tables),
        'application': sum(1 for t in tables if t['category'] == 'django-app'),
        'dataflow': sum(
            1 for t in tables
            if t['category'] in ('dataflow-core', 'dataflow-managed')
        ),
        'system': sum(1 for t in tables if t['category'] == 'system'),
        'third_party': sum(1 for t in tables if t['category'] == 'external'),
        'total_rows': sum(t['row_count'] for t in tables),
    }

    return render(request, 'dataflow/db_explorer.html', {
        'tables': tables,
        'stats': stats,
    })


@require_POST
def db_explorer_bulk_action(request):
    action = request.POST.get('bulk_action', '').strip()
    selected_tables = request.POST.getlist('table_names')
    existing_tables = set(connection.introspection.table_names())
    table_names = [name for name in selected_tables if name in existing_tables]

    if not table_names:
        messages.warning(request, 'No database tables selected.')
        return redirect('dataflow:db_explorer')

    if action == 'export_csv' or action == 'export_xlsx' or action == 'export_json':
        import io, zipfile
        import pandas as pd
        fmt = action.split('_')[1]
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for table_name in table_names:
                quoted = connection.ops.quote_name(table_name)
                with connection.cursor() as cursor:
                    cursor.execute(f'SELECT * FROM {quoted}')
                    cols = [d[0] for d in cursor.description]
                    rows = cursor.fetchall()
                df = pd.DataFrame(rows, columns=cols)
                if fmt == 'csv':
                    zf.writestr(f'{table_name}.csv', df.to_csv(index=False))
                elif fmt == 'xlsx':
                    xbuf = io.BytesIO()
                    df = _prepare_dataframe_for_excel(df)
                    with pd.ExcelWriter(xbuf, engine='openpyxl') as w:
                        df.to_excel(w, index=False)
                    zf.writestr(f'{table_name}.xlsx', xbuf.getvalue())
                elif fmt == 'json':
                    zf.writestr(f'{table_name}.json', df.to_json(orient='records', indent=2))
        resp = HttpResponse(buf.getvalue(), content_type='application/zip')
        resp['Content-Disposition'] = f'attachment; filename="export_{fmt}.zip"'
        return resp

    elif action == 'drop':
        protected = [name for name in table_names if not is_schema_drop_allowed(name)]
        droppable = [name for name in table_names if is_schema_drop_allowed(name)]

        for table_name in droppable:
            _drop_db_table(table_name, cascade=True)

        if droppable:
            messages.success(request, f'Dropped {len(droppable)} selected table(s).')
        if protected:
            messages.warning(
                request,
                f'Skipped {len(protected)} protected table(s): {", ".join(protected[:5])}.'
            )

    elif action == 'truncate':
        truncated = []
        protected = []
        fk_errors = []
        for table_name in table_names:
            if not is_row_replace_allowed(table_name):
                protected.append(table_name)
                continue
            quoted = connection.ops.quote_name(table_name)
            try:
                with connection.cursor() as cursor:
                    cursor.execute(f'DELETE FROM {quoted}')
                truncated.append(table_name)
            except Exception:
                fk_errors.append(table_name)
        if truncated:
            messages.success(request, f'Cleared data from {len(truncated)} table(s): {", ".join(truncated[:5])}.')
        if fk_errors:
            messages.error(request, f'Cannot clear {len(fk_errors)} table(s) due to foreign key references: {", ".join(fk_errors[:5])}. Clear the referencing tables first.')
        if protected:
            messages.warning(request, f'Skipped {len(protected)} protected table(s): {", ".join(protected[:5])}.')

    else:
        messages.error(request, 'Unsupported database table bulk action.')

    return redirect('dataflow:db_explorer')


def db_explorer_table(request, table_name):
    from .db_explorer import get_table_data, get_table_schema

    if table_name not in connection.introspection.table_names():
        raise Http404(f"Table '{table_name}' not found")

    columns = get_table_schema(table_name)
    page_size, page_size_param = _get_page_size(request)
    page_num = 1 if page_size is None else int(request.GET.get('page', 1))

    if page_size is None:
        _, total, col_names, pk_column = get_table_data(table_name, page=1, page_size=1)
        effective_page_size = total or 1
    else:
        effective_page_size = page_size

    rows, total, col_names, pk_column = get_table_data(
        table_name,
        page=page_num,
        page_size=effective_page_size,
    )

    paginator = Paginator(range(total), effective_page_size)
    try:
        page_obj = paginator.page(page_num)
    except EmptyPage:
        page_obj = paginator.page(1)

    context = {
        'table_name': table_name,
        'columns': columns,
        'row_data': rows,
        'col_names': col_names,
        'total': total,
        'page_obj': page_obj,
        'pk_column': pk_column,
        'can_drop_schema': is_schema_drop_allowed(table_name),
    }
    context.update(_page_size_context(page_size_param))
    return render(request, 'dataflow/db_explorer_table.html', context)


# ── DB Explorer: Export ──

def db_explorer_export(request, table_name):
    from .db_explorer import get_table_schema

    if table_name not in connection.introspection.table_names():
        raise Http404

    fmt = request.GET.get('format', 'csv')
    columns = get_table_schema(table_name)
    col_names = [c['name'] for c in columns]
    quoted_table = connection.ops.quote_name(table_name)
    quoted_cols = [connection.ops.quote_name(c) for c in col_names]
    select_list = ", ".join(quoted_cols) if quoted_cols else "*"

    with connection.cursor() as cursor:
        cursor.execute(f"SELECT {select_list} FROM {quoted_table}")
        rows = cursor.fetchall()

    import pandas as pd
    import io

    data = []
    for row in rows:
        d = {}
        for i, col in enumerate(col_names):
            d[col] = row[i]
        data.append(d)
    df = pd.DataFrame(data)

    if fmt == 'csv':
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        resp = HttpResponse(buf.getvalue(), content_type='text/csv')
        resp['Content-Disposition'] = f'attachment; filename="{table_name}.csv"'
    elif fmt == 'json':
        resp = HttpResponse(
            json.dumps(data, indent=2, ensure_ascii=False, cls=DjangoJSONEncoder),
            content_type='application/json')
        resp['Content-Disposition'] = f'attachment; filename="{table_name}.json"'
    elif fmt == 'sql':
        from datetime import date, datetime, time
        from decimal import Decimal
        from django.db import connection as db_conn

        pk_name = db_conn.introspection.get_primary_key_column(
            db_conn.cursor(), table_name)

        # Type code mapping
        type_map = {
            16: 'BOOLEAN', 20: 'BIGINT', 21: 'SMALLINT', 23: 'INTEGER',
            25: 'TEXT', 700: 'FLOAT', 701: 'FLOAT', 1042: 'VARCHAR',
            1043: 'VARCHAR', 1082: 'DATE', 1083: 'TIME', 1114: 'TIMESTAMP',
            1184: 'TIMESTAMPTZ', 1700: 'NUMERIC', 3802: 'JSONB',
        }

        with db_conn.cursor() as c:
            raw_cols = db_conn.introspection.get_table_description(c, table_name)

        col_defs = []
        for col in raw_cols:
            qc = db_conn.ops.quote_name(col.name)
            sql_type = type_map.get(col.type_code, 'TEXT')
            if sql_type == 'VARCHAR' and col.internal_size:
                sql_type = f'VARCHAR({col.internal_size})'
            null_cl = 'NULL' if col.null_ok else 'NOT NULL'
            pk_cl = ' PRIMARY KEY' if col.name == pk_name else ''
            col_defs.append(f'    {qc} {sql_type} {null_cl}{pk_cl}')

        sql_lines = [f'CREATE TABLE {quoted_table} (']
        sql_lines.append(',\n'.join(col_defs))
        sql_lines.append('\n);\n')

        # Generate INSERT statements
        for row in rows:
            vals = []
            for i, col in enumerate(col_names):
                val = row[i]
                if val is None:
                    vals.append('NULL')
                elif isinstance(val, bool):
                    vals.append('TRUE' if val else 'FALSE')
                elif isinstance(val, (int, float)):
                    vals.append(str(val))
                elif isinstance(val, (datetime, date, time)):
                    vals.append(f"'{val.isoformat()}'")
                elif isinstance(val, Decimal):
                    vals.append(str(float(val)))
                else:
                    escaped = str(val).replace("'", "''")
                    vals.append(f"'{escaped}'")
            sql_lines.append(f'INSERT INTO {quoted_table} VALUES ({", ".join(vals)});\n')

        resp = HttpResponse('\n'.join(sql_lines), content_type='text/plain')
        resp['Content-Disposition'] = f'attachment; filename="{table_name}.sql"'
    else:
        buf = io.BytesIO()
        df = _prepare_dataframe_for_excel(df)
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        resp = HttpResponse(
            buf.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        resp['Content-Disposition'] = f'attachment; filename="{table_name}.xlsx"'

    return resp


# ── DB Explorer: Import as Dataset ──

def db_explorer_import(request, table_name):
    return redirect('dataflow:db_explorer')


# ── DB Explorer: Delete Table ──

def db_explorer_delete(request, table_name):
    if table_name not in connection.introspection.table_names():
        raise Http404

    if request.method == 'POST':
        if not is_schema_drop_allowed(table_name):
            messages.error(
                request,
                f'Table "{table_name}" is managed by Django/Dataflow core and cannot be schema-dropped. '
                'Use row replace/import instead.'
            )
            return redirect('dataflow:db_explorer_table', table_name=table_name)

        _drop_db_table(table_name, cascade=True)

        messages.success(request, f'Table "{table_name}" dropped.')
        return redirect('dataflow:db_explorer')

    return render(request, 'dataflow/db_explorer_delete.html', {
        'table_name': table_name,
        'can_drop_schema': is_schema_drop_allowed(table_name),
    })
