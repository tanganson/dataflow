"""Django views for Dataflow Manager web UI."""
import os
import tempfile

from django.shortcuts import render, redirect, get_object_or_404
from django.http import Http404, HttpResponse
from django.contrib import messages
from django.core.paginator import Paginator, EmptyPage
from django.db import connection
from django.db.models import Count
from django.views.decorators.csrf import ensure_csrf_cookie

from dataflow.models import Dataset, DataRecord, CleaningLog, DatasetSchema
from dataflow.schema_manager import SchemaManager
from dataflow.cli import Pipeline, clean_dataset, delete_dataset as delete_dataset_fn

PAGE_SIZE = 50


# ── Dashboard ──
@ensure_csrf_cookie
def home(request):
    datasets = Dataset.objects.annotate(
        record_count=Count('datarecord')
    ).order_by('-updated_at')

    for ds in datasets:
        ds.is_ref = ds.description and ds.description.startswith('__ref:')

    stats = {
        'total': datasets.count(),
        'records': DataRecord.objects.count(),
        'schemas': DatasetSchema.objects.count(),
        'logs': CleaningLog.objects.count(),
    }
    return render(request, 'dataflow/home.html', {
        'datasets': datasets,
        'stats': stats,
    })


# ── Upload & Run ──
def upload(request):
    if request.method == 'POST':
        file = request.FILES.get('file')
        name = request.POST.get('name', '').strip()
        rules_file = request.FILES.get('rules_file')
        replace = request.POST.get('replace') == 'on'
        dry_run = request.POST.get('dry_run') == 'on'

        if not file:
            messages.error(request, 'Please select a file.')
            return render(request, 'dataflow/upload.html')

        ext = os.path.splitext(file.name)[1].lower() or '.csv'

        # ── SQL file import: execute directly against database ──
        if ext == '.sql':
            sql_content = file.read().decode('utf-8')
            statements = [s.strip() for s in sql_content.split(';') if s.strip()]

            # Extract table name from CREATE TABLE for replace logic
            table_name = None
            for stmt in statements:
                if stmt.upper().startswith('CREATE TABLE'):
                    # Parse "CREATE TABLE "table_name" (...)"
                    import re
                    match = re.search(r'CREATE\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?["\']?(\w+)["\']?', stmt, re.IGNORECASE)
                    if match:
                        table_name = match.group(1)
                    break

            if replace and table_name:
                with connection.cursor() as cursor:
                    cursor.execute(f'DROP TABLE IF EXISTS {connection.ops.quote_name(table_name)} CASCADE')

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

        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as tmp:
            for chunk in file.chunks():
                tmp.write(chunk)
            tmp_path = tmp.name

        rules_path = None
        if rules_file:
            with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
                for chunk in rules_file.chunks():
                    tmp.write(chunk)
                rules_path = tmp.name

        try:
            pipeline = Pipeline(name or os.path.splitext(file.name)[0])
            pipeline.load_file(tmp_path)

            if rules_path:
                pipeline.load_rules_file(rules_path)
            else:
                pipeline.auto_rules()

            pipeline.clean()

            if dry_run:
                pipeline.report()
                messages.info(request,
                    f'Dry-run: {len(pipeline.valid_rows)} valid, '
                    f'{len(pipeline.raw_rows) - len(pipeline.valid_rows)} invalid '
                    f'out of {len(pipeline.raw_rows)} rows.')
                return redirect('dataflow:home')
            else:
                pipeline.store(replace=replace)
                ds = pipeline.dataset_obj
                messages.success(request,
                    f'Imported "{ds.name}": {len(pipeline.valid_rows)} rows, '
                    f'{len(pipeline.rules)} columns.')
                return redirect('dataflow:dataset_detail', dataset_id=ds.id)
        finally:
            os.unlink(tmp_path)
            if rules_path:
                os.unlink(rules_path)

    return render(request, 'dataflow/upload.html')


# ── Dataset Detail ──
def dataset_detail(request, dataset_id):
    ds = get_object_or_404(Dataset, pk=dataset_id)

    # Reference dataset: read live from source table
    is_ref = ds.description and ds.description.startswith('__ref:')
    ds.is_ref = is_ref  # for template
    ref_table = ds.description.split(':', 1)[1] if is_ref else None

    if is_ref and ref_table and ref_table in connection.introspection.table_names():
        from .db_explorer import get_table_data, get_table_schema

        columns = get_table_schema(ref_table)
        field_names = [c['name'] for c in columns]
        page_num = int(request.GET.get('page', 1))
        row_data, total_rows, col_names, pk_column = get_table_data(ref_table, page=page_num)

        paginator = Paginator(range(total_rows), PAGE_SIZE)
        try:
            rows = paginator.page(page_num)
        except EmptyPage:
            rows = paginator.page(1)

        return render(request, 'dataflow/dataset_detail.html', {
            'ds': ds,
            'schema': None,
            'field_names': field_names,
            'rows': rows,
            'row_data': row_data,
            'total_rows': total_rows,
            'logs': [],
        })

    # Original logic for uploaded datasets
    ds.is_ref = False
    schema = None
    field_names = []
    try:
        schema = ds.schema
        field_names = [f.get('name', '') for f in schema.fields_json]
    except DatasetSchema.DoesNotExist:
        pass

    model = SchemaManager.get_model_for_dataset(ds.id)
    if model:
        queryset = model.objects.all().order_by('id')
        total_rows = model.objects.count()
    else:
        queryset = DataRecord.objects.filter(dataset=ds).order_by('id')
        total_rows = queryset.count()

    page = request.GET.get('page', 1)
    paginator = Paginator(queryset, PAGE_SIZE)
    try:
        rows = paginator.page(page)
    except EmptyPage:
        rows = paginator.page(1)

    row_data = []
    for r in rows:
        if hasattr(r, 'data'):
            row_data.append(r.data)
        else:
            d = {f.name: getattr(r, f.name) for f in r._meta.fields if f.name != 'id'}
            row_data.append(d)

    if not field_names and row_data:
        field_names = list(row_data[0].keys())

    logs = CleaningLog.objects.filter(dataset=ds).order_by('-created_at')[:5]

    return render(request, 'dataflow/dataset_detail.html', {
        'ds': ds,
        'schema': schema,
        'field_names': field_names,
        'rows': rows,
        'row_data': row_data,
        'total_rows': total_rows,
        'logs': logs,
    })


# ── Re-clean (HTMX) ──
def dataset_clean(request, dataset_id):
    ds = get_object_or_404(Dataset, pk=dataset_id)
    rules_file = request.FILES.get('rules_file')

    rules_path = None
    if rules_file:
        with tempfile.NamedTemporaryFile(suffix='.json', delete=False) as tmp:
            for chunk in rules_file.chunks():
                tmp.write(chunk)
            rules_path = tmp.name

    try:
        if rules_path:
            clean_dataset(ds.name, rules_path)
        else:
            clean_dataset(ds.name)
        messages.success(request, f'Re-cleaned "{ds.name}" successfully.')
    finally:
        if rules_path:
            os.unlink(rules_path)

    return redirect('dataflow:dataset_detail', dataset_id=ds.id)


# ── Export ──
def dataset_export(request, dataset_id):
    ds = get_object_or_404(Dataset, pk=dataset_id)
    fmt = request.GET.get('format', 'csv')

    import pandas as pd
    import io

    # Reference dataset: export from source table
    is_ref = ds.description and ds.description.startswith('__ref:')
    ref_table = ds.description.split(':', 1)[1] if is_ref else None

    if is_ref and ref_table and ref_table in connection.introspection.table_names():
        from .db_explorer import get_table_schema
        columns = get_table_schema(ref_table)
        col_names = [c['name'] for c in columns]
        quoted_table = connection.ops.quote_name(ref_table)
        quoted_cols = [connection.ops.quote_name(c) for c in col_names]
        select_list = ", ".join(quoted_cols)

        with connection.cursor() as cursor:
            cursor.execute(f"SELECT {select_list} FROM {quoted_table}")
            rows = cursor.fetchall()

        data = [{col_names[i]: row[i] for i in range(len(col_names))} for row in rows]
        df = pd.DataFrame(data)

    else:
        model = SchemaManager.get_model_for_dataset(ds.id)
        if model:
            qs = model.objects.all().values()
            df = pd.DataFrame(list(qs))
            if 'id' in df.columns:
                df = df.drop(columns=['id'])
        else:
            records = DataRecord.objects.filter(dataset=ds).values_list('data', flat=True)
            df = pd.DataFrame(list(records))

    if fmt == 'csv':
        buf = io.StringIO()
        df.to_csv(buf, index=False)
        resp = HttpResponse(buf.getvalue(), content_type='text/csv')
        resp['Content-Disposition'] = f'attachment; filename="{ds.name}.csv"'
    elif fmt == 'json':
        resp = HttpResponse(
            df.to_json(orient='records', indent=2, force_ascii=False),
            content_type='application/json')
        resp['Content-Disposition'] = f'attachment; filename="{ds.name}.json"'
    elif fmt == 'sql':
        from datetime import date, datetime, time
        from decimal import Decimal
        from django.db import connection as db_conn

        if is_ref and ref_table:
            with db_conn.cursor() as c:
                raw_cols = db_conn.introspection.get_table_description(c, ref_table)
                pk_name = db_conn.introspection.get_primary_key_column(c, ref_table)

            type_map = {
                16: 'BOOLEAN', 20: 'BIGINT', 21: 'SMALLINT', 23: 'INTEGER',
                25: 'TEXT', 700: 'FLOAT', 701: 'FLOAT', 1042: 'VARCHAR',
                1043: 'VARCHAR', 1082: 'DATE', 1083: 'TIME', 1114: 'TIMESTAMP',
                1184: 'TIMESTAMPTZ', 1700: 'NUMERIC', 3802: 'JSONB',
            }
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
        else:
            # Uploaded dataset: generate basic CREATE TABLE from DataFrame
            sql_lines = [f'-- SQL Dump for dataset: {ds.name}\n']

        for _, row in df.iterrows():
            vals = []
            for col_name in df.columns:
                val = row[col_name]
                if pd.isna(val):
                    vals.append('NULL')
                elif isinstance(val, (bool,)):
                    vals.append('TRUE' if val else 'FALSE')
                elif isinstance(val, (int, float)):
                    vals.append(str(val))
                else:
                    escaped = str(val).replace("'", "''")
                    vals.append(f"'{escaped}'")
            sql_lines.append(f'INSERT INTO "{ds.name}" VALUES ({", ".join(vals)});\n')

        resp = HttpResponse('\n'.join(sql_lines), content_type='text/plain')
        resp['Content-Disposition'] = f'attachment; filename="{ds.name}.sql"'
    else:
        buf = io.BytesIO()
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        resp = HttpResponse(
            buf.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        resp['Content-Disposition'] = f'attachment; filename="{ds.name}.xlsx"'

    return resp


# ── Delete (HTMX) ──
def dataset_delete(request, dataset_id):
    ds = get_object_or_404(Dataset, pk=dataset_id)
    if request.method == 'POST':
        name = ds.name
        delete_dataset_fn(name)
        if request.headers.get('HX-Request') == 'true':
            resp = HttpResponse('')
            resp['HX-Redirect'] = '/dataflow/'
            return resp
        messages.success(request, f'Deleted "{name}".')
        return redirect('dataflow:home')
    return render(request, 'dataflow/delete_confirm.html', {'ds': ds})


# ── Database Explorer ──

def db_explorer(request):
    from .db_explorer import get_all_tables

    tables = get_all_tables()

    stats = {
        'total': len(tables),
        'application': sum(1 for t in tables if t['category'] == 'application'),
        'dataflow': sum(1 for t in tables if t['category'] == 'dataflow'),
        'system': sum(1 for t in tables if t['category'] == 'system'),
        'third_party': sum(1 for t in tables if t['category'] == 'third-party'),
        'total_rows': sum(t['row_count'] for t in tables),
    }

    return render(request, 'dataflow/db_explorer.html', {
        'tables': tables,
        'stats': stats,
    })


def db_explorer_table(request, table_name):
    from .db_explorer import get_table_data, get_table_schema

    if table_name not in connection.introspection.table_names():
        raise Http404(f"Table '{table_name}' not found")

    columns = get_table_schema(table_name)
    page_num = int(request.GET.get('page', 1))
    rows, total, col_names, pk_column = get_table_data(table_name, page=page_num)

    paginator = Paginator(range(total), PAGE_SIZE)
    try:
        page_obj = paginator.page(page_num)
    except EmptyPage:
        page_obj = paginator.page(1)

    return render(request, 'dataflow/db_explorer_table.html', {
        'table_name': table_name,
        'columns': columns,
        'row_data': rows,
        'col_names': col_names,
        'total': total,
        'page_obj': page_obj,
        'pk_column': pk_column,
    })


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
            df.to_json(orient='records', indent=2, force_ascii=False),
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
        with pd.ExcelWriter(buf, engine='openpyxl') as writer:
            df.to_excel(writer, index=False)
        resp = HttpResponse(
            buf.getvalue(),
            content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
        resp['Content-Disposition'] = f'attachment; filename="{table_name}.xlsx"'

    return resp


# ── DB Explorer: Import as Dataset ──

def db_explorer_import(request, table_name):
    from .db_explorer import get_table_data, get_table_schema
    from dataflow.models import Dataset

    if table_name not in connection.introspection.table_names():
        raise Http404

    columns = get_table_schema(table_name)
    _, total, _, _ = get_table_data(table_name, page=1, page_size=1)

    # Replace existing reference or dataset with same name
    Dataset.objects.filter(name=table_name).delete()

    Dataset.objects.create(
        name=table_name,
        description=f'__ref:{table_name}',
        raw_data=[],
    )

    messages.success(request,
        f'Linked "{table_name}" as dataset: {total} rows, {len(columns)} columns (live reference).')
    return redirect('dataflow:home')


# ── DB Explorer: Delete Table ──

def db_explorer_delete(request, table_name):
    if table_name not in connection.introspection.table_names():
        raise Http404

    if request.method == 'POST':
        # Also delete any corresponding Dataset
        from dataflow.models import Dataset
        Dataset.objects.filter(name=table_name).delete()

        quoted = connection.ops.quote_name(table_name)
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {quoted} CASCADE")

        messages.success(request, f'Table "{table_name}" dropped.')
        return redirect('dataflow:db_explorer')

    return render(request, 'dataflow/db_explorer_delete.html', {
        'table_name': table_name,
    })
