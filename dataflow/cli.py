#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import logging

logger = logging.getLogger('pipeline')

def _import_file_to_table(conn, file_path, table_name, replace):
    import pandas as pd
    from django.db import transaction
    ext = os.path.splitext(file_path)[1].lower()
    df = pd.read_csv(file_path) if ext == '.csv' else \
        pd.read_excel(file_path) if ext in ('.xls', '.xlsx') else \
        pd.read_json(file_path)
    df = df.replace({pd.NA: None, float('nan'): None})

    existing = conn.introspection.table_names()
    if table_name not in existing:
        from dataflow.views import _sql_type_for_series
        col_defs, used = [], set()
        for col in df.columns:
            if col in used: continue
            used.add(col)
            pk = ' PRIMARY KEY' if col == 'id' else ''
            col_defs.append(f'{conn.ops.quote_name(col)} {_sql_type_for_series(df[col])}{pk}')
        with conn.cursor() as cur:
            cur.execute(f'CREATE TABLE {conn.ops.quote_name(table_name)} ({", ".join(col_defs)})')
        _ok(f'Created table "{table_name}"')

    db_cols = {c.name for c in conn.introspection.get_table_description(conn.cursor(), table_name)}
    insert_cols = [c for c in df.columns if c in db_cols]
    if not insert_cols:
        _err(f'No matching columns for "{table_name}" — skipped.')
        return
    quoted_t = conn.ops.quote_name(table_name)
    quoted_c = [conn.ops.quote_name(c) for c in insert_cols]
    sql = f'INSERT INTO {quoted_t} ({", ".join(quoted_c)}) VALUES ({", ".join(["%s"]*len(insert_cols))})'
    values = [[row.get(c) for c in insert_cols] for row in df.to_dict(orient='records')]
    with transaction.atomic():
        with conn.cursor() as cur:
            if replace:
                cur.execute(f'DELETE FROM {quoted_t}')
            cur.executemany(sql, values)
    _ok(f'{len(values)} rows imported into "{table_name}".')


def _step(n, label):
    print(f'\n  \033[1;36mStep {n}\033[0m  {label}')
    print('  ' + '─' * 50)

def _ok(msg):
    print(f'  \033[1;32m✔\033[0m  {msg}')

def _err(msg):
    print(f'  \033[1;31m✘\033[0m  {msg}')

def _ask(prompt, default=None):
    hint = f' [{default}]' if default is not None else ''
    val = input(f'  › {prompt}{hint}: ').strip()
    return val or default or ''

def _choose(prompt, options):
    print(f'\n  {prompt}')
    for i, (label, _) in enumerate(options, 1):
        print(f'    {i}) {label}')
    while True:
        raw = input('  › Enter number: ').strip()
        if raw.isdigit() and 1 <= int(raw) <= len(options):
            return options[int(raw) - 1][1]
        print('    Invalid choice, try again.')


def _validate_file_exists(file_path: str):
    if not os.path.exists(file_path):
        logger.error("File not found: %s", file_path)
        sys.exit(1)


def run_wizard():
    """Interactive step-by-step wizard."""
    import pandas as pd

    print()
    print('  \033[1;37m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m')
    print('  \033[1;37m  Dataflow Manager — Interactive Wizard       \033[0m')
    print('  \033[1;37m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m')

    # ── Step 1: View databases ──
    _step(1, 'All Tables in Database')
    from django.db import connection as _conn
    _SYS = {'django_', 'auth_', 'dataflow_', 'taggit_'}
    all_table_names = _conn.introspection.table_names()
    app_tables  = sorted(t for t in all_table_names if not any(t.startswith(p) for p in _SYS))
    sys_tables  = sorted(t for t in all_table_names if any(t.startswith(p) for p in _SYS))
    all_tables  = app_tables  # used later for view/export choices

    def _print_table_list(tables):
        print(f'  {"Table":<35} {"Rows"}')
        print('  ' + '─' * 45)
        with _conn.cursor() as cur:
            for t in tables:
                cur.execute(f'SELECT COUNT(*) FROM {_conn.ops.quote_name(t)}')
                print(f'  {t:<35} {cur.fetchone()[0]}')

    if app_tables:
        print('\n  \033[1;32mApplication Tables\033[0m')
        _print_table_list(app_tables)
    if sys_tables:
        print('\n  \033[90mSystem Tables\033[0m')
        _print_table_list(sys_tables)
    if not app_tables and not sys_tables:
        print('  (no tables found)')
    datasets = list(Dataset.objects.all().order_by('-updated_at'))

    # ── Step 2: Choose action ──
    _step(2, 'What do you want to do?')
    action = _choose('Choose an action:', [
        ('View table data',             'view'),
        ('Export data to file',         'export'),
        ('Import file into table',      'run'),
        ('Clear table data',            'delete'),
        ('Quit',                        'quit'),
    ])

    if action == 'quit':
        print('\n  Bye!\n')
        return

    # ── Step 3+: Action-specific steps ──

    if action == 'view':
        _step(3, 'Select table to view')
        opts = [(t, t) for t in sorted(all_tables)]
        table = _choose('Which table?', opts)
        with _conn.cursor() as cur:
            cur.execute(f'SELECT COUNT(*) FROM {_conn.ops.quote_name(table)}')
            total = cur.fetchone()[0]
            cur.execute(f'SELECT * FROM {_conn.ops.quote_name(table)}')
            cols = [d[0] for d in cur.description]
            all_rows = cur.fetchall()

        PAGE = 10
        key_w = max(len(c) for c in cols)

        def _print_records(page_rows, offset):
            for i, row in enumerate(page_rows, offset + 1):
                print(f'  \033[1;36m── Record {i} ──\033[0m')
                for j, c in enumerate(cols):
                    val = str(row[j]) if row[j] is not None else ''
                    if len(val) > 80:
                        val = val[:77] + '...'
                    print(f'  {c:<{key_w}}  {val}')
                print()

        if total <= PAGE:
            print(f'\n  \033[1;33m=== {table} ({total} rows) ===\033[0m\n')
            if not all_rows:
                print('  (no data)')
            else:
                _print_records(all_rows, 0)
        else:
            page = 0
            while True:
                start = page * PAGE
                page_rows = all_rows[start:start + PAGE]
                print(f'\n  \033[1;33m=== {table} — Page {page+1}/{(total+PAGE-1)//PAGE} ({total} rows total) ===\033[0m\n')
                _print_records(page_rows, start)
                nav = []
                if page > 0:
                    nav.append(('Previous page', 'prev'))
                if start + PAGE < total:
                    nav.append(('Next page', 'next'))
                nav.append(('Back to main menu', 'back'))
                choice = _choose('Navigation:', nav)
                if choice == 'next':
                    page += 1
                elif choice == 'prev':
                    page -= 1
                else:
                    break
            run_wizard()
            return

        _choose('', [('Back to main menu', 'back')])
        run_wizard()
        return

    if action == 'export':
        _step(3, 'Select table(s) to export')
        if not app_tables:
            _err('No application tables found.')
            return
        opts = [('All application tables', '__all__')] + [(t, t) for t in app_tables]
        target = _choose('Which table?', opts)

        _step(4, 'Output format')
        dest = _choose('Save as:', [
            ('CSV',   'csv'),
            ('Excel', 'xlsx'),
            ('JSON',  'json'),
        ])

        names = app_tables if target == '__all__' else [target]
        import pandas as pd
        for name in names:
            with _conn.cursor() as cur:
                cur.execute(f'SELECT * FROM {_conn.ops.quote_name(name)}')
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
            df = pd.DataFrame(rows, columns=cols)
            filename = f'{name}.{dest}'
            output_dir = os.path.join(os.path.dirname(__file__), 'output')
            os.makedirs(output_dir, exist_ok=True)
            path = os.path.join(output_dir, filename)
            if dest == 'csv':
                df.to_csv(path, index=False)
            elif dest == 'xlsx':
                df.to_excel(path, index=False)
            elif dest == 'json':
                df.to_json(path, orient='records', indent=2,
                           force_ascii=False, date_format='iso')
            _ok(f'{len(df)} rows saved → {path}')

    elif action == 'run':
        _step(3, 'Select input file or folder')
        file_path = _ask('Path to file (.csv/.xlsx/.json) or folder')
        if not os.path.exists(file_path):
            _err(f'Not found: {file_path}')
            return

        import pandas as pd
        from django.db import transaction

        EXTS = {'.csv', '.xlsx', '.xls', '.json'}

        if os.path.isdir(file_path):
            files = [(f, os.path.join(file_path, f)) for f in os.listdir(file_path)
                if os.path.splitext(f)[1].lower() in EXTS]
            if not files:
                _err('No CSV/Excel/JSON files found in folder.')
                return
            replace = _choose('Import mode:', [
                ('Replace all existing data', True),
                ('Append to existing data',   False),
            ])
            _step(4, f'Importing {len(files)} file(s)…')
            for fname, fpath in files:
                table_name = os.path.splitext(fname)[0]
                _import_file_to_table(_conn, fpath, table_name, replace)
            return

        # single file
        default_name = os.path.splitext(os.path.basename(file_path))[0]
        _step(4, 'Target table')
        if app_tables:
            opts = [(t, t) for t in app_tables] + [('Create new table from file', '__new__')]
            target_table = _choose('Which table?', opts)
        else:
            target_table = '__new__'
        if target_table == '__new__':
            target_table = _ask('New table name', default_name)
        replace = _choose('Import mode:', [
            ('Replace all existing data', True),
            ('Append to existing data',   False),
        ])
        _step(5, 'Importing…')
        _import_file_to_table(_conn, file_path, target_table, replace)

    elif action == 'delete':
        _step(3, 'Select table to clear')
        if not app_tables:
            _err('No application tables available.')
            return
        opts = [('All application tables', '__all__')] + [(t, t) for t in app_tables]
        target = _choose('Which table?', opts)
        names = app_tables if target == '__all__' else [target]
        label = 'ALL' if target == '__all__' else target
        confirm = _ask(f'Type "{label}" to confirm clearing all data', '')
        if confirm != label:
            _err('Cancelled.')
            return
        with _conn.cursor() as cur:
            for name in names:
                cur.execute(f'DELETE FROM {_conn.ops.quote_name(name)}')
                _ok(f'All rows deleted from "{name}".')

    elif action == 'rules':
        _step(3, 'Select CSV file')
        file_path = _ask('Path to CSV file')
        if not os.path.exists(file_path):
            _err(f'File not found: {file_path}')
            return
        default_out = os.path.splitext(os.path.basename(file_path))[0] + '_rules.json'
        out = _ask('Output rules filename', default_out)
        sample = _ask('Sample size', '200')
        _step(4, 'Generating rules…')
        generate_rules_file(file_path, out, int(sample))
        _ok('Done.')

    next_action = _choose('What next?', [
        ('Return to main menu', 'menu'),
        ('Quit',                'quit'),
    ])
    if next_action == 'menu':
        run_wizard()


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
