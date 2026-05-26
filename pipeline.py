#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL Pipeline — clean, format, store, and export data via Django.

Usage:
    python pipeline.py run data.csv --name "dataset"              # auto-infer rules
    python pipeline.py run data.csv --name "dataset" -r rules.json  # with rules
    python pipeline.py export "dataset" -o cleaned.csv              # export back
    python pipeline.py report "dataset"                             # summary
    python pipeline.py list                                         # list all datasets
"""
import json
import os
import sys
from typing import Dict, List, Any, Optional

import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from core.models import Dataset, DataRecord, CleaningLog
from core.data_processor import (
    load_any_file,
    GenericDataCleaner,
    _load_rules,
)
from auto_rules import generate_rules


# ------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------
class Pipeline:
    """Complete ETL pipeline: load → clean → format → store → export."""

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.dataset_obj: Optional[Dataset] = None
        self.raw_rows: List[Dict] = []
        self.valid_rows: List[Dict] = []
        self.errors: List[Dict] = []
        self.rules: Dict[str, Any] = {}

    # ---- Load ----
    def load_file(self, file_path: str) -> 'Pipeline':
        print(f"[LOAD] {file_path}")
        self.raw_rows = load_any_file(file_path)
        print(f"       {len(self.raw_rows)} rows read")
        return self

    def load_records(self, records: List[Dict]) -> 'Pipeline':
        print(f"[LOAD] {len(records)} records from memory")
        self.raw_rows = records
        return self

    # ---- Rules ----
    def auto_rules(self, sample: int = 200) -> 'Pipeline':
        print(f"[RULES] Auto-detecting from sample of {min(sample, len(self.raw_rows))}")
        values_by_col: Dict[str, List[str]] = {}
        for row in self.raw_rows[:sample]:
            for col, val in row.items():
                values_by_col.setdefault(col, []).append(str(val) if val is not None else '')

        from auto_rules import infer_type
        for col, values in values_by_col.items():
            t = infer_type(values)
            rule: Dict[str, Any] = {'type': t, 'required': False}
            if t == 'int':
                rule['default'] = 0
            elif t == 'float':
                rule['default'] = 0.0
            elif t == 'boolean':
                rule['default'] = False
            self.rules[col] = rule

        for col, rule in self.rules.items():
            print(f"       {col:30s} -> {rule['type']}")
        return self

    def load_rules_file(self, rules_path: str) -> 'Pipeline':
        rules = _load_rules(rules_path)
        if rules:
            self.rules = rules
            print(f"[RULES] Loaded from {rules_path}: {list(rules.keys())}")
        else:
            print(f"[RULES] File not found: {rules_path}")
        return self

    # ---- Clean & Format ----
    def clean(self) -> 'Pipeline':
        print(f"[CLEAN] Processing {len(self.raw_rows)} rows")
        cleaner = GenericDataCleaner(self.rules) if self.rules else GenericDataCleaner()
        self.valid_rows, self.errors = cleaner.clean_dataset(self.raw_rows)
        invalid = len(self.raw_rows) - len(self.valid_rows)
        print(f"        {len(self.valid_rows)} valid, {invalid} invalid")
        if invalid:
            for e in self.errors:
                if e['errors']:
                    print(f"        Row {e['row_num']}: {'; '.join(e['errors'])}")
        return self

    # ---- Store ----
    def store(self, replace: bool = False) -> 'Pipeline':
        print(f"[STORE] Dataset: {self.dataset_name}")

        if replace:
            from core.schema_manager import SchemaManager
            from core.models import DatasetSchema
            old_schema = DatasetSchema.objects.filter(dataset__name=self.dataset_name).first()
            if old_schema and SchemaManager.table_exists(old_schema.table_name):
                SchemaManager.drop_table(old_schema.table_name)

            deleted, _ = Dataset.objects.filter(name=self.dataset_name).delete()
            if deleted:
                print(f"        Replaced existing ({deleted} record(s) removed)")

        self.dataset_obj = Dataset.objects.create(name=self.dataset_name)

        # Save raw rows for future re-cleaning
        self.dataset_obj.raw_data = self.raw_rows
        self.dataset_obj.save(update_fields=['raw_data'])

        # Generate auto rules from column keys if not already set
        if not self.rules and self.valid_rows:
            self.rules = {col: {'type': 'string', 'required': False, 'default': ''}
                          for col in self.valid_rows[0].keys()}

        # Dual-write: JSON backup
        for rec in self.valid_rows:
            DataRecord.objects.create(dataset=self.dataset_obj, data=rec)
        print(f"        {len(self.valid_rows)} records saved (JSON)")

        # Dual-write: typed dynamic table
        if self.rules and self.valid_rows:
            from core.schema_manager import SchemaManager, prepare_for_dynamic_table
            schema_obj = SchemaManager.create_schema_for_dataset(self.dataset_obj, self.rules)
            model = SchemaManager.get_model_for_dataset(self.dataset_obj.id)
            if model:
                for rec in self.valid_rows:
                    db_rec = prepare_for_dynamic_table(rec, schema_obj.fields_json)
                    model.objects.create(**db_rec)
                print(f"        {len(self.valid_rows)} records in dynamic table '{schema_obj.table_name}'")

        log_result = {
            'total_rows': len(self.raw_rows),
            'valid_rows': len(self.valid_rows),
            'invalid_rows': len(self.raw_rows) - len(self.valid_rows),
            'errors_per_row': self.errors,
        }
        CleaningLog.objects.create(
            dataset=self.dataset_obj,
            status='completed' if self.valid_rows else 'failed',
            result=log_result,
        )
        print(f"        CleaningLog created")
        return self

    # ---- Export ----
    def export(self, filename: str) -> 'Pipeline':
        if not self.valid_rows:
            print("[EXPORT] No valid rows to export. Run clean() first.")
            return self

        output_dir = os.path.join(os.path.dirname(__file__), 'output')
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, filename)

        import pandas as pd
        df = pd.DataFrame(self.valid_rows)
        if output_path.endswith('.csv'):
            df.to_csv(output_path, index=False)
        elif output_path.endswith(('.xls', '.xlsx')):
            df.to_excel(output_path, index=False)
        elif output_path.endswith('.json'):
            df.to_json(output_path, orient='records', indent=2)
        elif output_path.endswith('.parquet'):
            df.to_parquet(output_path, index=False)
        else:
            df.to_csv(output_path, index=False)

        print(f"[EXPORT] {len(self.valid_rows)} rows -> {output_path}")
        return self

    # ---- Report ----
    def report(self) -> 'Pipeline':
        print(f"\n{'='*60}")
        print(f"  Dataset: {self.dataset_name}")
        print(f"{'='*60}")
        print(f"  Total rows:    {len(self.raw_rows)}")
        print(f"  Valid:         {len(self.valid_rows)}")
        print(f"  Invalid:       {len(self.raw_rows) - len(self.valid_rows)}")
        print(f"  Rules used:    {len(self.rules)} columns")
        if self.dataset_obj:
            print(f"  Stored as:     Dataset id={self.dataset_obj.id}")
            print(f"  View at:       /admin/core/dataset/{self.dataset_obj.id}/")
        print(f"{'='*60}\n")
        return self


def export_dataset(dataset_name: str, filename: str):
    """Export a previously stored dataset from DB to output/ folder."""
    import pandas as pd

    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        print(f"Dataset '{dataset_name}' not found.")
        return

    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    # Prefer dynamic table; fall back to JSON DataRecord
    from core.schema_manager import SchemaManager
    model = SchemaManager.get_model_for_dataset(ds.id)
    if model:
        qs = model.objects.all().values()
        df = pd.DataFrame(list(qs))
        if 'id' in df.columns:
            df = df.drop(columns=['id'])
    else:
        records = DataRecord.objects.filter(dataset=ds).values_list('data', flat=True)
        df = pd.DataFrame(list(records))

    if output_path.endswith('.csv'):
        df.to_csv(output_path, index=False)
    elif output_path.endswith(('.xls', '.xlsx')):
        df.to_excel(output_path, index=False)
    elif output_path.endswith('.json'):
        df.to_json(output_path, orient='records', indent=2)
    else:
        df.to_csv(output_path, index=False)

    print(f"[EXPORT] {len(df)} rows from '{dataset_name}' -> {output_path}")


def report_dataset(dataset_name: str):
    """Print a summary report for a stored dataset."""
    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        print(f"Dataset '{dataset_name}' not found.")
        return

    records = DataRecord.objects.filter(dataset=ds)
    logs = CleaningLog.objects.filter(dataset=ds)

    print(f"\n{'='*60}")
    print(f"  Dataset: {ds.name}")
    print(f"  ID: {ds.id} | Created: {ds.updated_at.strftime('%Y-%m-%d %H:%M')}")
    print(f"{'='*60}")
    print(f"  Description:  {ds.description or '-'}")
    print(f"  Records:      {records.count()}")
    print(f"  CleaningLogs: {logs.count()}")
    print(f"  Admin:        /admin/core/dataset/{ds.id}/")

    if records.exists():
        first = records.first()
        print(f"  Columns ({len(first.data)}): {', '.join(first.data.keys())}")
        print(f"  Sample:")
        for rec in records[:3]:
            print(f"    {json.dumps(rec.data, ensure_ascii=False)[:120]}")

    for log in logs:
        print(f"\n  CleaningLog #{log.id} [{log.status}] {log.created_at.strftime('%Y-%m-%d %H:%M')}")
        result = log.result
        print(f"    Total: {result.get('total_rows', '?')} rows")
        print(f"    Valid: {result.get('valid_rows', '?')} | Invalid: {result.get('invalid_rows', '?')}")

    print(f"{'='*60}\n")


def clean_dataset(dataset_name: str, rules_path: str):
    """Re-clean a stored dataset with new rules from the DB."""
    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        print(f"Dataset '{dataset_name}' not found.")
        return

    if not ds.raw_data:
        print(f"Dataset '{dataset_name}' has no raw data to re-clean.")
        return

    pipeline = Pipeline(dataset_name)
    pipeline.raw_rows = ds.raw_data
    pipeline.load_rules_file(rules_path)
    if not pipeline.rules:
        print(f"No rules loaded from '{rules_path}'.")
        return
    pipeline.clean()
    pipeline.store(replace=True)
    pipeline.report()


def delete_dataset(dataset_name: str):
    """Delete a stored dataset, its dynamic table, and all related records."""
    from core.schema_manager import SchemaManager
    from core.models import DatasetSchema

    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        print(f"Dataset '{dataset_name}' not found.")
        return

    schema = DatasetSchema.objects.filter(dataset=ds).first()
    if schema and SchemaManager.table_exists(schema.table_name):
        SchemaManager.drop_table(schema.table_name)
        print(f"        Dropped dynamic table '{schema.table_name}'")

    deleted, details = ds.delete()
    count = details.get('core.Dataset', 0)
    print(f"Deleted dataset '{dataset_name}' ({count} record(s) removed)")


def list_datasets():
    """List all datasets in the database."""
    datasets = Dataset.objects.all().order_by('-updated_at')
    if not datasets:
        print("No datasets found.")
        return

    print(f"\n{'ID':<6} {'Name':<30} {'Records':<10} {'Updated'}")
    print(f"{'-'*6} {'-'*30} {'-'*10} {'-'*20}")
    for ds in datasets:
        count = DataRecord.objects.filter(dataset=ds).count()
        print(f"{ds.id:<6} {ds.name:<30} {count:<10} {ds.updated_at.strftime('%Y-%m-%d %H:%M')}")
    print()


def generate_rules_file(csv_path: str, output_name: str = None, sample: int = 200):
    """Generate a cleaning rules JSON file from a CSV."""
    if output_name is None:
        base = os.path.splitext(os.path.basename(csv_path))[0]
        output_name = f"{base}_rules.json"

    rules_dir = os.path.join(os.path.dirname(__file__), 'rules')
    os.makedirs(rules_dir, exist_ok=True)
    output_path = os.path.join(rules_dir, output_name)

    from auto_rules import generate_rules
    rules = generate_rules(csv_path, output_path, sample_rows=sample)

    print(f"Detected {len(rules)} columns:")
    for col, rule in rules.items():
        print(f"  {col:30s} -> {rule['type']}")
    print(f"\nRules saved to: {output_path}")


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(description='ETL Pipeline — clean, format, store, export')
    sub = parser.add_subparsers(dest='command', required=True)

    # run
    run_parser = sub.add_parser('run', help='Load, clean, and store a file')
    run_parser.add_argument('file', help='Path to data file')
    run_parser.add_argument('--name', '-n', default=None, help='Dataset name (auto-derived from filename if omitted)')
    run_parser.add_argument('--rules', '-r', default=None, help='Rules JSON file (auto-detect if omitted)')
    run_parser.add_argument('--replace', action='store_true', help='Replace existing dataset with same name')
    run_parser.add_argument('--export', '-o', default=None, help='Also export cleaned data to file')

    # export
    export_parser = sub.add_parser('export', help='Export a stored dataset to file')
    export_parser.add_argument('name', help='Dataset name')
    export_parser.add_argument('--output', '-o', required=True, help='Output file path')

    # report
    report_parser = sub.add_parser('report', help='Show dataset summary')
    report_parser.add_argument('name', help='Dataset name')

    # list
    sub.add_parser('list', help='List all datasets')

    # clean
    clean_parser = sub.add_parser('clean', help='Re-clean a stored dataset with new rules')
    clean_parser.add_argument('name', help='Dataset name')
    clean_parser.add_argument('--rules', '-r', required=True, help='Rules JSON file')

    # delete
    delete_parser = sub.add_parser('delete', help='Delete a stored dataset and its dynamic table')
    delete_parser.add_argument('name', help='Dataset name')

    # rules
    rules_parser = sub.add_parser('rules', help='Generate cleaning rules JSON from a CSV')
    rules_parser.add_argument('file', help='Path to CSV file')
    rules_parser.add_argument('--output', '-o', default=None, help='Output rules file name')
    rules_parser.add_argument('--sample', '-s', type=int, default=200, help='Sample size for type detection')

    args = parser.parse_args()

    if args.command == 'run':
        name = args.name or os.path.splitext(os.path.basename(args.file))[0]
        pipeline = Pipeline(name)
        pipeline.load_file(args.file)

        if args.rules:
            pipeline.load_rules_file(args.rules)
        else:
            pipeline.auto_rules()

        pipeline.clean()
        pipeline.store(replace=args.replace)
        pipeline.report()

        if args.export:
            pipeline.export(args.export)

    elif args.command == 'export':
        export_dataset(args.name, args.output)

    elif args.command == 'report':
        report_dataset(args.name)

    elif args.command == 'list':
        list_datasets()

    elif args.command == 'clean':
        clean_dataset(args.name, args.rules)

    elif args.command == 'delete':
        delete_dataset(args.name)

    elif args.command == 'rules':
        generate_rules_file(args.file, args.output, args.sample)
