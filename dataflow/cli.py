#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
ETL Pipeline — clean, format, store, and export data via Django.

Usage:
    python pipeline.py run data.csv --name "dataset"              # auto-infer rules
    python pipeline.py run data.csv --name "dataset" -r rules.json  # with rules
    python pipeline.py run data.csv --dry-run                       # preview only
    python pipeline.py run data.csv --verbose                       # per-row progress
    python pipeline.py run data.csv --quiet                         # errors only
    python pipeline.py export "dataset" -o cleaned.csv              # export back
    python pipeline.py report "dataset"                             # summary
    python pipeline.py list                                         # list all datasets
"""
import json
import logging
import os
import sys
from typing import Dict, List, Any, Optional

import django

# When running from the source directory, ensure demo/ is importable
_src_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if os.path.exists(os.path.join(_src_dir, 'demo', 'settings.py')):
    sys.path.insert(0, _src_dir)

if not os.environ.get('DJANGO_SETTINGS_MODULE'):
    # Auto-detect demo project when running from source
    if os.path.exists(os.path.join(_src_dir, 'demo', 'settings.py')):
        os.environ['DJANGO_SETTINGS_MODULE'] = 'demo.settings'
    else:
        sys.exit(
            'DJANGO_SETTINGS_MODULE is not set.\n'
            '  export DJANGO_SETTINGS_MODULE=myproject.settings\n'
        )

django.setup()

from dataflow.models import Dataset, DataRecord, CleaningLog
from dataflow.data_processor import (
    load_any_file,
    GenericDataCleaner,
    _load_rules,
)
from dataflow.auto_rules import generate_rules

logger = logging.getLogger('pipeline')


# ------------------------------------------------------------
# Pipeline
# ------------------------------------------------------------
class Pipeline:
    """Complete ETL pipeline: load -> clean -> format -> store -> export."""

    def __init__(self, dataset_name: str):
        self.dataset_name = dataset_name
        self.dataset_obj: Optional[Dataset] = None
        self.raw_rows: List[Dict] = []
        self.valid_rows: List[Dict] = []
        self.errors: List[Dict] = []
        self.rules: Dict[str, Any] = {}

    # ---- Load ----
    def load_file(self, file_path: str) -> 'Pipeline':
        logger.info("[LOAD] %s", file_path)
        self.raw_rows = load_any_file(file_path)
        logger.info("       %d rows read", len(self.raw_rows))
        return self

    def load_records(self, records: List[Dict]) -> 'Pipeline':
        logger.info("[LOAD] %d records from memory", len(records))
        self.raw_rows = records
        return self

    # ---- Rules ----
    def auto_rules(self, sample: int = 200) -> 'Pipeline':
        n = min(sample, len(self.raw_rows))
        logger.info("[RULES] Auto-detecting from sample of %d", n)
        values_by_col: Dict[str, List[str]] = {}
        for row in self.raw_rows[:sample]:
            for col, val in row.items():
                values_by_col.setdefault(col, []).append(str(val) if val is not None else '')

        from dataflow.auto_rules import infer_type
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
            logger.info("       %-30s -> %s", col, rule['type'])
        return self

    def load_rules_file(self, rules_path: str) -> 'Pipeline':
        rules = _load_rules(rules_path)
        if rules:
            self.rules = rules
            logger.info("[RULES] Loaded from %s: %s", rules_path, list(rules.keys()))
        else:
            logger.error("[RULES] File not found: %s", rules_path)
        return self

    # ---- Clean & Format ----
    def clean(self) -> 'Pipeline':
        logger.info("[CLEAN] Processing %d rows", len(self.raw_rows))
        cleaner = GenericDataCleaner(self.rules) if self.rules else GenericDataCleaner()
        self.valid_rows, self.errors = cleaner.clean_dataset(self.raw_rows)
        invalid = len(self.raw_rows) - len(self.valid_rows)
        logger.info("        %d valid, %d invalid", len(self.valid_rows), invalid)
        if invalid:
            for e in self.errors:
                if e['errors']:
                    logger.warning("        Row %d: %s", e['row_num'], '; '.join(e['errors']))
        return self

    # ---- Store ----
    def store(self, replace: bool = False) -> 'Pipeline':
        logger.info("[STORE] Dataset: %s", self.dataset_name)

        if replace:
            from dataflow.schema_manager import SchemaManager
            from dataflow.models import DatasetSchema
            old_schema = DatasetSchema.objects.filter(dataset__name=self.dataset_name).first()
            if old_schema and SchemaManager.table_exists(old_schema.table_name):
                SchemaManager.drop_table(old_schema.table_name)

            deleted, _ = Dataset.objects.filter(name=self.dataset_name).delete()
            if deleted:
                logger.info("        Replaced existing (%d record(s) removed)", deleted)

        self.dataset_obj = Dataset.objects.create(name=self.dataset_name)
        self.dataset_obj.raw_data = self.raw_rows
        self.dataset_obj.save(update_fields=['raw_data'])

        if not self.rules and self.valid_rows:
            self.rules = {col: {'type': 'string', 'required': False, 'default': ''}
                          for col in self.valid_rows[0].keys()}

        # Dual-write: JSON backup
        for rec in self.valid_rows:
            DataRecord.objects.create(dataset=self.dataset_obj, data=rec)
        logger.info("        %d records saved (JSON)", len(self.valid_rows))

        # Dual-write: typed dynamic table
        if self.rules and self.valid_rows:
            from dataflow.schema_manager import SchemaManager, prepare_for_dynamic_table
            schema_obj = SchemaManager.create_schema_for_dataset(self.dataset_obj, self.rules)
            model = SchemaManager.get_model_for_dataset(self.dataset_obj.id)
            if model:
                iterable = self.valid_rows
                if logger.isEnabledFor(logging.INFO):
                    try:
                        from tqdm import tqdm
                        iterable = tqdm(self.valid_rows, desc="       Dynamic table", unit=" rows")
                    except ImportError:
                        pass
                for rec in iterable:
                    db_rec = prepare_for_dynamic_table(rec, schema_obj.fields_json)
                    model.objects.create(**db_rec)
                logger.info("        %d records in dynamic table '%s'", len(self.valid_rows), schema_obj.table_name)

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
        logger.info("        CleaningLog created")
        return self

    # ---- Export ----
    def export(self, filename: str) -> 'Pipeline':
        if not self.valid_rows:
            logger.warning("[EXPORT] No valid rows to export. Run clean() first.")
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

        logger.info("[EXPORT] %d rows -> %s", len(self.valid_rows), output_path)
        return self

    # ---- Report ----
    def report(self) -> 'Pipeline':
        lines = [
            "",
            "=" * 60,
            f"  Dataset: {self.dataset_name}",
            "=" * 60,
            f"  Total rows:    {len(self.raw_rows)}",
            f"  Valid:         {len(self.valid_rows)}",
            f"  Invalid:       {len(self.raw_rows) - len(self.valid_rows)}",
            f"  Rules used:    {len(self.rules)} columns",
        ]
        if self.dataset_obj:
            lines.append(f"  Stored as:     Dataset id={self.dataset_obj.id}")
            lines.append(f"  View at:       /admin/dataflow/dataset/{self.dataset_obj.id}/")
        lines.append("=" * 60)
        logger.info("\n".join(lines))
        return self


def export_dataset(dataset_name: str, filename: str):
    """Export a previously stored dataset from DB to output/ folder."""
    import pandas as pd

    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        logger.error("Dataset '%s' not found.", dataset_name)
        return

    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, filename)

    from dataflow.schema_manager import SchemaManager
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

    logger.info("[EXPORT] %d rows from '%s' -> %s", len(df), dataset_name, output_path)


def report_dataset(dataset_name: str):
    """Print a summary report for a stored dataset."""
    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        logger.error("Dataset '%s' not found.", dataset_name)
        return

    records = DataRecord.objects.filter(dataset=ds)
    logs = CleaningLog.objects.filter(dataset=ds)

    lines = [
        "",
        "=" * 60,
        f"  Dataset: {ds.name}",
        f"  ID: {ds.id} | Created: {ds.updated_at.strftime('%Y-%m-%d %H:%M')}",
        "=" * 60,
        f"  Description:  {ds.description or '-'}",
        f"  Records:      {records.count()}",
        f"  CleaningLogs: {logs.count()}",
        f"  Admin:        /admin/dataflow/dataset/{ds.id}/",
    ]
    logger.info("\n".join(lines))

    if records.exists():
        first = records.first()
        logger.info("  Columns (%d): %s", len(first.data), ', '.join(first.data.keys()))
        logger.info("  Sample:")
        for rec in records[:3]:
            logger.info("    %s", json.dumps(rec.data, ensure_ascii=False)[:120])

    for log in logs:
        result = log.result
        logger.info("  CleaningLog #%d [%s] %s", log.id, log.status,
                    log.created_at.strftime('%Y-%m-%d %H:%M'))
        logger.info("    Total: %s | Valid: %s | Invalid: %s",
                    result.get('total_rows', '?'),
                    result.get('valid_rows', '?'),
                    result.get('invalid_rows', '?'))

    logger.info("=" * 60)


def clean_dataset(dataset_name: str, rules_path: str = None):
    """Re-clean a stored dataset with new rules from the DB."""
    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        logger.error("Dataset '%s' not found.", dataset_name)
        return

    if not ds.raw_data:
        logger.error("Dataset '%s' has no raw data to re-clean.", dataset_name)
        return

    pipeline = Pipeline(dataset_name)
    pipeline.raw_rows = ds.raw_data
    if rules_path:
        pipeline.load_rules_file(rules_path)
        if not pipeline.rules:
            logger.error("No rules loaded from '%s'.", rules_path)
            return
    else:
        pipeline.auto_rules()
    pipeline.clean()
    pipeline.store(replace=True)
    pipeline.report()


def delete_dataset(dataset_name: str):
    """Delete a stored dataset, its dynamic table, and all related records."""
    from dataflow.schema_manager import SchemaManager
    from dataflow.models import DatasetSchema

    try:
        ds = Dataset.objects.get(name=dataset_name)
    except Dataset.DoesNotExist:
        logger.error("Dataset '%s' not found.", dataset_name)
        return

    schema = DatasetSchema.objects.filter(dataset=ds).first()
    if schema and SchemaManager.table_exists(schema.table_name):
        SchemaManager.drop_table(schema.table_name)
        logger.info("        Dropped dynamic table '%s'", schema.table_name)

    deleted, details = ds.delete()
    count = details.get('dataflow.Dataset', 0)
    logger.info("Deleted dataset '%s' (%d record(s) removed)", dataset_name, count)


def list_datasets():
    """List all datasets in the database."""
    datasets = Dataset.objects.all().order_by('-updated_at')
    if not datasets:
        logger.info("No datasets found.")
        return

    logger.info("")
    logger.info("%-6s %-30s %-10s %s", 'ID', 'Name', 'Records', 'Updated')
    logger.info("%s %s %s %s", '-' * 6, '-' * 30, '-' * 10, '-' * 20)
    for ds in datasets:
        count = DataRecord.objects.filter(dataset=ds).count()
        logger.info("%-6d %-30s %-10d %s", ds.id, ds.name, count,
                    ds.updated_at.strftime('%Y-%m-%d %H:%M'))
    logger.info("")


def generate_rules_file(csv_path: str, output_name: str = None, sample: int = 200):
    """Generate a cleaning rules JSON file from a CSV."""
    if output_name is None:
        base = os.path.splitext(os.path.basename(csv_path))[0]
        output_name = f"{base}_rules.json"

    rules_dir = os.path.join(os.path.dirname(__file__), 'rules')
    os.makedirs(rules_dir, exist_ok=True)
    output_path = os.path.join(rules_dir, output_name)

    from dataflow.auto_rules import generate_rules
    rules = generate_rules(csv_path, output_path, sample_rows=sample)

    logger.info("Detected %d columns:", len(rules))
    for col, rule in rules.items():
        logger.info("  %-30s -> %s", col, rule['type'])
    logger.info("")
    logger.info("Rules saved to: %s", output_path)


def _validate_file_exists(file_path: str):
    """Check that the input file exists before starting the pipeline."""
    if not os.path.exists(file_path):
        logger.error("File not found: %s", file_path)
        sys.exit(1)


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------
def main():
    import argparse

    parser = argparse.ArgumentParser(description='ETL Pipeline - clean, format, store, export')
    sub = parser.add_subparsers(dest='command', required=True)

    # Shared flags for run
    def _add_output_flags(p):
        p.add_argument('--verbose', '-v', action='store_true', help='Show per-row progress')
        p.add_argument('--quiet', '-q', action='store_true', help='Show errors only')

    # run
    run_parser = sub.add_parser('run', help='Load, clean, and store a file')
    run_parser.add_argument('file', help='Path to data file')
    run_parser.add_argument('--name', '-n', default=None, help='Dataset name (auto-derived from filename if omitted)')
    run_parser.add_argument('--rules', '-r', default=None, help='Rules JSON file (auto-detect if omitted)')
    run_parser.add_argument('--replace', action='store_true', help='Replace existing dataset with same name')
    run_parser.add_argument('--export', '-o', default=None, help='Also export cleaned data to file')
    run_parser.add_argument('--dry-run', action='store_true', help='Preview only - skip database writes')
    _add_output_flags(run_parser)

    # export
    export_parser = sub.add_parser('export', help='Export a stored dataset to file')
    export_parser.add_argument('name', help='Dataset name')
    export_parser.add_argument('--output', '-o', required=True, help='Output file path')
    _add_output_flags(export_parser)

    # report
    report_parser = sub.add_parser('report', help='Show dataset summary')
    report_parser.add_argument('name', help='Dataset name')
    _add_output_flags(report_parser)

    # list
    list_parser = sub.add_parser('list', help='List all datasets')
    _add_output_flags(list_parser)

    # clean
    clean_parser = sub.add_parser('clean', help='Re-clean a stored dataset with new rules')
    clean_parser.add_argument('name', help='Dataset name')
    clean_parser.add_argument('--rules', '-r', default=None, help='Rules JSON file (auto-detect if omitted)')
    _add_output_flags(clean_parser)

    # delete
    delete_parser = sub.add_parser('delete', help='Delete a stored dataset and its dynamic table')
    delete_parser.add_argument('name', help='Dataset name')
    _add_output_flags(delete_parser)

    # rules
    rules_parser = sub.add_parser('rules', help='Generate cleaning rules JSON from a CSV')
    rules_parser.add_argument('file', help='Path to CSV file')
    rules_parser.add_argument('--output', '-o', default=None, help='Output rules file name')
    rules_parser.add_argument('--sample', '-s', type=int, default=200, help='Sample size for type detection')
    _add_output_flags(rules_parser)

    args = parser.parse_args()

    # Configure logging based on verbosity flags
    level = logging.INFO
    fmt = '%(message)s'
    if hasattr(args, 'verbose') and args.verbose:
        level = logging.DEBUG
        fmt = '%(levelname)-8s %(message)s'
    elif hasattr(args, 'quiet') and args.quiet:
        level = logging.WARNING

    logging.basicConfig(level=level, format=fmt, stream=sys.stdout)

    if args.command == 'run':
        _validate_file_exists(args.file)

        name = args.name or os.path.splitext(os.path.basename(args.file))[0]
        pipeline = Pipeline(name)
        pipeline.load_file(args.file)

        if args.rules:
            pipeline.load_rules_file(args.rules)
        else:
            pipeline.auto_rules()

        pipeline.clean()

        if args.dry_run:
            logger.info("[DRY-RUN] Skipping database write. Preview:")
            pipeline.report()
        else:
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
        _validate_file_exists(args.file)
        generate_rules_file(args.file, args.output, args.sample)


if __name__ == '__main__':
    main()
