#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import django
import json
import re
from datetime import datetime
from decimal import Decimal
from typing import Dict, List, Any, Optional

import pandas as pd

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'config.settings')
django.setup()

from core.models import Dataset, DataRecord, CleaningLog

# ------------------------------------------------------------
# Built-in cleaning functions
# ------------------------------------------------------------
def clean_string(value: Any) -> str:
    if value is None or value == '':
        return ''
    return str(value).strip()

def clean_upper(value: Any) -> str:
    return clean_string(value).upper()

def clean_lower(value: Any) -> str:
    return clean_string(value).lower()

def clean_email(value: Any) -> str:
    v = clean_lower(value)
    if not re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', v):
        raise ValueError(f"Invalid email: {v}")
    return v

def clean_phone(value: Any) -> str:
    digits = re.sub(r'\D', '', clean_string(value))
    return digits[:15] if digits else ''

def clean_int(value: Any, default: int = 0, min_val: Optional[int] = None, max_val: Optional[int] = None) -> int:
    try:
        v = int(float(str(value))) if value else default
    except (ValueError, TypeError):
        v = default
    if min_val is not None:
        v = max(min_val, v)
    if max_val is not None:
        v = min(max_val, v)
    return v

def clean_float(value: Any, default: float = 0.0, min_val: Optional[float] = None, max_val: Optional[float] = None) -> float:
    try:
        v = float(value) if value else default
    except (ValueError, TypeError):
        v = default
    if min_val is not None:
        v = max(min_val, v)
    if max_val is not None:
        v = min(max_val, v)
    return v

def clean_decimal(value: Any, default: Decimal = Decimal('0'), min: Optional[Decimal] = None) -> Decimal:
    try:
        v = Decimal(str(value)) if value else default
    except:
        v = default
    if min_val is not None and v < min_val:
        v = min_val
    return v

def clean_date(value: Any, formats: List[str] = None) -> Optional[str]:
    if not value:
        return None
    if formats is None:
        formats = ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%m/%d/%Y']
    s = clean_string(value)
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.date().isoformat()
        except ValueError:
            continue
    raise ValueError(f"Date '{s}' not match formats {formats}")

def clean_datetime(value: Any, formats: List[str] = None) -> Optional[str]:
    if not value:
        return None
    if formats is None:
        formats = ['%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M', '%Y/%m/%d %H:%M:%S']
    s = clean_string(value)
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            return dt.isoformat()
        except ValueError:
            continue
    raise ValueError(f"Datetime '{s}' not parsable")

def clean_boolean(value: Any) -> bool:
    v = clean_string(value).lower()
    if v in ('true', 'yes', '1', 't', 'y'):
        return True
    if v in ('false', 'no', '0', 'f', 'n'):
        return False
    return bool(v)

BUILTIN_RULES = {
    'string': clean_string,
    'upper': clean_upper,
    'lower': clean_lower,
    'email': clean_email,
    'phone': clean_phone,
    'int': clean_int,
    'float': clean_float,
    'decimal': clean_decimal,
    'date': clean_date,
    'datetime': clean_datetime,
    'boolean': clean_boolean,
}

# ------------------------------------------------------------
# Generic Data Cleaner
# ------------------------------------------------------------
class GenericDataCleaner:
    """
    Apply cleaning rules defined in a configuration dict.

    Rules format:
    {
        'field_name': {
            'type': 'email',
            'required': True/False,
            'default': some_value,
            'min': ..., 'max': ...     # for int/float
        },
        'another_field': 'string',     # shorthand
    }

    If rules is None or empty, all fields are treated as string (non-required).
    """

    def __init__(self, rules: Dict[str, Any] = None):
        self.rules = rules or {}

    def _clean_value(self, col: str, value: Any) -> tuple:
        """Returns (cleaned_value, error_message_or_None)"""
        rule = self.rules.get(col)
        if rule is None:
            return clean_string(value), None

        if isinstance(rule, str):
            rule = {'type': rule}

        rule_type = rule.get('type', 'string')
        if rule_type not in BUILTIN_RULES:
            return None, f"Unknown rule type: {rule_type}"

        func = BUILTIN_RULES[rule_type]
        kwargs = {}
        for k, v in rule.items():
            if k in ('type', 'required'):
                continue
            if k == 'min':
                kwargs['min_val'] = v
            elif k == 'max':
                kwargs['max_val'] = v
            else:
                kwargs[k] = v

        try:
            if value in (None, ''):
                if rule.get('required', False):
                    return None, "Required field is empty"
                default = rule.get('default', None)
                return default, None
            cleaned = func(value, **kwargs)
            return cleaned, None
        except Exception as e:
            if rule.get('required', False):
                return None, str(e)
            default = rule.get('default', None)
            return default, None

    def clean_row(self, row: Dict[str, str]) -> tuple:
        """Returns (is_valid, cleaned_dict, list_of_errors)"""
        cleaned = {}
        errors = []
        for col, raw_value in row.items():
            val, err = self._clean_value(col, raw_value)
            if err:
                errors.append(f"{col}: {err}")
            cleaned[col] = val
        return len(errors) == 0, cleaned, errors

    def clean_dataset(self, raw_records: List[Dict[str, Any]]) -> tuple:
        """Returns (valid_records, all_errors_per_row)"""
        valid = []
        all_errors = []
        for idx, rec in enumerate(raw_records):
            ok, cleaned, errs = self.clean_row(rec)
            if ok:
                valid.append(cleaned)
            all_errors.append({
                'row_num': idx + 2,  # +2 for header row
                'errors': errs,
            })
        return valid, all_errors


# ------------------------------------------------------------
# File loading (CSV, Excel, JSON, Parquet, Feather)
# ------------------------------------------------------------
def load_any_file(file_path: str) -> List[Dict]:
    """Read any supported file format and return a list of row dicts."""
    if file_path.endswith('.csv'):
        df = pd.read_csv(file_path)
    elif file_path.endswith(('.xls', '.xlsx')):
        df = pd.read_excel(file_path)
    elif file_path.endswith('.json'):
        df = pd.read_json(file_path)
    elif file_path.endswith('.parquet'):
        df = pd.read_parquet(file_path)
    elif file_path.endswith('.feather'):
        df = pd.read_feather(file_path)
    else:
        raise ValueError(f"Unsupported format: {file_path}")

    return df.replace({pd.NA: None, float('nan'): None}).to_dict(orient='records')


def _load_rules(rules_file_path: Optional[str]) -> Optional[Dict]:
    """Load cleaning rules from a JSON file."""
    if rules_file_path and os.path.exists(rules_file_path):
        with open(rules_file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None


# ------------------------------------------------------------
# File processing (CSV, Excel, JSON, Parquet, Feather)
# ------------------------------------------------------------
def process_file(
    file_path: str,
    dataset_name: str,
    dataset_description: str = "",
    rules_file_path: Optional[str] = None,
    replace: bool = False,
):
    """
    Read any supported file, clean it, and store results in the database.

    Supported formats: CSV, Excel (.xls/.xlsx), JSON, Parquet, Feather.
    Format is auto-detected from file extension.

    Args:
        file_path: Path to the data file.
        dataset_name: Name for the Dataset entry.
        dataset_description: Optional description.
        rules_file_path: Optional JSON file with per-column cleaning rules.
        replace: If True, delete existing Datasets with the same name first.

    Returns:
        (dataset_obj, valid_records)
    """
    print(f"\n>>> Processing: {file_path}")
    print(f"    Dataset: {dataset_name}")

    if replace:
        # Drop existing dynamic table before cascade delete
        from core.schema_manager import SchemaManager
        from core.models import DatasetSchema
        old_schema = DatasetSchema.objects.filter(dataset__name=dataset_name).first()
        if old_schema and SchemaManager.table_exists(old_schema.table_name):
            SchemaManager.drop_table(old_schema.table_name)

        deleted, _ = Dataset.objects.filter(name=dataset_name).delete()
        if deleted:
            print(f"    Replaced existing '{dataset_name}' ({deleted} record(s) removed)")

    rules = _load_rules(rules_file_path)
    if rules:
        print(f"    Rules loaded: {list(rules.keys())}")
    else:
        print("    No rules file — all columns treated as string")

    raw_rows = load_any_file(file_path)
    total_rows = len(raw_rows)
    columns = list(raw_rows[0].keys()) if raw_rows else []
    print(f"    Read {total_rows} rows, columns: {columns}")

    # Auto-generate rules from column names if none provided
    if not rules and columns:
        rules = {col: {'type': 'string', 'required': False, 'default': ''} for col in columns}
        print("    Auto-generated string rules for all columns")

    dataset_obj = Dataset.objects.create(
        name=dataset_name,
        description=dataset_description
    )

    cleaner = GenericDataCleaner(rules)
    valid_records, all_errors = cleaner.clean_dataset(raw_rows)

    # Dual-write: 1) JSON backup via DataRecord
    for rec in valid_records:
        DataRecord.objects.create(dataset=dataset_obj, data=rec)

    # Dual-write: 2) Typed dynamic table via SchemaManager
    if rules and valid_records:
        from core.schema_manager import SchemaManager, prepare_for_dynamic_table
        schema_obj = SchemaManager.create_schema_for_dataset(dataset_obj, rules)
        model = SchemaManager.get_model_for_dataset(dataset_obj.id)
        if model:
            for rec in valid_records:
                db_rec = prepare_for_dynamic_table(rec, schema_obj.fields_json)
                model.objects.create(**db_rec)
            print(f"    {len(valid_records)} records in dynamic table '{schema_obj.table_name}'")

    log_result = {
        'total_rows': total_rows,
        'valid_rows': len(valid_records),
        'invalid_rows': total_rows - len(valid_records),
        'errors_per_row': all_errors,
        'file': file_path,
    }
    status = 'completed' if valid_records else 'failed'
    CleaningLog.objects.create(
        dataset=dataset_obj,
        status=status,
        result=log_result
    )

    print(f"    Done: {len(valid_records)} valid, {total_rows - len(valid_records)} invalid")
    return dataset_obj, valid_records


def process_multiple_files(file_configs: List[Dict]):
    """
    Batch-process multiple files.

    file_configs: [
        {
            'path': '/path/to/file.csv',
            'name': 'Dataset Name',
            'description': 'Optional',
            'rules_file': '/path/to/rules.json',
        },
        ...
    ]
    """
    for cfg in file_configs:
        process_file(
            file_path=cfg['path'],
            dataset_name=cfg['name'],
            dataset_description=cfg.get('description', ''),
            rules_file_path=cfg.get('rules_file'),
            replace=cfg.get('replace', False),
        )


# ------------------------------------------------------------
# Programmatic API (works with in-memory data, no file needed)
# ------------------------------------------------------------
def process_generic_dataset(
    dataset_name: str,
    dataset_description: str,
    raw_records: List[Dict[str, Any]],
    cleaning_rules: Dict[str, Any]
):
    """
    Clean a list of dicts and store in the database.
    Use this when data comes from an API, JSON, or other non-CSV source.
    """
    print(f"\n>>> Processing dataset: {dataset_name}")

    dataset_obj = Dataset.objects.create(
        name=dataset_name,
        description=dataset_description
    )

    cleaner = GenericDataCleaner(cleaning_rules)
    valid_records, all_errors = cleaner.clean_dataset(raw_records)

    # Dual-write: JSON backup + typed dynamic table
    for rec in valid_records:
        DataRecord.objects.create(dataset=dataset_obj, data=rec)

    if cleaning_rules and valid_records:
        from core.schema_manager import SchemaManager, prepare_for_dynamic_table
        schema_obj = SchemaManager.create_schema_for_dataset(dataset_obj, cleaning_rules)
        model = SchemaManager.get_model_for_dataset(dataset_obj.id)
        if model:
            for rec in valid_records:
                db_rec = prepare_for_dynamic_table(rec, schema_obj.fields_json)
                model.objects.create(**db_rec)
            print(f"    {len(valid_records)} records in dynamic table '{schema_obj.table_name}'")

    log_result = {
        'total_raw': len(raw_records),
        'valid': len(valid_records),
        'invalid': len(raw_records) - len(valid_records),
        'errors': all_errors,
    }
    status = 'completed' if valid_records else 'failed'
    CleaningLog.objects.create(
        dataset=dataset_obj,
        status=status,
        result=log_result
    )

    print(f"    Saved {len(valid_records)} valid records out of {len(raw_records)}")
    return dataset_obj, valid_records


# ------------------------------------------------------------
# CLI usage
# ------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generic data cleaner — supports CSV, Excel, JSON, Parquet, Feather")
    parser.add_argument('file', help='Path to data file (.csv, .xlsx, .json, .parquet, .feather)')
    parser.add_argument('--name', '-n', required=True, help='Dataset name')
    parser.add_argument('--description', '-d', default='', help='Dataset description')
    parser.add_argument('--rules', '-r', default=None, help='Optional JSON rules file')
    parser.add_argument('--clear', action='store_true', help='Clear all existing data first')
    parser.add_argument('--replace', action='store_true', help='Replace existing dataset with the same name')

    args = parser.parse_args()

    if args.clear:
        DataRecord.objects.all().delete()
        CleaningLog.objects.all().delete()
        Dataset.objects.all().delete()
        print("Cleared existing data.\n")

    process_file(
        file_path=args.file,
        dataset_name=args.name,
        dataset_description=args.description,
        rules_file_path=args.rules,
        replace=args.replace,
    )

    print(f"\nTotal Datasets: {Dataset.objects.count()}")
    print(f"Total DataRecords: {DataRecord.objects.count()}")
    print(f"Total CleaningLogs: {CleaningLog.objects.count()}")
