#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Auto-detect column types from a CSV and generate a cleaning rules JSON file.

Usage:
    python auto_rules.py input.csv output_rules.json
    python auto_rules.py input.csv output_rules.json --sample 100
"""
import csv
import json
import os
import re
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional


def _is_float(v: str) -> bool:
    try:
        float(v)
        return True
    except (ValueError, TypeError):
        return False


def _is_int(v: str) -> bool:
    """True if value is an integer (no decimal point)."""
    try:
        return '.' not in v and int(v) is not None
    except (ValueError, TypeError):
        return False


def _is_email(v: str) -> bool:
    return bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', v))


def _is_date(v: str) -> bool:
    formats = ['%Y-%m-%d', '%Y/%m/%d', '%d-%m-%Y', '%m/%d/%Y',
               '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S']
    for fmt in formats:
        try:
            datetime.strptime(v, fmt)
            return True
        except (ValueError, TypeError):
            continue
    return False


def _is_boolean(v: str) -> bool:
    return v.lower() in ('true', 'false', 'yes', 'no', '1', '0', 't', 'f', 'y', 'n')


def infer_type(values: List[str]) -> str:
    """
    Infer the cleaning rule type for a list of string values.
    Returns one of: int, float, email, date, boolean, string
    """
    non_empty = [v for v in values if v and v.strip()]

    if not non_empty:
        return 'string'

    checks = [
        ('boolean', _is_boolean),
        ('email', _is_email),
        ('date', _is_date),
        ('int', _is_int),
        ('float', _is_float),
    ]

    for type_name, check_fn in checks:
        if all(check_fn(v) for v in non_empty):
            return type_name

    return 'string'


def generate_rules(
    csv_path: str,
    output_path: str,
    sample_rows: int = 200
) -> Dict[str, Any]:
    """
    Read a CSV, infer types, and write a rules JSON file.
    Returns the rules dict.
    """
    with open(csv_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        rows = [row for row in reader]

    # Sample rows for type detection
    sample = rows[:sample_rows]

    rules: Dict[str, Any] = {}
    for col in headers:
        values = [row.get(col, '') for row in sample]
        col_type = infer_type(values)

        rule: Dict[str, Any] = {'type': col_type, 'required': False}

        # Add sensible defaults based on type
        if col_type == 'int':
            rule['default'] = 0
        elif col_type == 'float':
            rule['default'] = 0.0
        elif col_type == 'boolean':
            rule['default'] = False

        rules[col] = rule

    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(rules, f, indent=4, ensure_ascii=False)

    return rules


def rules_to_schema_fields(rules: Dict[str, Any]) -> list:
    """Convert rules dict to fields_json format for DatasetSchema."""
    fields = []
    for col_name, rule in rules.items():
        if isinstance(rule, str):
            rule = {'type': rule}
        fields.append({
            'name': col_name,
            'type': rule.get('type', 'string'),
            'required': rule.get('required', False),
            'default': rule.get('default'),
            'min': rule.get('min'),
            'max': rule.get('max'),
        })
    return fields


def main():
    if len(sys.argv) < 3:
        print("Usage: python auto_rules.py input.csv output_rules.json [--sample N]")
        sys.exit(1)

    csv_path = sys.argv[1]
    output_name = sys.argv[2]
    sample = 200

    for i, arg in enumerate(sys.argv):
        if arg == '--sample' and i + 1 < len(sys.argv):
            sample = int(sys.argv[i + 1])

    rules_dir = os.path.join(os.path.dirname(__file__), 'rules')
    os.makedirs(rules_dir, exist_ok=True)
    output_path = os.path.join(rules_dir, output_name)

    print(f"Reading: {csv_path}")
    rules = generate_rules(csv_path, output_path, sample_rows=sample)

    print(f"Detected {len(rules)} columns:")
    for col, rule in rules.items():
        print(f"  {col:30s} -> {rule['type']}")

    print(f"\nRules saved to: {output_path}")


if __name__ == '__main__':
    main()
