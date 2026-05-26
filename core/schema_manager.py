#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Dynamic model factory — creates real Django models with typed DB columns per dataset.
"""
import re
import keyword
from typing import Any, Dict, List, Optional, Type

from django.apps import apps
from django.db import models, connection
from django.db.models import Manager

DJANGO_RESERVED = {'objects', 'id', 'pk', '_state', 'DoesNotExist', 'MultipleObjectsReturned', 'save', 'delete'}
RE_VALID_COL = re.compile(r'[^a-zA-Z0-9_]')


def _sanitize_column_name(name: str) -> str:
    """Sanitize a column name to be a valid DB/Python identifier."""
    # Replace non-alphanumeric chars with underscores
    name = RE_VALID_COL.sub('_', name)
    # Collapse repeated underscores
    name = re.sub(r'_+', '_', name).strip('_')
    # Prefix if starts with digit
    if not name or name[0].isdigit():
        name = 'c_' + name
    # Avoid Python keywords and Django reserved names
    if keyword.iskeyword(name) or name.lower() in DJANGO_RESERVED:
        name = 'col_' + name
    # Truncate to 63 chars (max identifier length for most DBs)
    return name[:63]


RULE_TYPE_TO_FIELD = {
    'int':     lambda **kw: models.IntegerField(null=not kw.pop('required', False), default=kw.pop('default', None)),
    'float':   lambda **kw: models.FloatField(null=not kw.pop('required', False), default=kw.pop('default', None)),
    'string':  lambda **kw: models.CharField(max_length=kw.pop('max_length', 500), null=not kw.pop('required', False), default=kw.pop('default', ''), blank=True),
    'upper':   lambda **kw: models.CharField(max_length=kw.pop('max_length', 500), null=not kw.pop('required', False), default=kw.pop('default', ''), blank=True),
    'lower':   lambda **kw: models.CharField(max_length=kw.pop('max_length', 500), null=not kw.pop('required', False), default=kw.pop('default', ''), blank=True),
    'email':   lambda **kw: models.CharField(max_length=kw.pop('max_length', 255), null=not kw.pop('required', False), default=kw.pop('default', ''), blank=True),
    'phone':   lambda **kw: models.CharField(max_length=kw.pop('max_length', 20), null=not kw.pop('required', False), default=kw.pop('default', ''), blank=True),
    'boolean': lambda **kw: models.BooleanField(null=not kw.pop('required', False), default=kw.pop('default', False)),
    'date':     lambda **kw: models.DateField(null=not kw.pop('required', False), default=kw.pop('default', None)),
    'datetime': lambda **kw: models.DateTimeField(null=not kw.pop('required', False), default=kw.pop('default', None)),
    'decimal':  lambda **kw: models.DecimalField(max_digits=kw.pop('max_digits', 18), decimal_places=kw.pop('decimal_places', 4), null=not kw.pop('required', False), default=kw.pop('default', None)),
}

# Type values that need conversion from cleaner string output to Python objects
TYPE_NEEDS_CONVERSION = {'date', 'datetime', 'decimal'}


class SchemaManager:
    """Manages dynamic Django model creation, registration, and table lifecycle."""

    MODEL_NAME_PREFIX = 'DynamicDataset'

    # ---------------- Model class factory ----------------
    @classmethod
    def build_model_class(cls, schema_obj) -> Type[models.Model]:
        """Build a Django Model class from a DatasetSchema."""
        field_defs = schema_obj.fields_json  # list of {name, type, required, default, ...}

        attrs: Dict[str, Any] = {
            '__module__': 'core.schema_manager',
            'objects': Manager(),
        }

        # Track used sanitized names to avoid duplicates
        used_names: set = set()

        for fd in field_defs:
            col_name = fd.get('name', '')
            raw_name = _sanitize_column_name(col_name)

            # Deduplicate
            safe_name = raw_name
            counter = 2
            while safe_name in used_names:
                base = raw_name[:60]
                safe_name = f"{base}_{counter}"
                counter += 1
            used_names.add(safe_name)

            rule_type = fd.get('type', 'string')
            field_factory = RULE_TYPE_TO_FIELD.get(rule_type, RULE_TYPE_TO_FIELD['string'])

            # Build kwargs for field factory
            kwargs: Dict[str, Any] = {
                'required': fd.get('required', False),
            }
            if 'default' in fd and fd['default'] is not None:
                kwargs['default'] = fd['default']
            if 'max_length' in fd:
                kwargs['max_length'] = fd['max_length']
            if 'max_digits' in fd:
                kwargs['max_digits'] = fd['max_digits']
            if 'decimal_places' in fd:
                kwargs['decimal_places'] = fd['decimal_places']

            attrs[safe_name] = field_factory(**kwargs)

        # Inner Meta
        class Meta:
            app_label = 'core'
            db_table = schema_obj.table_name
            managed = False
            verbose_name = schema_obj.dataset.name
            verbose_name_plural = schema_obj.dataset.name

        attrs['Meta'] = Meta

        model_class = type(
            f'{cls.MODEL_NAME_PREFIX}_{schema_obj.dataset_id}',
            (models.Model,),
            attrs,
        )
        return model_class

    # ---------------- Table lifecycle ----------------
    @classmethod
    def create_table(cls, model_class: Type[models.Model]) -> None:
        """Create the DB table for a dynamic model using SchemaEditor."""
        with connection.schema_editor() as editor:
            editor.create_model(model_class)

    @classmethod
    def drop_table(cls, table_name: str) -> None:
        """Drop a dynamic table by name."""
        with connection.cursor() as cursor:
            cursor.execute(
                f"DROP TABLE IF EXISTS {connection.ops.quote_name(table_name)}"
            )

    @classmethod
    def table_exists(cls, table_name: str) -> bool:
        return table_name in connection.introspection.table_names()

    # ---------------- App registry management ----------------
    @classmethod
    def register_model(cls, model_class: Type[models.Model]) -> None:
        """Register a dynamic model into Django's app registry."""
        key = model_class.__name__.lower()
        if key in apps.all_models.get('core', {}):
            return
        apps.all_models.setdefault('core', {})[key] = model_class
        apps.clear_cache()

    @classmethod
    def get_model_for_dataset(cls, dataset_id: int) -> Optional[Type[models.Model]]:
        """Retrieve a previously registered dynamic model by dataset ID."""
        key = f'{cls.MODEL_NAME_PREFIX}_{dataset_id}'.lower()
        return apps.all_models.get('core', {}).get(key)

    # ---------------- Startup re-registration ----------------
    @classmethod
    def register_all_on_startup(cls) -> None:
        """Rebuild and register all dynamic models from stored schemas. Called from AppConfig.ready()."""
        from django.db.utils import OperationalError, ProgrammingError
        from core.models import DatasetSchema

        try:
            schemas = list(DatasetSchema.objects.select_related('dataset').all())
        except (OperationalError, ProgrammingError):
            # Table doesn't exist yet (before migration)
            return

        for schema_obj in schemas:
            try:
                model_class = cls.build_model_class(schema_obj)
                cls.register_model(model_class)

                if not cls.table_exists(schema_obj.table_name):
                    cls.create_table(model_class)
            except Exception:
                # If a schema is broken, skip it rather than crashing startup
                pass

    # ---------------- Schema creation (on import) ----------------
    @classmethod
    def create_schema_for_dataset(cls, dataset, rules: Dict[str, Any]) -> 'DatasetSchema':
        """
        Convert rules dict to fields_json, create DatasetSchema, build model, create table, register.

        Args:
            dataset: Dataset instance.
            rules: Dict of column_name -> rule dict, same format as GenericDataCleaner uses.

        Returns:
            DatasetSchema instance.
        """
        from core.models import DatasetSchema

        # Convert rules to fields_json
        if isinstance(rules, dict):
            fields_json = _rules_to_schema_fields(rules)
        else:
            fields_json = rules or []

        table_name = f"core_dataset_{dataset.id}"

        schema_obj = DatasetSchema.objects.create(
            dataset=dataset,
            table_name=table_name,
            fields_json=fields_json,
        )

        model_class = cls.build_model_class(schema_obj)
        cls.create_table(model_class)
        cls.register_model(model_class)

        return schema_obj


def _rules_to_schema_fields(rules: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert rules dict format to fields_json format."""
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
            'max_length': rule.get('max_length'),
        })
    return fields


def prepare_for_dynamic_table(record: Dict[str, Any], fields_json: List[Dict]) -> Dict[str, Any]:
    """
    Convert cleaned values to types expected by Django dynamic model fields.
    e.g. date string -> date object, decimal string -> Decimal.
    """
    from datetime import date, datetime
    from decimal import Decimal

    type_map = {fd['name']: fd.get('type', 'string') for fd in fields_json}
    sanitize_map = {fd['name']: _sanitize_column_name(fd['name']) for fd in fields_json}

    result = {}
    for key, value in record.items():
        safe_key = sanitize_map.get(key, _sanitize_column_name(key))
        col_type = type_map.get(key, 'string')

        if value is None:
            result[safe_key] = None
            continue

        if col_type == 'date' and isinstance(value, str):
            result[safe_key] = date.fromisoformat(value)
        elif col_type == 'datetime' and isinstance(value, str):
            result[safe_key] = datetime.fromisoformat(value)
        elif col_type == 'decimal' and not isinstance(value, Decimal):
            result[safe_key] = Decimal(str(value))
        else:
            result[safe_key] = value

    return result
