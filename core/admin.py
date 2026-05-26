from django.contrib import admin
from .models import Dataset, DataRecord, CleaningLog


@admin.register(Dataset)
class DatasetAdmin(admin.ModelAdmin):
    list_display = ('id', 'name', 'updated_at')
    search_fields = ('name',)


@admin.register(DataRecord)
class DataRecordAdmin(admin.ModelAdmin):
    list_display = ('id', 'dataset', 'created_at')
    list_filter = ('dataset', 'created_at')
    search_fields = ('data',)


@admin.register(CleaningLog)
class CleaningLogAdmin(admin.ModelAdmin):
    list_display = ('id', 'dataset', 'status', 'created_at')
    list_filter = ('status', 'dataset')


# ------------------------------------------------------------
# Dynamic admin registration
# ------------------------------------------------------------
def _build_dynamic_admin(model_class, field_names):
    """Create a ModelAdmin showing typed columns for a dynamic model."""
    list_display = ('id',) + tuple(field_names)

    admin_class = type(
        f'{model_class.__name__}Admin',
        (admin.ModelAdmin,),
        {
            'list_display': list_display,
            'search_fields': field_names[:10],
            'list_per_page': 50,
        },
    )
    return admin_class


def _register_single_admin(model_class, field_names):
    """Register a single dynamic model's admin. Called at import time (not just startup)."""
    if model_class in admin.site._registry:
        return
    admin_class = _build_dynamic_admin(model_class, field_names)
    admin.site.register(model_class, admin_class)


def register_dynamic_admins():
    """Register dynamic model admins. Called from AppConfig.ready()."""
    from django.db.utils import OperationalError, ProgrammingError
    from core.models import DatasetSchema
    from core.schema_manager import SchemaManager

    try:
        schemas = list(DatasetSchema.objects.select_related('dataset').all())
    except (OperationalError, ProgrammingError):
        return

    for schema_obj in schemas:
        model = SchemaManager.get_model_for_dataset(schema_obj.dataset_id)
        if model is None:
            # Model not in this process's app registry yet — rebuild and register
            model = SchemaManager.build_model_class(schema_obj)
            SchemaManager.register_model(model)
        if model in admin.site._registry:
            continue

        sanitized_names = []
        for fd in schema_obj.fields_json:
            from core.schema_manager import _sanitize_column_name
            sanitized_names.append(_sanitize_column_name(fd.get('name', '')))

        admin_class = _build_dynamic_admin(model, sanitized_names)
        admin.site.register(model, admin_class)

    # Unregister stale dynamic models whose schemas have been deleted
    active_tables = {s.table_name for s in schemas}
    stale = []
    for model in list(admin.site._registry):
        if 'DynamicDataset' in model.__name__:
            if model._meta.db_table not in active_tables:
                stale.append(model)
    for model in stale:
        admin.site.unregister(model)
