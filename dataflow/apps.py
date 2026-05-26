from django.apps import AppConfig


class DataflowConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = "dataflow"

    def ready(self):
        from django.db.utils import OperationalError, ProgrammingError
        try:
            from dataflow.schema_manager import SchemaManager
            from dataflow.admin import register_dynamic_admins
            SchemaManager.register_all_on_startup()
            register_dynamic_admins()
        except (OperationalError, ProgrammingError):
            # Table doesn't exist yet (before migration) — skip silently
            pass
