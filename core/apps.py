from django.apps import AppConfig


class CoreConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'core'

    def ready(self):
        from core.schema_manager import SchemaManager
        from core.admin import register_dynamic_admins
        SchemaManager.register_all_on_startup()
        register_dynamic_admins()
