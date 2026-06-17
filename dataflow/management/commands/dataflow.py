from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Dataflow Manager — interactive wizard for DB table management.'

    def handle(self, *args, **options):
        from dataflow.cli import run_wizard
        run_wizard()
