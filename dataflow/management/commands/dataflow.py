"""Django management command for the Dataflow ETL Pipeline.

Usage:
    python manage.py dataflow run data.csv --name "dataset"
    python manage.py dataflow export "dataset" -o cleaned.csv
    python manage.py dataflow report "dataset"
    python manage.py dataflow list
    python manage.py dataflow clean "dataset" --rules rules.json
    python manage.py dataflow delete "dataset"
    python manage.py dataflow rules data.csv -o rules.json
"""
from django.core.management.base import BaseCommand


class Command(BaseCommand):
    help = 'Dataflow ETL Pipeline — clean, format, store, and export data.'

    def add_arguments(self, parser):
        sub = parser.add_subparsers(dest='subcommand', required=True)

        # run
        run_p = sub.add_parser('run', help='Load, clean, and store a file')
        run_p.add_argument('file', help='Path to data file')
        run_p.add_argument('--name', '-n', default=None)
        run_p.add_argument('--rules', '-r', default=None)
        run_p.add_argument('--replace', action='store_true')
        run_p.add_argument('--export', '-o', default=None)
        run_p.add_argument('--dry-run', action='store_true')
        run_p.add_argument('--verbose', '-v', action='store_true')
        run_p.add_argument('--quiet', '-q', action='store_true')

        # export
        exp_p = sub.add_parser('export', help='Export a stored dataset to file')
        exp_p.add_argument('name')
        exp_p.add_argument('--output', '-o', required=True)

        # report
        rep_p = sub.add_parser('report', help='Show dataset summary')
        rep_p.add_argument('name')

        # list
        sub.add_parser('list', help='List all datasets')

        # clean
        cln_p = sub.add_parser('clean', help='Re-clean a stored dataset')
        cln_p.add_argument('name')
        cln_p.add_argument('--rules', '-r', default=None)

        # delete
        del_p = sub.add_parser('delete', help='Delete a stored dataset')
        del_p.add_argument('name')

        # rules
        rul_p = sub.add_parser('rules', help='Generate rules JSON from CSV')
        rul_p.add_argument('file')
        rul_p.add_argument('--output', '-o', default=None)
        rul_p.add_argument('--sample', '-s', type=int, default=200)

    def handle(self, *args, **options):
        import logging
        from dataflow.cli import (
            Pipeline,
            export_dataset,
            report_dataset,
            list_datasets,
            clean_dataset,
            delete_dataset,
            generate_rules_file,
            _validate_file_exists,
        )

        verbosity = options.get('verbosity', 1)
        subcommand = options['subcommand']

        # Configure logging
        if verbosity >= 2:
            level = logging.DEBUG
        elif verbosity == 0:
            level = logging.WARNING
        else:
            level = logging.INFO
        logging.basicConfig(level=level, format='%(message)s', stream=self.stdout)

        if subcommand == 'run':
            _validate_file_exists(options['file'])
            name = options['name'] or os.path.splitext(os.path.basename(options['file']))[0]
            p = Pipeline(name)
            p.load_file(options['file'])
            if options['rules']:
                p.load_rules_file(options['rules'])
            else:
                p.auto_rules()
            p.clean()
            if options['dry_run']:
                p.report()
            else:
                p.store(replace=options['replace'])
                p.report()
                if options['export']:
                    p.export(options['export'])

        elif subcommand == 'export':
            export_dataset(options['name'], options['output'])

        elif subcommand == 'report':
            report_dataset(options['name'])

        elif subcommand == 'list':
            list_datasets()

        elif subcommand == 'clean':
            clean_dataset(options['name'], options['rules'])

        elif subcommand == 'delete':
            delete_dataset(options['name'])

        elif subcommand == 'rules':
            _validate_file_exists(options['file'])
            generate_rules_file(options['file'], options['output'], options['sample'])


import os  # noqa: E402 — needed for handle() inline name derivation
