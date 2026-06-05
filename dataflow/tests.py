from django.core.files.uploadedfile import SimpleUploadedFile
from django.db import connection
from django.test import TestCase
from django.urls import reverse

from dataflow.models import Dataset, DataRecord, DatasetSchema


class UploadViewTests(TestCase):
    def tearDown(self):
        with connection.cursor() as cursor:
            cursor.execute('DROP TABLE IF EXISTS course')
            cursor.execute('DROP TABLE IF EXISTS course_sub_category')
            cursor.execute('DROP TABLE IF EXISTS course_main_category')
            cursor.execute('DROP TABLE IF EXISTS taggit_tag')
        super().tearDown()

    def test_folder_upload_imports_each_csv_as_dataset_when_requested(self):
        customers = SimpleUploadedFile(
            'customers.csv',
            b'id,name\n1,Ada\n2,Grace\n',
            content_type='text/csv',
        )
        orders = SimpleUploadedFile(
            'orders.csv',
            b'order_id,total\n100,19.5\n',
            content_type='text/csv',
        )
        notes = SimpleUploadedFile(
            'notes.txt',
            b'not,a,csv\n',
            content_type='text/plain',
        )

        response = self.client.post(reverse('dataflow:upload'), {
            'upload_mode': 'folder',
            'target_mode': 'dataset',
            'name': 'batch',
            'folder_files': [customers, orders, notes],
        })

        self.assertRedirects(response, reverse('dataflow:home'))
        self.assertQuerySetEqual(
            Dataset.objects.order_by('name').values_list('name', flat=True),
            ['batch_customers', 'batch_orders'],
        )
        self.assertEqual(DataRecord.objects.count(), 3)
        self.assertEqual(DatasetSchema.objects.count(), 2)

    def test_folder_upload_defaults_to_matching_existing_table(self):
        with connection.cursor() as cursor:
            cursor.execute(
                'CREATE TABLE course ('
                'id integer PRIMARY KEY, '
                'title varchar(100), '
                'content text NOT NULL, '
                'is_published boolean)'
            )
            cursor.execute(
                'INSERT INTO course (id, title, content, is_published) VALUES (%s, %s, %s, %s)',
                [99, 'Old Course', 'old content', True],
            )

        courses = SimpleUploadedFile(
            'course.csv',
            b'id,title,is_published\n1,Math,1\n2,Science,0\n',
            content_type='text/csv',
        )

        response = self.client.post(reverse('dataflow:upload'), {
            'upload_mode': 'folder',
            'replace': 'on',
            'folder_files': [courses],
        })

        self.assertRedirects(response, reverse('dataflow:home'))
        self.assertEqual(Dataset.objects.count(), 0)
        self.assertEqual(DataRecord.objects.count(), 0)

        with connection.cursor() as cursor:
            cursor.execute('SELECT id, title, content, is_published FROM course ORDER BY id')
            rows = cursor.fetchall()

        self.assertEqual(rows, [(1, 'Math', '', True), (2, 'Science', '', False)])

    def test_folder_table_upload_creates_missing_table_from_csv(self):
        with connection.cursor() as cursor:
            cursor.execute(
                'CREATE TABLE course ('
                'id integer PRIMARY KEY, '
                'title varchar(100))'
            )

        courses = SimpleUploadedFile(
            'course.csv',
            b'id,title\n1,Math\n',
            content_type='text/csv',
        )
        missing = SimpleUploadedFile(
            'taggit_tag.csv',
            b'id,name\n1,tag\n',
            content_type='text/csv',
        )

        response = self.client.post(reverse('dataflow:upload'), {
            'upload_mode': 'folder',
            'replace': 'on',
            'folder_files': [missing, courses],
        })

        self.assertRedirects(response, reverse('dataflow:home'))
        with connection.cursor() as cursor:
            cursor.execute('SELECT id, title FROM course')
            self.assertEqual(cursor.fetchall(), [(1, 'Math')])
            cursor.execute('SELECT id, name FROM taggit_tag')
            self.assertEqual(cursor.fetchall(), [(1, 'tag')])

    def test_single_table_upload_creates_missing_table_from_csv(self):
        tags = SimpleUploadedFile(
            'taggit_tag.csv',
            b'id,name\n1,tag\n',
            content_type='text/csv',
        )

        response = self.client.post(reverse('dataflow:upload'), {
            'target_mode': 'table',
            'file': tags,
        })

        self.assertRedirects(response, reverse('dataflow:db_explorer_table', kwargs={'table_name': 'taggit_tag'}))
        with connection.cursor() as cursor:
            cursor.execute('SELECT id, name FROM taggit_tag')
            self.assertEqual(cursor.fetchall(), [(1, 'tag')])

    def test_folder_table_upload_orders_foreign_key_parents_first(self):
        with connection.cursor() as cursor:
            cursor.execute(
                'CREATE TABLE course_main_category ('
                'id integer PRIMARY KEY, '
                'name varchar(100) NOT NULL)'
            )
            cursor.execute(
                'CREATE TABLE course_sub_category ('
                'id integer PRIMARY KEY, '
                'main_category_id integer NOT NULL REFERENCES course_main_category(id), '
                'name varchar(100) NOT NULL)'
            )
            cursor.execute(
                'CREATE TABLE course ('
                'id integer PRIMARY KEY, '
                'sub_category_id integer NOT NULL REFERENCES course_sub_category(id), '
                'title varchar(100))'
            )

        course = SimpleUploadedFile(
            'course.csv',
            b'id,sub_category_id,title\n3,9,HAJIHAJI\n',
            content_type='text/csv',
        )
        sub_category = SimpleUploadedFile(
            'course_sub_category.csv',
            b'id,main_category_id,name\n9,2,Language\n',
            content_type='text/csv',
        )
        main_category = SimpleUploadedFile(
            'course_main_category.csv',
            b'id,name\n2,Main\n',
            content_type='text/csv',
        )

        response = self.client.post(reverse('dataflow:upload'), {
            'upload_mode': 'folder',
            'replace': 'on',
            'folder_files': [course, sub_category, main_category],
        })

        self.assertRedirects(response, reverse('dataflow:home'))
        with connection.cursor() as cursor:
            cursor.execute('SELECT id, name FROM course_main_category')
            self.assertEqual(cursor.fetchall(), [(2, 'Main')])
            cursor.execute('SELECT id, main_category_id, name FROM course_sub_category')
            self.assertEqual(cursor.fetchall(), [(9, 2, 'Language')])
            cursor.execute('SELECT id, sub_category_id, title FROM course')
            self.assertEqual(cursor.fetchall(), [(3, 9, 'HAJIHAJI')])

    def test_dataset_bulk_delete_removes_selected_datasets(self):
        keep = Dataset.objects.create(name='keep', description='')
        remove_one = Dataset.objects.create(name='remove_one', description='')
        remove_two = Dataset.objects.create(name='remove_two', description='')

        response = self.client.post(reverse('dataflow:dataset_bulk_action'), {
            'bulk_action': 'delete',
            'dataset_ids': [str(remove_one.id), str(remove_two.id)],
        })

        self.assertRedirects(response, reverse('dataflow:home'))
        self.assertQuerySetEqual(
            Dataset.objects.order_by('name').values_list('name', flat=True),
            [keep.name],
        )

    def test_db_explorer_bulk_link_selected_tables_as_datasets(self):
        with connection.cursor() as cursor:
            cursor.execute(
                'CREATE TABLE course ('
                'id integer PRIMARY KEY, '
                'title varchar(100))'
            )

        response = self.client.post(reverse('dataflow:db_explorer_bulk_action'), {
            'bulk_action': 'link_dataset',
            'table_names': ['course'],
        })

        self.assertRedirects(response, reverse('dataflow:db_explorer'))
        dataset = Dataset.objects.get(name='course')
        self.assertEqual(dataset.description, '__ref:course')

    def test_db_explorer_bulk_drop_selected_tables(self):
        with connection.cursor() as cursor:
            cursor.execute(
                'CREATE TABLE course ('
                'id integer PRIMARY KEY, '
                'title varchar(100))'
            )

        response = self.client.post(reverse('dataflow:db_explorer_bulk_action'), {
            'bulk_action': 'drop',
            'table_names': ['course', 'auth_user'],
        })

        self.assertRedirects(response, reverse('dataflow:db_explorer'))
        self.assertNotIn('course', connection.introspection.table_names())
        self.assertIn('auth_user', connection.introspection.table_names())
