from django.urls import path
from . import views

app_name = 'dataflow'

urlpatterns = [
    path('', views.home, name='home'),
    path('upload/', views.upload, name='upload'),
    path('datasets/bulk/', views.dataset_bulk_action, name='dataset_bulk_action'),
    path('dataset/<int:dataset_id>/', views.dataset_detail, name='dataset_detail'),
    path('dataset/<int:dataset_id>/clean/', views.dataset_clean, name='dataset_clean'),
    path('dataset/<int:dataset_id>/export/', views.dataset_export, name='dataset_export'),
    path('dataset/<int:dataset_id>/delete/', views.dataset_delete, name='dataset_delete'),
    path('db-explorer/', views.db_explorer, name='db_explorer'),
    path('db-explorer/bulk/', views.db_explorer_bulk_action, name='db_explorer_bulk_action'),
    path('db-explorer/<str:table_name>/', views.db_explorer_table, name='db_explorer_table'),
    path('db-explorer/<str:table_name>/export/', views.db_explorer_export, name='db_explorer_export'),
    path('db-explorer/<str:table_name>/import/', views.db_explorer_import, name='db_explorer_import'),
    path('db-explorer/<str:table_name>/delete/', views.db_explorer_delete, name='db_explorer_delete'),
]
