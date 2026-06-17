from django.urls import path
from django.views.generic import RedirectView
from . import views

app_name = 'dataflow'

urlpatterns = [
    path('', RedirectView.as_view(pattern_name='dataflow:db_explorer', permanent=False), name='home'),
    path('upload/', views.upload, name='upload'),
    path('db-explorer/', views.db_explorer, name='db_explorer'),
    path('db-explorer/bulk/', views.db_explorer_bulk_action, name='db_explorer_bulk_action'),
    path('db-explorer/<str:table_name>/', views.db_explorer_table, name='db_explorer_table'),
    path('db-explorer/<str:table_name>/export/', views.db_explorer_export, name='db_explorer_export'),
    path('db-explorer/<str:table_name>/import/', views.db_explorer_import, name='db_explorer_import'),
    path('db-explorer/<str:table_name>/delete/', views.db_explorer_delete, name='db_explorer_delete'),
]
