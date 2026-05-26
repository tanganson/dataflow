from django.urls import path
from . import views

urlpatterns = [
    path('', views.home, name='home'),
    path('upload/', views.upload, name='upload'),
    path('dataset/<int:dataset_id>/', views.dataset_detail, name='dataset_detail'),
    path('dataset/<int:dataset_id>/clean/', views.dataset_clean, name='dataset_clean'),
    path('dataset/<int:dataset_id>/export/', views.dataset_export, name='dataset_export'),
    path('dataset/<int:dataset_id>/delete/', views.dataset_delete, name='dataset_delete'),
]
