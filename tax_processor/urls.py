# tax_processor/urls.py

from django.urls import path, include
from . import views

urlpatterns = [
    # Data Ingestion Paths (from previous steps)
    path('upload/', views.upload_statement, name='upload_statement'),
    path('declaration/<int:declaration_id>/', views.declaration_detail, name='declaration_detail'),
    path('analyze/<int:declaration_id>/', views.run_declaration_analysis, name='run_analysis'),

    # Rule Management Paths (NEW)
    path('rules/', views.rule_list, name='rule_list'),
    path('rules/create/', views.rule_create_or_update, name='rule_create'),
    path('rules/edit/<int:rule_id>/', views.rule_create_or_update, name='rule_update'),
    path('rules/delete/<int:rule_id>/', views.rule_delete, name='rule_delete'),
]
