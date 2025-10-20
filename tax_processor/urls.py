# tax_processor/urls.py (Update)

from django.urls import path
from . import views

urlpatterns = [
    # Dashboard / Home view for the app
    path('', views.user_dashboard, name='user_dashboard'),

    # Data Ingestion Paths
    path('upload/', views.upload_statement, name='upload_statement'),

    # Analysis Paths
    path('declaration/<int:declaration_id>/', views.declaration_detail, name='declaration_detail'),
    path('analyze/<int:declaration_id>/', views.run_declaration_analysis, name='run_analysis'),

    # Review Queue Paths (NEW)
    path('review/', views.review_queue, name='review_queue'),
    path('review/<int:unmatched_id>/resolve/', views.resolve_transaction, name='resolve_transaction'),

    # Reporting Paths (NEW)
    path('report/<int:declaration_id>/', views.tax_report, name='tax_report'),

    # Rule Management Paths
    path('rules/', views.rule_list, name='rule_list'),
    path('rules/create/', views.rule_create_or_update, name='rule_create'),
    path('rules/edit/<int:rule_id>/', views.rule_create_or_update, name='rule_update'),
    path('rules/delete/<int:rule_id>/', views.rule_delete, name='rule_delete'),

    # Proposal Review Paths (NEW)
    path('rules/proposals/', views.review_proposals, name='review_proposals'),
    path('rules/proposals/<int:unmatched_id>/finalize/', views.finalize_rule, name='finalize_rule'),
]
