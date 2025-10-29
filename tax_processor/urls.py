# tax_processor/urls.py
from django.urls import path
from . import views

urlpatterns = [
    # 1. Main Dashboard & Ingestion
    path('', views.user_dashboard, name='user_dashboard'),
    path('upload/', views.upload_statement, name='upload_statement'),

    # 2. Declaration Workflow
    path('declaration/<int:declaration_id>/', views.declaration_detail, name='declaration_detail'),
    path('declaration/<int:declaration_id>/add_statements/', views.add_statements_to_declaration, name='add_statements'),path('declaration/<int:declaration_id>/add_statements/', views.add_statements_to_declaration, name='add_statements'),
    path('analyze/<int:declaration_id>/', views.run_declaration_analysis, name='run_analysis'), # Changed path from declaration/.../run_analysis
    path('analyze_pending/<int:declaration_id>/', views.run_analysis_pending, name='run_analysis_pending'),
    path('report/<int:declaration_id>/', views.tax_report, name='tax_report'), # Changed path from declaration/.../report
    path('declaration/<int:declaration_id>/transactions/', views.all_transactions_list, name='all_transactions_list'),

    # 3. Review Queues (Manual Resolution)
    path('review/', views.review_queue, name='review_queue'), # Global queue
    path('review/declaration/<int:declaration_id>/', views.review_queue, name='review_queue_declaration'), # Filtered queue - path changed
    path('review/resolve/<int:unmatched_id>/', views.resolve_transaction, name='resolve_transaction'), # Changed path

    # 4. Global Rule Management (Superadmin Only) - Paths adjusted slightly for clarity
    path('rules/global/', views.rule_list_global, name='rule_list_global'), # Renamed view name
    path('rules/global/create/', views.rule_create_or_update, name='rule_create_global'), # Renamed view name
    path('rules/global/update/<int:rule_id>/', views.rule_create_or_update, name='rule_update_global'), # Renamed view name
    path('rules/global/delete/<int:rule_id>/', views.rule_delete, name='rule_delete_global'), # Renamed view name

    # 5. Declaration-Specific Rule Management (NEW)
    path('declaration/<int:declaration_id>/rules/', views.declaration_rule_list, name='declaration_rule_list'),
    path('declaration/<int:declaration_id>/rules/create/', views.rule_create_or_update, name='declaration_rule_create'),
    path('declaration/<int:declaration_id>/rules/update/<int:rule_id>/', views.rule_create_or_update, name='declaration_rule_update'),
    path('declaration/<int:declaration_id>/rules/delete/<int:rule_id>/', views.rule_delete, name='declaration_rule_delete'),

    # 6. Global Rule Proposal Workflow (NEW)
    path('rules/propose_global/<int:rule_id>/', views.propose_rule_global, name='propose_rule_global'), # For user to propose
    path('rules/review_global/', views.review_global_proposals, name='review_global_proposals'), # For admin to list proposals
    path('rules/review_global/approve/<int:rule_id>/', views.approve_global_proposal, name='approve_global_proposal'), # For admin to approve
    path('rules/review_global/reject/<int:rule_id>/', views.reject_global_proposal, name='reject_global_proposal'), # For admin to reject

    # 7. Superadmin: Manual Rule Proposal Workflow (Existing - paths adjusted for consistency)
    path('rules/proposals/', views.review_proposals, name='review_proposals'),
    path('rules/proposals/finalize/<int:unmatched_id>/', views.finalize_rule, name='finalize_rule'),
    path('rules/proposals/reject_manual/<int:unmatched_id>/', views.reject_proposal, name='reject_proposal'), # Renamed view name

]
