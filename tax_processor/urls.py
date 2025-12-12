# tax_processor/urls.py
from django.urls import path
from . import views

urlpatterns = [
    # 1. Main Dashboard & Ingestion
    path('', views.user_dashboard, name='user_dashboard'),
    path('upload/', views.upload_statement, name='upload_statement'),

    # 2. Declaration Workflow
    path('declaration/<int:declaration_id>/', views.declaration_detail, name='declaration_detail'),
    path('declaration/<int:declaration_id>/add_statements/', views.add_statements_to_declaration, name='add_statements'),
    path('declaration/<int:declaration_id>/share/', views.share_declaration, name='share_declaration'),
    path('analyze/<int:declaration_id>/', views.run_declaration_analysis, name='run_analysis'),
    path('analyze_pending/<int:declaration_id>/', views.run_analysis_pending, name='run_analysis_pending'),
    path('report/<int:declaration_id>/', views.tax_report, name='tax_report'),
    path('declaration/<int:declaration_id>/transactions/', views.all_transactions_list, name='all_transactions_list'),
    path('transaction/<int:transaction_id>/edit/', views.edit_transaction, name='edit_transaction'),
    path('declaration/<int:declaration_id>/mark_filed/', views.mark_declaration_filed, name='mark_declaration_filed'),

    # 3. Review Queues (Manual Resolution)
    path('review/', views.review_queue, name='review_queue'), # Global queue
    path('review/declaration/<int:declaration_id>/', views.review_queue, name='review_queue_declaration'),
    path('review/resolve/<int:unmatched_id>/', views.resolve_transaction, name='resolve_transaction'),

    # --- NEW: Hint Dismissal URL ---
    path('hints/dismiss/<int:hint_id>/', views.dismiss_hint, name='dismiss_hint'),
    # --- END NEW ---

    # 4. Global Rule Management (Superadmin Only) - Main Category
    path('rules/global/', views.rule_list_global, name='rule_list_global'),
    path('rules/global/create/', views.rule_create_or_update, name='rule_create_global'),
    path('rules/global/update/<int:rule_id>/', views.rule_create_or_update, name='rule_update_global'),
    path('rules/global/delete/<int:rule_id>/', views.rule_delete, name='rule_delete_global'),

    # 5. Declaration-Specific Rule Management - Main Category
    path('declaration/<int:declaration_id>/rules/', views.declaration_rule_list, name='declaration_rule_list'),
    path('declaration/<int:declaration_id>/rules/create/', views.rule_create_or_update, name='declaration_rule_create'),
    path('declaration/<int:declaration_id>/rules/update/<int:rule_id>/', views.rule_create_or_update, name='declaration_rule_update'),
    path('declaration/<int:declaration_id>/rules/delete/<int:rule_id>/', views.rule_delete, name='declaration_rule_delete'),

    # 6. Global Rule Proposal Workflow
    path('rules/propose_global/<int:rule_id>/', views.propose_rule_global, name='propose_rule_global'),
    path('rules/review_global/', views.review_global_proposals, name='review_global_proposals'),
    path('rules/review_global/approve/<int:rule_id>/', views.approve_global_proposal, name='approve_global_proposal'),
    path('rules/review_global/reject/<int:rule_id>/', views.reject_global_proposal, name='reject_global_proposal'),

    # 7. Superadmin: Manual Rule Proposal Workflow
    path('rules/proposals/', views.review_proposals, name='review_proposals'),
    path('rules/proposals/finalize/<int:unmatched_id>/', views.finalize_rule, name='finalize_rule'),
    path('rules/proposals/reject_manual/<int:unmatched_id>/', views.reject_proposal, name='reject_proposal'),

    # --- Entity Type Rule Management (Global) ---
    path('rules/entity/global/', views.entity_rule_list, name='entity_rule_list_global'),
    path('rules/entity/global/create/', views.entity_rule_create_or_update, name='entity_rule_create_global'),
    path('rules/entity/global/update/<int:rule_id>/', views.entity_rule_create_or_update, name='entity_rule_update_global'),
    path('rules/entity/global/delete/<int:rule_id>/', views.entity_rule_delete, name='entity_rule_delete_global'),

    # --- Entity Type Rule Management (Specific) ---
    path('declaration/<int:declaration_id>/rules/entity/', views.entity_rule_list, name='entity_rule_list_specific'),
    path('declaration/<int:declaration_id>/rules/entity/create/', views.entity_rule_create_or_update, name='entity_rule_create_specific'),
    path('declaration/<int:declaration_id>/rules/entity/update/<int:rule_id>/', views.entity_rule_create_or_update, name='entity_rule_update_specific'),
    path('declaration/<int:declaration_id>/rules/entity/delete/<int:rule_id>/', views.entity_rule_delete, name='entity_rule_delete_specific'),

    # --- Transaction Scope Rule Management (Global) ---
    path('rules/scope/global/', views.scope_rule_list, name='scope_rule_list_global'),
    path('rules/scope/global/create/', views.scope_rule_create_or_update, name='scope_rule_create_global'),
    path('rules/scope/global/update/<int:rule_id>/', views.scope_rule_create_or_update, name='scope_rule_update_global'),
    path('rules/scope/global/delete/<int:rule_id>/', views.scope_rule_delete, name='scope_rule_delete_global'),

    # --- Transaction Scope Rule Management (Specific) ---
    path('declaration/<int:declaration_id>/rules/scope/', views.scope_rule_list, name='scope_rule_list_specific'),
    path('declaration/<int:declaration_id>/rules/scope/create/', views.scope_rule_create_or_update, name='scope_rule_create_specific'),
    path('declaration/<int:declaration_id>/rules/scope/update/<int:rule_id>/', views.scope_rule_create_or_update, name='scope_rule_update_specific'),
    path('declaration/<int:declaration_id>/rules/scope/delete/<int:rule_id>/', views.scope_rule_delete, name='scope_rule_delete_specific'),
]
