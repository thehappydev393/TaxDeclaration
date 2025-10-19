# tax_processor/views.py

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.db import IntegrityError
from django.db.models import Sum, F # For Tax Reporting
from .forms import StatementUploadForm, TaxRuleForm
from .services import import_statement_service
from .rules_engine import RulesEngine
from .models import Declaration, Transaction, TaxRule, UnmatchedTransaction
from datetime import date
import json # Used for initial rule boilerplate

# -----------------------------------------------------------
# 1. PERMISSION HELPERS
# -----------------------------------------------------------

def is_superadmin(user):
    """Returns True if the user is authenticated and has the SUPERADMIN role."""
    # Checks the related UserProfile object
    return user.is_authenticated and hasattr(user, 'profile') and user.profile.role == 'SUPERADMIN'

def is_permitted_user(user):
    """Returns True if the user is authenticated and has either SUPERADMIN or REGULAR_USER role."""
    # Checks if the user is authenticated and has a profile with a valid role for general access
    return user.is_authenticated and hasattr(user, 'profile') and user.profile.role in ['SUPERADMIN', 'REGULAR_USER']

# -----------------------------------------------------------
# 2. DATA INGESTION & DECLARATION MANAGEMENT (Regular User Access)
# -----------------------------------------------------------

@user_passes_test(is_permitted_user)
def upload_statement(request):
    """
    Handles file upload, automatically creating or retrieving the Declaration
    based on the Client Name and Year, and iterating over multiple uploaded files.
    """
    if request.method == 'POST':
        form = StatementUploadForm(request.POST, request.FILES)
        if form.is_valid():
            client_name = form.cleaned_data['client_name']
            year = form.cleaned_data['year']
            uploaded_files = request.FILES.getlist('statement_files')

            # --- DECLARATION CREATION/RETRIEVAL LOGIC ---

            user_client_name = client_name
            period_start = date(year, 1, 1)
            period_end = date(year + 1, 1, 31)
            declaration_name = f"{year} Tax Filing - {user_client_name}"

            try:
                declaration_obj, created = Declaration.objects.get_or_create(
                    name=declaration_name,
                    defaults={
                        'tax_period_start': period_start,
                        'tax_period_end': period_end,
                        'client_reference': user_client_name,
                        'status': 'DRAFT',
                    }
                )
                if created:
                    messages.info(request, f"New Declaration '{declaration_name}' created automatically.")

            except IntegrityError:
                messages.error(request, "A database error occurred while trying to find or create the Declaration.")
                return render(request, 'tax_processor/upload_statement.html', {'form': form})

            # --- END DECLARATION LOGIC ---

            total_imported = 0

            # --- Loop over all files ---
            if uploaded_files:
                for uploaded_file in uploaded_files:
                    count, message = import_statement_service(
                        uploaded_file=uploaded_file,
                        declaration_obj=declaration_obj,
                        user=request.user
                    )
                    if count > 0:
                        total_imported += count
                        messages.success(request, f"File {uploaded_file.name}: {message}")
                    else:
                        messages.error(request, f"File {uploaded_file.name}: {message}")

                if total_imported > 0:
                    messages.success(request, f"Batch import complete. Total {total_imported} transactions processed and saved to '{declaration_name}'.")
                    return redirect('upload_statement')
                else:
                    messages.error(request, "Batch import failed. No transactions were successfully processed.")
            else:
                messages.warning(request, "No files were selected for upload.")

    else:
        form = StatementUploadForm()

    return render(request, 'tax_processor/upload_statement.html', {'form': form})


@user_passes_test(is_permitted_user)
def declaration_detail(request, declaration_id):
    """Displays declaration summary and provides the link/button to trigger analysis."""
    declaration = get_object_or_404(Declaration, pk=declaration_id)

    # Calculate stats
    total_statements = declaration.statements.count()
    total_transactions = Transaction.objects.filter(statement__declaration=declaration).count()
    unassigned_transactions = Transaction.objects.filter(statement__declaration=declaration, declaration_point__isnull=True).count()

    context = {
        'declaration': declaration,
        'total_statements': total_statements,
        'total_transactions': total_transactions,
        'unassigned_transactions': unassigned_transactions,
        'is_superadmin': is_superadmin(request.user) # Used to show/hide the Rule Management button
    }
    return render(request, 'tax_processor/declaration_detail.html', context)


@user_passes_test(is_permitted_user)
def run_declaration_analysis(request, declaration_id):
    """Triggers the rule matching engine for a specific declaration."""
    declaration = get_object_or_404(Declaration, pk=declaration_id)

    if request.method == 'POST':
        assigned_user = request.user

        # Initialize and run the engine
        engine = RulesEngine(declaration_id=declaration.pk)
        matched, unmatched = engine.run_analysis(assigned_user=assigned_user)

        messages.success(request, f"Analysis Complete for '{declaration.name}'.")
        messages.info(f"Matched {matched} transactions. Found {unmatched} new transactions requiring manual review.")

        # Redirect back to the detail page
        return redirect('declaration_detail', declaration_id=declaration.pk)

    # If accessed via GET, redirect to detail page (should be triggered via POST form/button)
    return redirect('declaration_detail', declaration_id=declaration.pk)

# -----------------------------------------------------------
# 3. RULE MANAGEMENT (Superadmin Only)
# -----------------------------------------------------------

@user_passes_test(is_superadmin)
def rule_list(request):
    """Displays a list of all Tax Rules (Superadmin dashboard)."""
    rules = TaxRule.objects.all()
    context = {
        'rules': rules,
    }
    return render(request, 'tax_processor/rule_list.html', context)

@user_passes_test(is_superadmin)
def rule_create_or_update(request, rule_id=None):
    """Handles creating a new rule or updating an existing one."""
    if rule_id:
        rule = get_object_or_404(TaxRule, pk=rule_id)
        title = f"Update Rule: {rule.rule_name}"
    else:
        rule = None
        title = "Create New Tax Rule"

    if request.method == 'POST':
        form = TaxRuleForm(request.POST, instance=rule)
        if form.is_valid():
            new_rule = form.save(commit=False)

            if not new_rule.pk:
                new_rule.created_by = request.user

            new_rule.save()
            messages.success(request, f"Tax Rule '{new_rule.rule_name}' saved successfully.")
            return redirect('rule_list')
    else:
        initial_data = {}
        if not rule:
            initial_data['conditions_json'] = json.dumps([
                {"logic": "AND", "checks": [
                    {"field": "description", "type": "CONTAINS_KEYWORD", "value": "Example Keyword"}
                ]}
            ], indent=2)

        form = TaxRuleForm(instance=rule, initial=initial_data)

    context = {'form': form, 'title': title, 'rule': rule}
    return render(request, 'tax_processor/rule_form.html', context)

@user_passes_test(is_superadmin)
def rule_delete(request, rule_id):
    """Deletes a rule (requires POST confirmation)."""
    rule = get_object_or_404(TaxRule, pk=rule_id)

    if request.method == 'POST':
        rule.delete()
        messages.success(request, f"Tax Rule '{rule.rule_name}' successfully deleted.")
        return redirect('rule_list')

    messages.warning(request, "Deletion requires POST confirmation.")
    return redirect('rule_list')

# -----------------------------------------------------------
# 4. UNMATCHED REVIEW QUEUE (Next phase implementation)
# -----------------------------------------------------------

# Future: @user_passes_test(is_permitted_user)
# Future: def review_queue(request):
# Future: def resolve_transaction(request, unmatched_id):
