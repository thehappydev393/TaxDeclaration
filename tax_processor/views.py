# tax_processor/views.py

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.db import IntegrityError
from django.db.models import Q, Count, Sum
from django.utils import timezone
from .forms import StatementUploadForm, TaxRuleForm, ResolutionForm, BaseConditionFormSet
from .services import import_statement_service
from .rules_engine import RulesEngine
from .models import Declaration, Transaction, TaxRule, UnmatchedTransaction, UserProfile
from datetime import date
import json
from django.db import transaction as db_transaction

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
    based on the Client Name and Year, and assigning the task to the logged-in user.
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

            # 2. Define the unique Declaration Name based on Year and Client Name
            declaration_name = f"{year} Հայտարարագիր - {client_name}"

            try:
                declaration_obj, created = Declaration.objects.get_or_create(
                    name=declaration_name,
                    defaults={
                        'tax_period_start': period_start,
                        'tax_period_end': period_end,
                        'client_reference': client_name,
                        'status': 'DRAFT',
                        'created_by': request.user,
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

def filter_declarations_by_user(user):
    """Filters declarations based on user role (Superadmin sees all; Regular User sees only their own)."""
    if is_superadmin(user):
        return Declaration.objects.all()
    else:
        # Regular users only see declarations they created
        return Declaration.objects.filter(created_by=user)

@user_passes_test(is_permitted_user)
def declaration_detail(request, declaration_id):
    """Displays declaration summary and provides the link/button to trigger analysis."""
    declaration_qs = filter_declarations_by_user(request.user)
    declaration = get_object_or_404(declaration_qs, pk=declaration_id)

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
        matched, new_unmatched, cleared_unmatched = engine.run_analysis(assigned_user=assigned_user)

        # Success Message
        messages.success(request, f"Վերլուծությունն ավարտվեց «{declaration.name}»-ի համար:")

        # Summary 1: Total processed
        total_processed = matched + new_unmatched + cleared_unmatched
        messages.info(request, f"Ընդհանուր մշակված գործարքներ՝ {total_processed}")

        # Summary 2: Matched and Cleared
        messages.info(request, f"Համընկել է {matched} նոր/վերագնահատված գործարք։ Մաքրվել է {cleared_unmatched} գոյություն ունեցող վերանայման գործարք։")

        # Summary 3: New Unmatched Items
        messages.info(request, f"Հայտնաբերվել է {new_unmatched} նոր գործարք, որոնք պահանջում են ձեռքով վերանայում։")

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
    """
    Handles creating a new rule or updating an existing one using
    a dynamic formset for conditions.
    """
    if rule_id:
        rule = get_object_or_404(TaxRule, pk=rule_id)
        title = f"Update Rule: {rule.rule_name}"
    else:
        rule = None
        title = "Create New Tax Rule"

    # A "prefix" is necessary when using multiple formsets on one page
    formset_prefix = 'conditions'

    if request.method == 'POST':
        form = TaxRuleForm(request.POST, instance=rule)
        formset = BaseConditionFormSet(request.POST, prefix=formset_prefix)

        if form.is_valid() and formset.is_valid():
            new_rule = form.save(commit=False)

            if not new_rule.pk:
                new_rule.created_by = request.user

            # --- SERIALIZE FORMSET DATA TO JSON ---
            checks = []
            for check_form in formset.cleaned_data:
                # Only include non-empty forms that haven't been marked for deletion
                if check_form and not check_form.get('DELETE'):
                    checks.append({
                        'field': check_form['field'],
                        'type': check_form['condition_type'],
                        'value': check_form['value']
                    })

            # We wrap it in a list to match the engine's expected structure
            new_rule.conditions_json = [{
                'logic': form.cleaned_data['logic'],
                'checks': checks
            }]
            # --- END SERIALIZATION ---

            new_rule.save()
            messages.success(request, f"Tax Rule '{new_rule.rule_name}' saved successfully.")
            return redirect('rule_list')

    else:
        # --- DESERIALIZE JSON TO FORMSET (for GET request) ---
        initial_form_data = {}
        initial_formset_data = []

        if rule:
            # If editing an existing rule, parse its JSON
            if rule.conditions_json and isinstance(rule.conditions_json, list) and len(rule.conditions_json) > 0 and rule.conditions_json[0]:
                logic_block = rule.conditions_json[0]
                initial_form_data['logic'] = logic_block.get('logic', 'AND')

                # Re-map the check structure to the formset structure
                for check in logic_block.get('checks', []):
                    initial_formset_data.append({
                        'field': check.get('field'),
                        'condition_type': check.get('type'),
                        'value': check.get('value')
                    })

        else:
            # This is a new rule
            initial_form_data['logic'] = 'AND'
            # (formset will be blank, extra=1 handles it)

        form = TaxRuleForm(instance=rule, initial=initial_form_data)
        formset = BaseConditionFormSet(initial=initial_formset_data, prefix=formset_prefix)
        # --- END DESERIALIZATION ---

    context = {
        'form': form,
        'formset': formset, # Pass the formset to the template
        'title': title,
        'rule': rule
    }
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

@user_passes_test(is_permitted_user)
def user_dashboard(request):
    """
    Displays the list of Declarations created by the user (or all for Superadmin),
    and checks for pending rule proposals.
    """
    user = request.user

    # Use the filtering helper function we already created
    declarations_qs = filter_declarations_by_user(user)

    # Annotate the queryset to efficiently count related data for the dashboard view
    declarations = declarations_qs.annotate(
        statement_count=Count('statements', distinct=True),
        total_transactions=Count('statements__transactions', distinct=True),
        unmatched_count=Count(
            'statements__transactions__unmatched_record',
            filter=Q(statements__transactions__unmatched_record__status='PENDING_REVIEW'),
            distinct=True
        )
    ).order_by('-tax_period_start')

    # --- CRITICAL NEW LOGIC: Count total pending rule proposals (Superadmin task) ---
    pending_proposals_count = 0
    if is_superadmin(user):
        pending_proposals_count = UnmatchedTransaction.objects.filter(
            status='NEW_RULE_PROPOSED'
        ).count()
    # ---------------------------------------------------------------------------------

    context = {
        'declarations': declarations,
        'is_admin': is_superadmin(user),
        'pending_proposals_count': pending_proposals_count, # Pass the count to the template
    }
    return render(request, 'tax_processor/user_dashboard.html', context)


@user_passes_test(is_permitted_user)
def review_queue(request, declaration_id=None): # ADDED declaration_id=None
    """
    Displays the list of PENDING_REVIEW transactions, filtered by declaration_id
    if provided, otherwise following user/admin rules.
    """
    user = request.user

    # Base Query: Filter by status
    unmatched_qs = UnmatchedTransaction.objects.filter(status='PENDING_REVIEW')

    is_filtered_by_declaration = False

    if declaration_id:
        # If ID is provided, filter by that specific declaration and enforce access check
        declaration = get_object_or_404(Declaration, pk=declaration_id)

        # Enforce access: Only Superadmin or the creator can view this Declaration's queue
        if not (is_superadmin(user) or declaration.created_by == user):
            messages.error(request, "Դուք թույլտվություն չունեք դիտելու այս հայտարարագրի վերանայման հերթը։")
            return redirect('user_dashboard')

        unmatched_qs = unmatched_qs.filter(transaction__statement__declaration_id=declaration_id)
        title = f"Վերանայման Հերթ - {declaration.name}"
        is_filtered_by_declaration = True

    elif is_superadmin(user):
        # Admin Global View
        title = "SUPERADMIN-ի Վերանայման Հերթ (Բոլոր Սպասողները)"

    else:
        # Regular User Global View (Only sees their own assigned items)
        unmatched_qs = unmatched_qs.filter(assigned_user=user)
        title = f"{user.username}-ի Սպասվող Վերանայումները"

    # Pre-fetch the related transaction and statement data
    unmatched_items = unmatched_qs.select_related(
        'transaction__statement__declaration',
        'transaction__matched_rule',
        'assigned_user'
    ).order_by('-transaction__transaction_date')

    context = {
        'title': title,
        'unmatched_items': unmatched_items,
        'is_admin': is_superadmin(user),
        'is_filtered': is_filtered_by_declaration,
        'current_declaration_id': declaration_id,
    }
    return render(request, 'tax_processor/review_queue.html', context)

@user_passes_test(is_permitted_user)
def resolve_transaction(request, unmatched_id):
    """
    Displays the form for a user to resolve an unmatched transaction and updates
    the related Transaction and UnmatchedTransaction records.
    """
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)
    tx = unmatched_item.transaction

    # Permission Check: Only the assigned user or a Superadmin can resolve
    if not (request.user == unmatched_item.assigned_user or is_superadmin(request.user)):
        messages.error(request, "You are not authorized to resolve this transaction.")
        return redirect('review_queue')

    if request.method == 'POST':
        # Pass request.POST and initialize the form with the instance's transaction ID
        form = ResolutionForm(request.POST)
        if form.is_valid():
            # Form uses ModelChoiceField, so we get the DeclarationPoint object
            resolved_point_obj = form.cleaned_data['resolved_point']
            propose_rule = form.cleaned_data['propose_rule']
            rule_notes = form.cleaned_data['rule_notes']

            # 1. Update the original Transaction record
            tx.declaration_point = resolved_point_obj # Assign the DeclarationPoint object
            tx.save()

            # 2. Update the UnmatchedTransaction status and resolution info
            unmatched_item.resolved_point = resolved_point_obj.name # Store name for simple audit
            unmatched_item.resolution_date = timezone.now()
            unmatched_item.status = 'RESOLVED'

            # 3. Handle Rule Proposal (System Learning)
            if propose_rule:
                # Store necessary info for Superadmin to build the rule later
                proposal_data = {
                    'resolved_point_id': resolved_point_obj.pk,
                    'resolved_point_name': resolved_point_obj.name,
                    'notes': rule_notes,
                    'sample_description': tx.description,
                    'sample_amount': str(tx.amount),
                }
                unmatched_item.rule_proposal_json = proposal_data
                unmatched_item.status = 'NEW_RULE_PROPOSED' # Change status for Superadmin filter
                messages.info(request, "Resolution saved! New rule proposed for Superadmin review.")
            else:
                messages.success(request, "Transaction resolved and filed.")

            unmatched_item.save()

            return redirect('review_queue')

    else:
        # Initial form rendering
        # CRITICAL: Initialize the form with the unmatched_id for hidden tracking
        form = ResolutionForm(initial={'unmatched_id': unmatched_id})

    context = {
        'unmatched_item': unmatched_item,
        'transaction': tx,
        'form': form,
    }
    return render(request, 'tax_processor/resolve_transaction.html', context)

@user_passes_test(is_permitted_user)
def tax_report(request, declaration_id):
    """
    Aggregates all matched/resolved transactions for a declaration.
    """

    # Permission Check: Ensure user has access to this declaration
    declaration_qs = filter_declarations_by_user(request.user)
    declaration = get_object_or_404(declaration_qs, pk=declaration_id)

    # CRITICAL AGGREGATION: Group transactions by declaration_point and sum the amounts
    report_data = Transaction.objects.filter(
        statement__declaration=declaration,
        declaration_point__isnull=False # Only include transactions that are assigned/resolved
    ).values('declaration_point__name', 'declaration_point__is_income').annotate(
        total_amount=Sum('amount'),
        transaction_count=Count('pk')
    ).order_by('declaration_point__is_income', 'declaration_point__name')

    context = {
        'declaration': declaration,
        'report_data': report_data,
        # Calculate total sum of all reported amounts (for reconciliation)
        'total_sum_all': sum(item['total_amount'] for item in report_data),
    }
    return render(request, 'tax_processor/tax_report.html', context)

@user_passes_test(is_superadmin)
def review_proposals(request):
    """
    Superadmin view to list all transactions with a NEW_RULE_PROPOSED status.
    """
    proposals = UnmatchedTransaction.objects.filter(
        status='NEW_RULE_PROPOSED'
    ).select_related(
        'transaction__statement__declaration',
        'assigned_user'
    ).order_by('-resolution_date')

    context = {
        'proposals': proposals,
        'title': 'New Rule Proposals Awaiting Review'
    }
    return render(request, 'tax_consumer/review_proposals.html', context)

@user_passes_test(is_superadmin)
def finalize_rule(request, unmatched_id):
    """
    Allows Superadmin to finalize a proposed rule by creating a TaxRule instance.
    """
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)

    if unmatched_item.status != 'NEW_RULE_PROPOSED':
        messages.error(request, "This item is not a pending rule proposal.")
        return redirect('review_proposals')

    proposal_data = unmatched_item.rule_proposal_json

    # 1. Start with the boilerplate description provided by the user's notes
    sample_desc = proposal_data.get('sample_description', 'No description provided').replace('"', '\\"')

    # 1. Build the structured rule logic dynamically
    structured_conditions = [
        {"logic": "AND", "checks": [
            {"field": "description", "type": "CONTAINS_KEYWORD", "value": f"{sample_desc}"}
        ]}
    ]

    # 3. Format the JSON string for the Textarea widget (using indent=2 and avoiding escapes)
    formatted_json = json.dumps(structured_conditions, indent=2, ensure_ascii=False)

    # Pre-populate the TaxRuleForm with data from the user's resolution
    initial_data = {
        'rule_name': f"AUTO_RULE: {unmatched_item.pk} - {proposal_data.get('resolved_point_name', 'Unnamed')}",
        'declaration_point': proposal_data.get('resolved_point_id'),
        'priority': 50, # Set a medium priority default
        'is_active': True,
        'conditions_json': formatted_json,
    }

    if request.method == 'POST':
        # Use the TaxRuleForm to validate and save the new rule
        # Note: This part will also need to be updated if the finalize_rule
        # view is to *also* use the new dynamic formset.
        # For now, it still assumes the old TaxRuleForm with JSON textarea.

        # --- IMPORTANT ---
        # The code below STILL assumes TaxRuleForm takes 'conditions_json'
        # If we want finalize_rule to use the new dynamic formset, this
        # view must be updated just like rule_create_or_update was.

        # --- TEMPORARY WORKAROUND (assuming old form for finalize_rule) ---
        # We need to temporarily re-create a simple form for this view
        # This is complex. Let's update this view properly.

        # --- PROPER UPDATE FOR finalize_rule ---

        # We can't use TaxRuleForm here easily because it no longer has conditions_json
        # The *best* approach is to re-use the rule_create_or_update view

        # --- SIMPLEST FIX: Re-create TaxRule with the dynamic formset ---

        form = TaxRuleForm(request.POST, instance=None) # Create a NEW rule
        formset = BaseConditionFormSet(request.POST, prefix='conditions') # Use the same prefix

        if form.is_valid() and formset.is_valid():
            with db_transaction.atomic():
                # 1. Create the new permanent TaxRule
                new_rule = form.save(commit=False)
                new_rule.created_by = request.user

                # --- SERIALIZE FORMSET DATA TO JSON ---
                checks = []
                for check_form in formset.cleaned_data:
                    if check_form and not check_form.get('DELETE'):
                        checks.append({
                            'field': check_form['field'],
                            'type': check_form['condition_type'],
                            'value': check_form['value']
                        })

                new_rule.conditions_json = [{
                    'logic': form.cleaned_data['logic'],
                    'checks': checks
                }]
                # --- END SERIALIZATION ---

                new_rule.save()

                # 2. Update the UnmatchedTransaction status to indicate completion
                unmatched_item.status = 'RESOLVED'
                unmatched_item.save()

            messages.success(request, f"New Rule '{new_rule.rule_name}' created and proposal cleared.")
            return redirect('review_proposals')

    else:
        # --- DESERIALIZE for GET request ---
        # We are pre-filling the formset from the proposal data

        # 1. Build the initial data for the main form
        initial_form_data = {
            'rule_name': f"AUTO_RULE: {unmatched_item.pk} - {proposal_data.get('resolved_point_name', 'Unnamed')}",
            'declaration_point': proposal_data.get('resolved_point_id'),
            'priority': 50, # Set a medium priority default
            'is_active': True,
            'logic': 'AND' # Default to AND
        }

        # 2. Build the initial data for the formset
        initial_formset_data = [{
            'field': 'description',
            'condition_type': 'CONTAINS_KEYWORD',
            'value': proposal_data.get('sample_description', '')
        }]

        form = TaxRuleForm(initial=initial_form_data)
        formset = BaseConditionFormSet(initial=initial_formset_data, prefix='conditions')
        # --- END DESERIALIZATION ---

    context = {
        'form': form,
        'formset': formset, # Pass the formset
        'unmatched_item': unmatched_item,
        'title': f"Finalize Rule Proposal #{unmatched_id}",
        'proposal_notes': proposal_data.get('notes')
    }
    return render(request, 'tax_processor/finalize_rule.html', context)
