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
from .models import Declaration, Transaction, TaxRule, UnmatchedTransaction, UserProfile, DeclarationPoint # Added DeclarationPoint
from datetime import date
import json
from django.db import transaction as db_transaction
from django.views.decorators.http import require_POST
from django.urls import reverse # Import reverse

# -----------------------------------------------------------
# 1. PERMISSION HELPERS (Unchanged)
# -----------------------------------------------------------
def is_superadmin(user):
    return user.is_authenticated and hasattr(user, 'profile') and user.profile.role == 'SUPERADMIN'

def is_permitted_user(user):
    return user.is_authenticated and hasattr(user, 'profile') and user.profile.role in ['SUPERADMIN', 'REGULAR_USER']

# -----------------------------------------------------------
# 2. DATA INGESTION & DECLARATION MGMT (Unchanged)
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
def upload_statement(request):
    # ... (Keep existing view) ...
    if request.method == 'POST':
        form = StatementUploadForm(request.POST, request.FILES)
        if form.is_valid():
            client_name = form.cleaned_data['client_name']; year = form.cleaned_data['year']; uploaded_files = request.FILES.getlist('statement_files')
            user_client_name = client_name; period_start = date(year, 1, 1); period_end = date(year + 1, 1, 31) # Adjusted period_end slightly
            declaration_name = f"{year} Հայտարարագիր - {client_name}"
            try:
                declaration_obj, created = Declaration.objects.get_or_create(name=declaration_name, defaults={'tax_period_start': period_start, 'tax_period_end': period_end, 'client_reference': client_name, 'status': 'DRAFT', 'created_by': request.user,})
                if created: messages.info(request, f"New Declaration '{declaration_name}' created automatically.")
            except IntegrityError: messages.error(request, "DB error occurred finding/creating Declaration."); return render(request, 'tax_processor/upload_statement.html', {'form': form})
            total_imported = 0
            if uploaded_files:
                for uploaded_file in uploaded_files:
                    count, message = import_statement_service(uploaded_file=uploaded_file, declaration_obj=declaration_obj, user=request.user)
                    if count > 0: total_imported += count; messages.success(request, f"File {uploaded_file.name}: {message}")
                    else: messages.error(request, f"File {uploaded_file.name}: {message}")
                if total_imported > 0: messages.success(request, f"Batch import complete. Total {total_imported} transactions saved to '{declaration_name}'."); return redirect('upload_statement')
                else: messages.error(request, "Batch import failed. No transactions processed.")
            else: messages.warning(request, "No files selected.")
    else: form = StatementUploadForm()
    return render(request, 'tax_processor/upload_statement.html', {'form': form})


def filter_declarations_by_user(user):
    # ... (Keep existing helper) ...
    if is_superadmin(user): return Declaration.objects.all()
    else: return Declaration.objects.filter(created_by=user)

@user_passes_test(is_permitted_user)
def declaration_detail(request, declaration_id):
    # ... (Keep existing view) ...
    declaration_qs = filter_declarations_by_user(request.user); declaration = get_object_or_404(declaration_qs, pk=declaration_id)
    total_statements = declaration.statements.count(); total_transactions = Transaction.objects.filter(statement__declaration=declaration).count(); unassigned_transactions = Transaction.objects.filter(statement__declaration=declaration, declaration_point__isnull=True).count()
    context = {'declaration': declaration, 'total_statements': total_statements, 'total_transactions': total_transactions, 'unassigned_transactions': unassigned_transactions, 'is_superadmin': is_superadmin(request.user)}
    return render(request, 'tax_processor/declaration_detail.html', context)


@user_passes_test(is_permitted_user)
@require_POST # Ensure this is only called via POST
def run_declaration_analysis(request, declaration_id):
    # ... (Keep existing view, ensure @require_POST is added) ...
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    # --- Permission Check: Allow creator or superadmin ---
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "You do not have permission to analyze this declaration.")
        return redirect('user_dashboard')
    # --- End Check ---
    assigned_user = request.user
    engine = RulesEngine(declaration_id=declaration.pk); matched, new_unmatched, cleared_unmatched = engine.run_analysis(assigned_user=assigned_user)
    messages.success(request, f"Analysis complete for '{declaration.name}'.")
    total_processed = matched + new_unmatched + cleared_unmatched
    messages.info(request, f"Total transactions processed: {total_processed}")
    messages.info(request, f"Matched {matched} new/re-evaluated transactions. Cleared {cleared_unmatched} existing review items.")
    messages.info(request, f"Found {new_unmatched} new transactions requiring manual review.")
    return redirect('declaration_detail', declaration_id=declaration.pk)


# -----------------------------------------------------------
# 3. GLOBAL RULE MANAGEMENT (Superadmin Only) - VIEW NAMES UPDATED
# -----------------------------------------------------------

@user_passes_test(is_superadmin)
def rule_list_global(request): # Renamed function
    """Displays a list of all GLOBAL Tax Rules."""
    rules = TaxRule.objects.filter(declaration__isnull=True) # Filter for global rules
    context = {'rules': rules, 'is_global_list': True, 'list_title': "Global Tax Rules"}
    # Use rule_list.html template, pass context to differentiate
    return render(request, 'tax_processor/rule_list.html', context)


@user_passes_test(is_superadmin)
def rule_create_or_update(request, rule_id=None, declaration_id=None): # Added declaration_id
    """
    Handles creating/updating GLOBAL or SPECIFIC rules based on context.
    - If declaration_id is provided, it's a specific rule.
    - Otherwise, it's a global rule (Superadmin only).
    """
    is_specific_rule = declaration_id is not None
    declaration = None
    rule = None
    title = ""

    # --- Determine context: Global vs Specific ---
    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        # Permission check: User must own the declaration OR be superadmin
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "You don't have permission to manage rules for this declaration.")
             return redirect('user_dashboard')
        if rule_id:
             rule = get_object_or_404(TaxRule, pk=rule_id, declaration=declaration) # Ensure rule belongs to this declaration
             title = f"Update Specific Rule: {rule.rule_name}"
        else:
             title = f"Create New Rule for {declaration.name}"
        list_url_name = 'declaration_rule_list' # URL to redirect back to
        url_kwargs = {'declaration_id': declaration_id}
    else: # Global rule
        if not is_superadmin(request.user): # Only superadmins for global rules
            messages.error(request, "You need superadmin rights to manage global rules.")
            return redirect('user_dashboard')
        if rule_id:
            rule = get_object_or_404(TaxRule, pk=rule_id, declaration__isnull=True) # Ensure it's global
            title = f"Update Global Rule: {rule.rule_name}"
        else:
            title = "Create New Global Tax Rule"
        list_url_name = 'rule_list_global'
        url_kwargs = {}


    formset_prefix = 'conditions'

    if request.method == 'POST':
        form = TaxRuleForm(request.POST, instance=rule)
        formset = BaseConditionFormSet(request.POST, prefix=formset_prefix)

        if form.is_valid() and formset.is_valid():
            new_rule = form.save(commit=False)
            if not new_rule.pk: new_rule.created_by = request.user

            # --- Link to Declaration if specific ---
            if is_specific_rule:
                new_rule.declaration = declaration
            else:
                 new_rule.declaration = None # Ensure it's global

            # --- Serialize formset (unchanged) ---
            checks = []
            for check_form in formset.cleaned_data:
                if check_form and not check_form.get('DELETE'): checks.append({'field': check_form['field'], 'type': check_form['condition_type'], 'value': check_form['value']})
            new_rule.conditions_json = [{'logic': form.cleaned_data['logic'], 'checks': checks}]
            # --- End Serialization ---

            # --- Reset proposal status if edited ---
            # If a superadmin edits a proposed global rule, maybe reset status? Or handle in approve view?
            # For simplicity, we'll reset it here if it's being edited.
            if rule and rule.proposal_status == 'PENDING_GLOBAL' and not is_superadmin(request.user):
                 # Non-admin edited their own proposed rule, reset status
                 new_rule.proposal_status = 'NONE'
                 messages.info(request, "Rule edited, global proposal status reset.")
            elif rule and rule.proposal_status == 'PENDING_GLOBAL' and is_superadmin(request.user):
                 # Admin editing a proposed rule - Keep status PENDING unless approved/rejected elsewhere
                 pass


            try:
                 new_rule.save()
                 messages.success(request, f"Tax Rule '{new_rule.rule_name}' saved successfully.")
                 return redirect(list_url_name, **url_kwargs)
            except IntegrityError:
                 # Catch unique_together violation
                 messages.error(request, f"A rule named '{new_rule.rule_name}' already exists for this scope (global or specific declaration). Please choose a different name.")
                 # Re-render form with error
                 context = {'form': form, 'formset': formset, 'title': title, 'rule': rule, 'declaration': declaration, 'is_specific_rule': is_specific_rule, 'list_url_name': list_url_name, 'url_kwargs': url_kwargs}
                 return render(request, 'tax_processor/rule_form.html', context)

    else: # GET request
        # --- Deserialize JSON to Formset (unchanged logic) ---
        initial_form_data = {}; initial_formset_data = []
        if rule:
            if rule.conditions_json and isinstance(rule.conditions_json, list) and len(rule.conditions_json) > 0 and rule.conditions_json[0]:
                logic_block = rule.conditions_json[0]; initial_form_data['logic'] = logic_block.get('logic', 'AND')
                for check in logic_block.get('checks', []): initial_formset_data.append({'field': check.get('field'), 'condition_type': check.get('type'), 'value': check.get('value')})
        else: initial_form_data['logic'] = 'AND'
        form = TaxRuleForm(instance=rule, initial=initial_form_data)
        formset = BaseConditionFormSet(initial=initial_formset_data, prefix=formset_prefix)
        # --- End Deserialization ---

    context = {
        'form': form, 'formset': formset, 'title': title, 'rule': rule,
        'declaration': declaration, # Pass declaration if specific
        'is_specific_rule': is_specific_rule,
        'list_url_name': list_url_name, # Pass redirect URL info
        'url_kwargs': url_kwargs,
    }
    return render(request, 'tax_processor/rule_form.html', context)


@user_passes_test(is_permitted_user) # Allow regular users or superadmin
@require_POST # Ensure POST for deletion
def rule_delete(request, rule_id, declaration_id=None): # Added declaration_id
    """Deletes a global or specific rule based on context."""
    is_specific_rule = declaration_id is not None
    rule = None

    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(TaxRule, pk=rule_id, declaration=declaration)
        list_url_name = 'declaration_rule_list'; url_kwargs = {'declaration_id': declaration_id}
    else: # Global rule
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(TaxRule, pk=rule_id, declaration__isnull=True)
        list_url_name = 'rule_list_global'; url_kwargs = {}

    rule_name = rule.rule_name # Store name before deletion
    rule.delete()
    messages.success(request, f"Tax Rule '{rule_name}' successfully deleted.")
    return redirect(list_url_name, **url_kwargs)


# -----------------------------------------------------------
# 4. DECLARATION-SPECIFIC RULE LIST VIEW (NEW)
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
def declaration_rule_list(request, declaration_id):
    """Displays rules specific to a single declaration."""
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    # Permission check
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "You don't have permission to view rules for this declaration.")
        return redirect('user_dashboard')

    rules = TaxRule.objects.filter(declaration=declaration)
    context = {
        'rules': rules,
        'declaration': declaration,
        'is_global_list': False, # Flag for template
        'list_title': f"Rules for {declaration.name}"
    }
    # Re-use the rule_list.html template
    return render(request, 'tax_processor/rule_list.html', context)


# -----------------------------------------------------------
# 5. GLOBAL RULE PROPOSAL WORKFLOW (NEW VIEWS)
# -----------------------------------------------------------
@user_passes_test(is_permitted_user) # Regular user can propose their own rules
@require_POST
def propose_rule_global(request, rule_id):
    """Marks a declaration-specific rule as 'PENDING_GLOBAL'."""
    # Find the rule, ensuring it's specific and owned by the user (or user is admin)
    rule_query = Q(pk=rule_id) & Q(declaration__isnull=False)
    if not is_superadmin(request.user):
        rule_query &= Q(declaration__created_by=request.user) # User must own declaration

    rule = get_object_or_404(TaxRule, rule_query)

    if rule.proposal_status == 'NONE':
        rule.proposal_status = 'PENDING_GLOBAL'
        rule.save()
        messages.success(request, f"Rule '{rule.rule_name}' proposed for global use. A superadmin will review it.")
    else:
        messages.warning(request, f"Rule '{rule.rule_name}' has already been proposed or processed.")

    # Redirect back to the declaration-specific rule list
    return redirect('declaration_rule_list', declaration_id=rule.declaration.pk)


@user_passes_test(is_superadmin)
def review_global_proposals(request):
    """Superadmin view to list rules pending global approval."""
    proposals = TaxRule.objects.filter(proposal_status='PENDING_GLOBAL').select_related('declaration', 'declaration_point', 'created_by')
    context = {
        'proposals': proposals,
        'title': "Review Proposed Global Rules"
    }
    return render(request, 'tax_processor/review_global_proposals.html', context)


@user_passes_test(is_superadmin)
@require_POST
def approve_global_proposal(request, rule_id):
    """Superadmin approves a proposal: makes the rule global."""
    rule = get_object_or_404(TaxRule, pk=rule_id, proposal_status='PENDING_GLOBAL')
    original_decl_id = rule.declaration_id # Store for message

    # Check for name conflict with existing global rules before making it global
    new_name = rule.rule_name # Or maybe prompt admin to rename if needed?
    if TaxRule.objects.filter(declaration__isnull=True, rule_name=new_name).exists():
        messages.error(request, f"Cannot approve rule '{new_name}'. A global rule with this name already exists. Please edit the name before approving or reject the proposal.")
        return redirect('review_global_proposals')

    # Convert to global rule
    rule.declaration = None
    rule.proposal_status = 'NONE' # Reset status after approval
    # rule.proposal_status = 'APPROVED_GLOBAL' # Or mark as approved
    rule.save()

    messages.success(request, f"Rule '{rule.rule_name}' (from Declaration {original_decl_id}) approved and converted to a global rule.")
    return redirect('review_global_proposals')


@user_passes_test(is_superadmin)
@require_POST
def reject_global_proposal(request, rule_id):
    """Superadmin rejects a proposal: resets status, keeps it specific."""
    rule = get_object_or_404(TaxRule, pk=rule_id, proposal_status='PENDING_GLOBAL')

    rule.proposal_status = 'NONE' # Reset status
    # rule.proposal_status = 'REJECTED_GLOBAL' # Or mark as rejected
    rule.save()

    messages.warning(request, f"Proposal for rule '{rule.rule_name}' rejected. It remains a specific rule for Declaration {rule.declaration_id}.")
    return redirect('review_global_proposals')


# -----------------------------------------------------------
# 6. USER DASHBOARD & REVIEW QUEUES (Minor updates may be needed later)
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
def user_dashboard(request):
    # ... (Keep existing view - might add count of pending global proposals later) ...
    user = request.user
    declarations_qs = filter_declarations_by_user(user)
    declarations = declarations_qs.annotate(statement_count=Count('statements', distinct=True), total_transactions=Count('statements__transactions', distinct=True), unmatched_count=Count('statements__transactions__unmatched_record', filter=Q(statements__transactions__unmatched_record__status='PENDING_REVIEW'), distinct=True)).order_by('-tax_period_start')
    pending_proposals_count = 0; pending_global_rules_count = 0 # NEW
    if is_superadmin(user):
        pending_proposals_count = UnmatchedTransaction.objects.filter(status='NEW_RULE_PROPOSED').count()
        pending_global_rules_count = TaxRule.objects.filter(proposal_status='PENDING_GLOBAL').count() # NEW

    context = {'declarations': declarations, 'is_admin': is_superadmin(user), 'pending_proposals_count': pending_proposals_count, 'pending_global_rules_count': pending_global_rules_count} # NEW count added
    return render(request, 'tax_processor/user_dashboard.html', context)


@user_passes_test(is_permitted_user)
def review_queue(request, declaration_id=None):
    # ... (Keep existing view) ...
    user = request.user; unmatched_qs = UnmatchedTransaction.objects.filter(status='PENDING_REVIEW'); is_filtered_by_declaration = False
    if declaration_id:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(user) or declaration.created_by == user): messages.error(request, "Permission denied."); return redirect('user_dashboard')
        unmatched_qs = unmatched_qs.filter(transaction__statement__declaration_id=declaration_id); title = f"Review Queue - {declaration.name}"; is_filtered_by_declaration = True
    elif is_superadmin(user): title = "SUPERADMIN Review Queue (All Pending)"
    else: unmatched_qs = unmatched_qs.filter(assigned_user=user); title = f"{user.username}'s Pending Reviews"
    unmatched_items = unmatched_qs.select_related('transaction__statement__declaration', 'transaction__matched_rule', 'assigned_user').order_by('-transaction__transaction_date')
    context = {'title': title, 'unmatched_items': unmatched_items, 'is_admin': is_superadmin(user), 'is_filtered': is_filtered_by_declaration, 'current_declaration_id': declaration_id}
    return render(request, 'tax_processor/review_queue.html', context)


@user_passes_test(is_permitted_user)
def resolve_transaction(request, unmatched_id):
    """
    Displays the form for a user to resolve an unmatched transaction, updates
    the related records, and optionally creates a NEW declaration-specific rule.
    """
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)
    tx = unmatched_item.transaction
    declaration = tx.statement.declaration # Get the related declaration

    # --- Permission Check: Owner or Superadmin ---
    # User must own the declaration OR be a superadmin to resolve/create rules for it
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք լուծելու այս գործարքը։") # Not authorized to resolve
        # Decide where to redirect: dashboard or maybe declaration detail if accessible?
        return redirect('user_dashboard') # Redirecting to dashboard for safety

    if request.method == 'POST':
        form = ResolutionForm(request.POST)
        if form.is_valid():
            resolved_point_obj = form.cleaned_data['resolved_point']
            create_rule = form.cleaned_data['create_specific_rule']
            new_rule = None # Initialize new_rule

            with db_transaction.atomic(): # Use atomic transaction for multi-step save
                # 1. Update the original Transaction record
                tx.declaration_point = resolved_point_obj
                # tx.matched_rule = None # Clear any old rule match if re-resolving? Optional.

                # 2. Handle Rule Creation (if requested)
                if create_rule:
                    rule_name = form.cleaned_data.get('rule_name')
                    priority = form.cleaned_data.get('priority')

                    # --- Basic Validation ---
                    if not rule_name:
                         form.add_error('rule_name', 'Կանոնի անվանումը պարտադիր է։') # Rule name required
                    if priority is None:
                         form.add_error('priority', 'Առաջնահերթությունը պարտադիր է։') # Priority required

                    if form.is_valid(): # Re-check form validity after adding potential errors
                        # --- Auto-generate simple condition based on description ---
                        condition_value = tx.description[:200] # Use first 200 chars as keyword
                        conditions = [{
                            'logic': 'AND',
                            'checks': [{
                                'field': 'description',
                                'type': 'CONTAINS_KEYWORD', # Simple default condition
                                'value': condition_value
                            }]
                        }]

                        try:
                            new_rule = TaxRule.objects.create(
                                rule_name=rule_name,
                                priority=priority,
                                declaration_point=resolved_point_obj,
                                conditions_json=conditions,
                                is_active=True,
                                created_by=request.user,
                                declaration=declaration, # Link to the specific declaration
                                proposal_status='NONE' # Default status
                            )
                            tx.matched_rule = new_rule # Link transaction to the new rule
                            messages.success(request, f"Գործարքը լուծված է։ Նոր հատուկ կանոն '{rule_name}' ստեղծված է։") # Tx resolved. New specific rule created.
                        except IntegrityError:
                             # Handle case where rule name already exists for this declaration
                             form.add_error('rule_name', f"Այս անունով կանոն արդեն գոյություն ունի այս հայտարարագրի համար։") # Rule with this name already exists...
                             # --- Need to re-render the form with error ---
                             context = {'unmatched_item': unmatched_item, 'transaction': tx, 'form': form,}
                             return render(request, 'tax_processor/resolve_transaction.html', context)
                    else:
                         # --- Form became invalid (missing name/priority), re-render ---
                         context = {'unmatched_item': unmatched_item, 'transaction': tx, 'form': form,}
                         return render(request, 'tax_processor/resolve_transaction.html', context)

                else: # Not creating a rule
                    messages.success(request, "Գործարքը լուծված է։") # Transaction resolved.

                # 3. Save the transaction (with updated point and possibly matched_rule)
                tx.save()

                # 4. Update the UnmatchedTransaction status
                unmatched_item.resolved_point = resolved_point_obj.name # Store name for audit
                unmatched_item.resolution_date = timezone.now()
                unmatched_item.status = 'RESOLVED'
                # Clear any old manual proposal data if it existed
                unmatched_item.rule_proposal_json = None
                unmatched_item.save()

            # Redirect after successful save (either with or without rule creation)
            # Redirect to the declaration-specific review queue if user came from there?
            # Or just the global one? Let's try redirecting back to declaration detail.
            return redirect('declaration_detail', declaration_id=declaration.pk)
            # return redirect('review_queue') # Or redirect to global queue

    else: # GET request
        # Pre-fill rule name suggestion based on description?
        initial_rule_name = f"Rule based on: {tx.description[:50]}..."
        form = ResolutionForm(initial={'unmatched_id': unmatched_id, 'rule_name': initial_rule_name})

    context = {
        'unmatched_item': unmatched_item,
        'transaction': tx,
        'form': form,
    }
    return render(request, 'tax_processor/resolve_transaction.html', context)


@user_passes_test(is_permitted_user)
def tax_report(request, declaration_id):
    # ... (Keep existing view) ...
    declaration_qs = filter_declarations_by_user(request.user); declaration = get_object_or_404(declaration_qs, pk=declaration_id)
    transactions_qs = Transaction.objects.filter(statement__declaration=declaration, declaration_point__isnull=False)
    report_data = transactions_qs.values('declaration_point__name', 'declaration_point__is_income','currency').annotate(total_amount=Sum('amount'), transaction_count=Count('pk')).order_by('declaration_point__is_income', 'declaration_point__name','currency')
    currency_totals = transactions_qs.values('currency').annotate(total_amount=Sum('amount')).order_by('currency')
    context = {'declaration': declaration, 'report_data': report_data, 'currency_totals': currency_totals,}
    return render(request, 'tax_processor/tax_report.html', context)


# -----------------------------------------------------------
# 7. SUPERADMIN MANUAL PROPOSAL WORKFLOW (Existing - View names maybe adjusted)
# -----------------------------------------------------------
@user_passes_test(is_superadmin)
def review_proposals(request): # List manual proposals from unmatched tx
    # ... (Keep existing view) ...
    proposals = UnmatchedTransaction.objects.filter(status='NEW_RULE_PROPOSED').select_related('transaction__statement__declaration', 'assigned_user').order_by('-resolution_date')
    context = {'proposals': proposals, 'title': 'New Manual Rule Proposals Awaiting Review'}
    return render(request, 'tax_processor/review_proposals.html', context)


@user_passes_test(is_superadmin)
def finalize_rule(request, unmatched_id): # Create rule from manual proposal
    # ... (Keep existing view) ...
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id); transaction = unmatched_item.transaction
    if unmatched_item.status != 'NEW_RULE_PROPOSED': messages.error(request, "Not a pending proposal."); return redirect('review_proposals')
    proposal_data = unmatched_item.rule_proposal_json; proposed_category_name = proposal_data.get('resolved_point_name', 'N/A')
    if request.method == 'POST':
        form = TaxRuleForm(request.POST, instance=None); formset = BaseConditionFormSet(request.POST, prefix='conditions')
        if form.is_valid() and formset.is_valid():
            with db_transaction.atomic():
                new_rule = form.save(commit=False); new_rule.created_by = request.user; new_rule.declaration = None # Ensure it's global
                checks = [];
                for check_form in formset.cleaned_data:
                     if check_form and not check_form.get('DELETE'): checks.append({'field': check_form['field'], 'type': check_form['condition_type'], 'value': check_form['value']})
                new_rule.conditions_json = [{'logic': form.cleaned_data['logic'], 'checks': checks}]
                try: # Add try-except for potential IntegrityError on save
                    new_rule.save()
                    unmatched_item.status = 'RESOLVED'; unmatched_item.save()
                    messages.success(request, f"New Global Rule '{new_rule.rule_name}' created from proposal.")
                    return redirect('review_proposals')
                except IntegrityError:
                     messages.error(request, f"Cannot save rule '{new_rule.rule_name}'. A global rule with this name already exists.")
                     # Re-render form with error
                     context = {'form': form, 'formset': formset, 'unmatched_item': unmatched_item, 'transaction': transaction, 'title': f"Finalize Rule Proposal #{unmatched_id}", 'proposal_notes': proposal_data.get('notes'), 'proposed_category_name': proposed_category_name}
                     return render(request, 'tax_processor/finalize_rule.html', context)
        else: # Form invalid, re-render
             context = {'form': form, 'formset': formset, 'unmatched_item': unmatched_item, 'transaction': transaction, 'title': f"Finalize Rule Proposal #{unmatched_id}", 'proposal_notes': proposal_data.get('notes'), 'proposed_category_name': proposed_category_name}
             return render(request, 'tax_processor/finalize_rule.html', context)
    else: # GET
        initial_form_data = {'rule_name': f"AUTO_RULE: {unmatched_item.pk} - {proposed_category_name}", 'declaration_point': proposal_data.get('resolved_point_id'), 'priority': 50, 'is_active': True, 'logic': 'AND'}
        initial_formset_data = [{'field': 'description', 'condition_type': 'CONTAINS_KEYWORD', 'value': proposal_data.get('sample_description', '')}]
        form = TaxRuleForm(initial=initial_form_data); formset = BaseConditionFormSet(initial=initial_formset_data, prefix='conditions')
        context = {'form': form, 'formset': formset, 'unmatched_item': unmatched_item, 'transaction': transaction, 'title': f"Finalize Rule Proposal #{unmatched_id}", 'proposal_notes': proposal_data.get('notes'), 'proposed_category_name': proposed_category_name}
        return render(request, 'tax_processor/finalize_rule.html', context)


@require_POST
@user_passes_test(is_superadmin)
def reject_proposal(request, unmatched_id): # Reject manual proposal
    # ... (Keep existing view) ...
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)
    if unmatched_item.status == 'NEW_RULE_PROPOSED':
        unmatched_item.status = 'PENDING_REVIEW'; unmatched_item.rule_proposal_json = None; unmatched_item.save()
        messages.warning(request, f"Manual proposal #{unmatched_id} rejected. Transaction returned to 'Pending Review'.")
    else: messages.error(request, "Proposal could not be rejected.")
    return redirect('review_proposals')
