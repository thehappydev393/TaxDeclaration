# tax_processor/views.py

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib import messages
from django.db import IntegrityError
from django.db.models import Q, Count, Sum
from django.utils import timezone
from .forms import StatementUploadForm, TaxRuleForm, ResolutionForm, BaseConditionFormSet, AddStatementsForm, TransactionEditForm
from .services import import_statement_service
from .rules_engine import RulesEngine
from .models import Declaration, Transaction, TaxRule, UnmatchedTransaction, UserProfile, DeclarationPoint
from .parser_logic import BANK_KEYWORDS # Import the dictionary
from datetime import date
import json
from django.db import transaction as db_transaction
from django.views.decorators.http import require_POST
from django.urls import reverse
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger # Import Paginator

BANK_NAMES_LIST = sorted(list(BANK_KEYWORDS.keys()))

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
    """
    Handles initial file upload for a NEW Declaration based on Client Name and Year.
    Prevents adding statements to existing declarations via this page.
    """
    if request.method == 'POST':
        form = StatementUploadForm(request.POST, request.FILES)
        if form.is_valid():
            client_name = form.cleaned_data['client_name']
            year = form.cleaned_data['year']
            uploaded_files = request.FILES.getlist('statement_files')

            # --- DECLARATION CREATION LOGIC ---
            period_start = date(year, 1, 1)
            period_end = date(year, 12, 31) # Standard end of year
            declaration_name = f"{year} Հայտարարագիր - {client_name}" # Declaration

            # --- MODIFIED: Try to CREATE only ---
            try:
                declaration_obj = Declaration.objects.create(
                    name=declaration_name,
                    tax_period_start=period_start,
                    tax_period_end=period_end,
                    client_reference=client_name,
                    status='DRAFT',
                    created_by=request.user,
                )
                messages.success(request, f"Նոր Հայտարարագիր '{declaration_name}' ստեղծված է։") # New Declaration created.

            except IntegrityError:
                # Declaration with this name already exists
                messages.error(
                    request,
                    f"'{declaration_name}' անունով Հայտարարագիր արդեն գոյություն ունի։ " # Declaration with name ... already exists.
                    f"Լրացուցիչ քաղվածքներ ավելացնելու համար խնդրում ենք գնալ համապատասխան " # To add more statements, please go to the relevant
                    f"<a href='{reverse('user_dashboard')}'>Հայտարարագրի մանրամասների էջ</a>։" # Declaration details page (linking dashboard for now)
                )
                # Re-render the form with the error
                return render(request, 'tax_processor/upload_statement.html', {'form': form})
            # --- END MODIFICATION ---


            # --- File Processing Logic (Unchanged) ---
            total_imported = 0
            if uploaded_files:
                for uploaded_file in uploaded_files:
                    count, message = import_statement_service(uploaded_file=uploaded_file, declaration_obj=declaration_obj, user=request.user)
                    if count > 0: total_imported += count; messages.success(request, f"Ֆայլ {uploaded_file.name}: {message}")
                    else: messages.error(request, f"Ֆայլ {uploaded_file.name}: {message}")

                if total_imported > 0:
                    messages.success(request, f"Բեռնումն ավարտված է։ Ընդհանուր {total_imported} գործարք մշակվել և պահպանվել է '{declaration_name}'-ում։") # Upload complete. Total ... transactions processed and saved...
                    # Redirect to the NEW declaration's detail page after successful creation and upload
                    return redirect('declaration_detail', declaration_id=declaration_obj.pk)
                else:
                    messages.warning(request, "Ֆայլերը վերբեռնվեցին, բայց գործարքներ չեն մշակվել։ Հայտարարագիրը ստեղծված է։") # Files uploaded, but no transactions processed. Declaration created.
                    return redirect('declaration_detail', declaration_id=declaration_obj.pk) # Still redirect
            else:
                messages.warning(request, "Ֆայլեր ընտրված չեն։ Հայտարարագիրը ստեղծված է։") # No files selected. Declaration created.
                return redirect('declaration_detail', declaration_id=declaration_obj.pk) # Still redirect

    else: # GET request
        form = StatementUploadForm()

    return render(request, 'tax_processor/upload_statement.html', {'form': form})

@user_passes_test(is_permitted_user)
def add_statements_to_declaration(request, declaration_id):
    """
    Handles uploading additional statement files to an EXISTING Declaration.
    """
    declaration = get_object_or_404(Declaration, pk=declaration_id)

    # Permission Check: User must own the declaration OR be superadmin
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք այս Հայտարարագրին քաղվածքներ ավելացնելու։") # Not authorized to add statements...
        return redirect('user_dashboard')

    if request.method == 'POST':
        form = AddStatementsForm(request.POST, request.FILES)
        if form.is_valid():
            uploaded_files = request.FILES.getlist('statement_files')
            total_imported = 0
            files_processed = 0

            if uploaded_files:
                for uploaded_file in uploaded_files:
                    files_processed += 1
                    count, message = import_statement_service(
                        uploaded_file=uploaded_file,
                        declaration_obj=declaration, # Use the existing declaration
                        user=request.user
                    )
                    if count > 0:
                        total_imported += count
                        messages.success(request, f"Ֆայլ {uploaded_file.name}: {message}")
                    else:
                        messages.error(request, f"Ֆայլ {uploaded_file.name}: {message}")

                if total_imported > 0:
                    messages.success(request, f"Բեռնումն ավարտված է։ Ընդհանուր {total_imported} նոր գործարք ավելացվել է '{declaration.name}'-ին։") # Upload complete. Total ... new transactions added...
                elif files_processed > 0:
                     messages.warning(request, "Ֆայլ(եր) մշակվեցին, բայց նոր գործարքներ չավելացվեցին։") # File(s) processed, but no new transactions added.
                else: # Should not happen if form requires files, but as safeguard
                     messages.error(request, "Վերբեռնման ընթացքում սխալ տեղի ունեցավ։") # Error during upload.

                # Redirect back to declaration detail after processing
                return redirect('declaration_detail', declaration_id=declaration.pk)
            else:
                 # Should be caught by form validation, but handle just in case
                 messages.warning(request, "Ֆայլեր ընտրված չեն։") # No files selected.
                 # Re-render form below

    else: # GET request
        form = AddStatementsForm()

    context = {
        'form': form,
        'declaration': declaration
    }
    return render(request, 'tax_processor/add_statements_form.html', context)


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

@user_passes_test(is_permitted_user)
@require_POST
def run_analysis_pending(request, declaration_id):
    """Triggers rule matching ONLY for NEW and PENDING_REVIEW transactions."""
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    # Permission Check
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Permission denied.")
        return redirect('user_dashboard')

    assigned_user = request.user
    engine = RulesEngine(declaration_id=declaration.pk)
    # Call the NEW engine method
    matched, new_unmatched, cleared_unmatched = engine.run_analysis_pending_only(assigned_user=assigned_user)

    messages.success(request, f"Վերլուծություն (Նոր և Սպասվող) ավարտվեց «{declaration.name}»-ի համար։") # Analysis (New & Pending) complete...
    total_processed = matched + new_unmatched; # Cleared is a subset of matched here
    messages.info(request, f"Ընդհանուր մշակված գործարքներ՝ {total_processed}")
    messages.info(request, f"Համընկել է {matched} նոր/սպասվող գործարք։") # Matched new/pending...
    if cleared_unmatched > 0:
        messages.info(request, f"Մաքրվել է {cleared_unmatched} գործարք 'Սպասում է Վերանայման' հերթից։") # Cleared ... from 'Pending Review' queue.
    messages.info(request, f"Հայտնաբերվել է {new_unmatched} նոր գործարք, որոնք պահանջում են ձեռքով վերանայում։") # Found ... new transactions requiring manual review.

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
        'bank_names': BANK_NAMES_LIST
    }
    return render(request, 'tax_processor/rule_form.html', context)

    if request.method == 'POST':
            form = TaxRuleForm(request.POST, instance=rule)
            formset = BaseConditionFormSet(request.POST, prefix=formset_prefix)

            if form.is_valid() and formset.is_valid():
                 # ... (save logic) ...
                 pass # Redirects on success
            else: # Form or formset invalid, prepare context for re-render
                 context = {
                    'form': form, 'formset': formset, 'title': title, 'rule': rule,
                    'declaration': declaration,
                    'is_specific_rule': is_specific_rule,
                    'list_url_name': list_url_name,
                    'url_kwargs': url_kwargs,
                    'bank_names': BANK_NAMES_LIST # <-- ADD THIS HERE TOO
                 }
                 return render(request, 'tax_processor/rule_form.html', context) # Re-render with errors AND bank names


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
    Handles resolving an unmatched transaction with options to:
    1. Just resolve.
    2. Resolve and create a new DECLARATION-SPECIFIC rule.
    3. Resolve and propose a new GLOBAL rule for admin review.
    """
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)
    tx = unmatched_item.transaction
    declaration = tx.statement.declaration

    # Permission Check: Owner or Superadmin
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք լուծելու այս գործարքը։")
        return redirect('user_dashboard')

    resolution_form_prefix = 'res'; rule_form_prefix = 'rule'; condition_formset_prefix = 'cond'

    # Initialize forms for GET request
    if request.method != 'POST':
        resolution_form = ResolutionForm(initial={'unmatched_id': unmatched_id}, prefix=resolution_form_prefix)
        suggested_rule_name = f"Rule based on: {tx.description[:50]}..."
        initial_rule_data = {'rule_name': suggested_rule_name, 'priority': 50, 'is_active': True, 'logic': 'AND'}
        # Pre-fill declaration point in rule form if a point is already selected in resolution form (though unlikely on GET)
        # initial_rule_data['declaration_point'] = resolution_form['resolved_point'].value() # Example if needed
        rule_form = TaxRuleForm(initial=initial_rule_data, prefix=rule_form_prefix)
        initial_condition_data = [{'field': 'description', 'condition_type': 'CONTAINS_KEYWORD', 'value': tx.description[:200]}]
        condition_formset = BaseConditionFormSet(initial=initial_condition_data, prefix=condition_formset_prefix)
    else: # POST request
        resolution_form = ResolutionForm(request.POST, prefix=resolution_form_prefix)
        rule_form = TaxRuleForm(request.POST, prefix=rule_form_prefix)
        condition_formset = BaseConditionFormSet(request.POST, prefix=condition_formset_prefix)

        # --- Step 1: Validate the Resolution Form FIRST ---
        if resolution_form.is_valid():
            resolved_point_obj = resolution_form.cleaned_data['resolved_point']
            action = resolution_form.cleaned_data['rule_action']
            forms_are_valid = True # Assume valid initially

            try:
                # --- Step 2: Conditionally Validate Rule Forms SERVER-SIDE ---
                if action == 'create_specific':
                    # Manually check required fields for the rule before calling is_valid
                    rule_name = request.POST.get(f'{rule_form_prefix}-rule_name', '').strip()
                    priority_str = request.POST.get(f'{rule_form_prefix}-priority', '').strip()
                    # Use resolved_point_obj from resolution_form for the rule's point
                    declaration_point_obj_for_rule = resolved_point_obj
                    logic = request.POST.get(f'{rule_form_prefix}-logic', '').strip()

                    if not rule_name:
                        rule_form.add_error('rule_name', 'Կանոնի անվանումը պարտադիր է։')
                        forms_are_valid = False
                    if not priority_str:
                         rule_form.add_error('priority', 'Առաջնահերթությունը պարտադիր է։')
                         forms_are_valid = False
                    # No need to check declaration_point_id from POST, use resolved_point_obj
                    if declaration_point_obj_for_rule is None: # Should not happen if resolution_form is valid
                         messages.error(request, "Հայտարարագրման կետը պարտադիր է կանոնի համար։") # Declaration point required for rule
                         forms_are_valid = False
                    if not logic: # Logic is part of rule_form now
                         rule_form.add_error('logic', 'Կանոնի տրամաբանությունը պարտադիր է։') # Logic is required
                         forms_are_valid = False

                    # Check condition formset validity
                    if not condition_formset.is_valid():
                         # Errors should be attached to the formset automatically
                         messages.error(request, "Խնդրում ենք ուղղել սխալները կանոնի պայմաններում։")
                         forms_are_valid = False
                    # Check if at least one *valid* (non-deleted) condition exists
                    elif not any(form and not form.get('DELETE', False) for form in condition_formset.cleaned_data):
                         messages.error(request, "Կանոն ստեղծելու համար պետք է ավելացնել առնվազն մեկ պայման։")
                         forms_are_valid = False

                    # Additionally run full validation on rule_form itself if manual checks passed
                    # This catches things like invalid priority number format etc.
                    # We need to manually set the declaration_point before validating rule_form
                    temp_data = rule_form.data.copy()
                    temp_data[f'{rule_form_prefix}-declaration_point'] = declaration_point_obj_for_rule.pk if declaration_point_obj_for_rule else ''
                    temp_rule_form_for_validation = TaxRuleForm(temp_data, prefix=rule_form_prefix)

                    if forms_are_valid and not temp_rule_form_for_validation.is_valid():
                         # Transfer errors from temp form to the actual form being rendered
                         rule_form._errors = temp_rule_form_for_validation.errors
                         forms_are_valid = False

                # --- Step 3: Save ONLY if necessary forms are valid ---
                if forms_are_valid:
                    with db_transaction.atomic():
                        tx.declaration_point = resolved_point_obj; tx.matched_rule = None
                        new_rule = None
                        if action == 'create_specific':
                            # Re-validate just to be safe, using the corrected declaration point
                            temp_data = rule_form.data.copy()
                            temp_data[f'{rule_form_prefix}-declaration_point'] = resolved_point_obj.pk
                            final_rule_form = TaxRuleForm(temp_data, prefix=rule_form_prefix)

                            if final_rule_form.is_valid() and condition_formset.is_valid():
                                new_rule = final_rule_form.save(commit=False)
                                new_rule.created_by = request.user
                                new_rule.declaration = declaration # Specific rule
                                new_rule.proposal_status = 'NONE'
                                checks = [{'field': f['field'], 'type': f['condition_type'], 'value': f['value']} for f in condition_formset.cleaned_data if f and not f.get('DELETE')]
                                new_rule.conditions_json = [{'logic': final_rule_form.cleaned_data['logic'], 'checks': checks}]
                                new_rule.save()
                                tx.matched_rule = new_rule
                                messages.success(request, f"Գործարքը լուծված է։ Նոր հատուկ կանոն '{new_rule.rule_name}' ստեղծված է։")
                            else:
                                # This path indicates a logic error in validation steps above
                                print("ERROR: Rule form/formset invalid during save attempt.")
                                messages.error(request, "Internal validation error during rule save.")
                                raise ValueError("Rule form/formset invalid during save.") # Force rollback

                        elif action == 'propose_global':
                            rule_notes = resolution_form.cleaned_data.get('rule_notes', '')
                            proposal_data = {'resolved_point_id': resolved_point_obj.pk, 'resolved_point_name': resolved_point_obj.name, 'notes': rule_notes, 'sample_description': tx.description, 'sample_amount': str(tx.amount),}
                            unmatched_item.rule_proposal_json = proposal_data
                            unmatched_item.status = 'NEW_RULE_PROPOSED'
                            messages.info(request, "Գործարքը լուծված է։ Նոր գլոբալ կանոն առաջարկված է Superadmin-ի վերանայման համար։")
                        else: # resolve_only
                            messages.success(request, "Գործարքը լուծված է։")

                        unmatched_item.status = 'RESOLVED' if action != 'propose_global' else 'NEW_RULE_PROPOSED'
                        unmatched_item.resolved_point = resolved_point_obj.name
                        unmatched_item.resolution_date = timezone.now()
                        if action != 'propose_global': unmatched_item.rule_proposal_json = None
                        unmatched_item.save()
                        tx.save() # Save transaction changes

                    # Redirect only after successful atomic block
                    return redirect('declaration_detail', declaration_id=declaration.pk)

                else: # Forms were invalid
                     messages.error(request, "Խնդրում ենք ուղղել նշված սխալները։")
                     # Fall through to re-render

            except IntegrityError:
                 messages.error(request, f"Այս անունով կանոն արդեն գոյություն ունի այս հայտարարագրի համար։ Խնդրում ենք ընտրել այլ անուն։")
                 # Fall through to re-render

            except Exception as e:
                 messages.error(request, f"An unexpected error occurred: {e}")
                 traceback.print_exc()
                 # Fall through to re-render

        else: # Resolution form itself is invalid
             messages.error(request, "Խնդրում ենք ուղղել լուծման ձևի սխալները։")
             # Fall through to re-render

    # --- Render forms (Handles GET request AND re-render on POST failure) ---
    context = {
        'unmatched_item': unmatched_item,
        'transaction': tx,
        'resolution_form': resolution_form, # Contains initial data or POST data with errors
        'rule_form': rule_form,             # Contains initial data or POST data with errors
        'condition_formset': condition_formset, # Contains initial data or POST data with errors
        'bank_names': BANK_NAMES_LIST       # Pass bank names for select widget
    }
    return render(request, 'tax_processor/resolve_transaction.html', context)


@user_passes_test(is_permitted_user)
def tax_report(request, declaration_id):
    # ... (Keep existing view) ...
    declaration_qs = filter_declarations_by_user(request.user); declaration = get_object_or_404(declaration_qs, pk=declaration_id)
    transactions_qs = Transaction.objects.filter(statement__declaration=declaration, declaration_point__isnull=False)
    report_data = transactions_qs.values('declaration_point__name', 'declaration_point__description', 'declaration_point__is_income','currency').annotate(total_amount=Sum('amount'), transaction_count=Count('pk')).order_by('declaration_point__is_income', 'declaration_point__name','currency')
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
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)
    if unmatched_item.status == 'NEW_RULE_PROPOSED':
        unmatched_item.status = 'PENDING_REVIEW'; unmatched_item.rule_proposal_json = None; unmatched_item.save()
        messages.warning(request, f"Manual proposal #{unmatched_id} rejected. Transaction returned to 'Pending Review'.")
    else: messages.error(request, "Proposal could not be rejected.")
    return redirect('review_proposals')


@user_passes_test(is_permitted_user)
def all_transactions_list(request, declaration_id):
    """
    Displays a paginated, searchable, and sortable list of ALL transactions
    for a specific declaration.
    """
    declaration = get_object_or_404(Declaration, pk=declaration_id)

    # Permission Check: User must own the declaration OR be superadmin
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք դիտելու այս հայտարարագրի գործարքները։") # Not authorized to view...
        return redirect('user_dashboard')

    # Base queryset for the declaration
    queryset = Transaction.objects.filter(statement__declaration=declaration).select_related(
        'declaration_point', 'matched_rule'
    )

    # --- Search ---
    search_query = request.GET.get('q', '').strip()
    if search_query:
        # Search in description OR sender OR amount (exact match for amount might be tricky)
        queryset = queryset.filter(
            Q(description__icontains=search_query) |
            Q(sender__icontains=search_query)
            # Add Q(amount=Decimal(search_query)) etc. if needed, with error handling
        )

    # --- Sorting ---
    sort_by = request.GET.get('sort', '-transaction_date') # Default sort by date descending
    valid_sort_fields = [
        'transaction_date', '-transaction_date',
        'amount', '-amount',
        'currency', '-currency',
        'declaration_point__name', '-declaration_point__name', # Sort by assigned point name
        'sender', '-sender'
    ]
    if sort_by not in valid_sort_fields:
        sort_by = '-transaction_date' # Fallback to default if invalid sort field

    queryset = queryset.order_by(sort_by)

    # --- Pagination ---
    paginator = Paginator(queryset, 50) # Show 50 transactions per page
    page_number = request.GET.get('page')
    try:
        page_obj = paginator.page(page_number)
    except PageNotAnInteger:
        page_obj = paginator.page(1) # If page is not an integer, deliver first page.
    except EmptyPage:
        page_obj = paginator.page(paginator.num_pages) # If page is out of range, deliver last page.

    context = {
        'declaration': declaration,
        'page_obj': page_obj,          # The paginated transactions
        'search_query': search_query,  # Pass search query back to template
        'current_sort': sort_by,       # Pass current sort back to template
        'is_superadmin': is_superadmin(request.user), # For potential future actions
    }
    return render(request, 'tax_processor/all_transactions_list.html', context)

@user_passes_test(is_permitted_user)
def edit_transaction(request, transaction_id):
    """ Allows editing the assigned declaration point or reverting a transaction to pending."""
    transaction_obj = get_object_or_404(Transaction.objects.select_related(
        'statement__declaration', 'declaration_point', 'matched_rule', 'unmatched_record'
    ), pk=transaction_id)
    declaration = transaction_obj.statement.declaration

    # Permission Check: User must own the declaration OR be superadmin
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք խմբագրելու այս գործարքը։") # Not authorized to edit...
        return redirect('user_dashboard')

    if request.method == 'POST':
        form = TransactionEditForm(request.POST)
        if form.is_valid():
            new_declaration_point = form.cleaned_data['declaration_point']
            revert = form.cleaned_data['revert_to_pending']

            with db_transaction.atomic():
                if revert:
                    # Revert to Pending Review
                    transaction_obj.declaration_point = None
                    transaction_obj.matched_rule = None
                    transaction_obj.save()

                    # Find or create UnmatchedTransaction record
                    unmatched, created = UnmatchedTransaction.objects.get_or_create(
                        transaction=transaction_obj,
                        defaults={'assigned_user': request.user} # Assign to current user
                    )
                    unmatched.status = 'PENDING_REVIEW'
                    unmatched.resolved_point = None # Clear previous resolution if any
                    unmatched.resolution_date = None
                    unmatched.rule_proposal_json = None # Clear proposal
                    unmatched.save()

                    messages.info(request, "Գործարքը վերադարձվել է 'Սպասում է Վերանայման' կարգավիճակին։") # Transaction reverted...

                elif new_declaration_point != transaction_obj.declaration_point:
                    # Change Declaration Point (only if different)
                    transaction_obj.declaration_point = new_declaration_point
                    transaction_obj.matched_rule = None # Clear rule match if manually changed
                    transaction_obj.save()

                    # If it had an 'UnmatchedTransaction' record, mark it resolved
                    if hasattr(transaction_obj, 'unmatched_record'):
                        unmatched = transaction_obj.unmatched_record
                        unmatched.status = 'RESOLVED'
                        unmatched.resolved_point = new_declaration_point.name if new_declaration_point else "Reverted"
                        unmatched.resolution_date = timezone.now()
                        unmatched.rule_proposal_json = None # Clear proposal
                        unmatched.save()

                    messages.success(request, f"Գործարքի հայտարարագրման կետը փոխվել է '{new_declaration_point.name if new_declaration_point else 'None'}'-ի։") # Transaction point changed...
                else:
                    # No changes were actually made
                    messages.warning(request, "Փոփոխություններ չեն կատարվել։") # No changes made.

            # Redirect back to the full list after saving
            return redirect('all_transactions_list', declaration_id=declaration.pk)
        # If form is invalid (shouldn't happen with current fields, but good practice)
        else:
             messages.error(request, "Խնդրում ենք ուղղել սխալները։") # Please correct the errors.

    else: # GET request
        # Pre-fill form with current declaration point
        form = TransactionEditForm(initial={'declaration_point': transaction_obj.declaration_point})

    context = {
        'form': form,
        'transaction': transaction_obj,
        'declaration': declaration
    }
    return render(request, 'tax_processor/edit_transaction.html', context)
