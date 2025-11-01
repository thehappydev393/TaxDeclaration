# tax_processor/views.py

from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth.decorators import login_required, user_passes_test
from django.contrib.auth.models import User
from django.contrib import messages
from django.db import IntegrityError
from django.db.models import Q, Count, Sum
from django.utils import timezone
# --- UPDATED: Import new forms ---
from .forms import (
    StatementUploadForm, TaxRuleForm, ResolutionForm, BaseConditionFormSet,
    AddStatementsForm, TransactionEditForm, EntityTypeRuleForm, TransactionScopeRuleForm
)
# --- END UPDATED ---
from .services import import_statement_service
from .rules_engine import RulesEngine
from .entity_type_rules_engine import EntityTypeRulesEngine
from .transaction_scope_rules_engine import TransactionScopeRulesEngine
from .models import (
    Declaration, Transaction, TaxRule, UnmatchedTransaction, UserProfile, DeclarationPoint,
    EntityTypeRule, TransactionScopeRule
)
from .parser_logic import BANK_KEYWORDS
from datetime import date
import json
import traceback
from django.db import transaction as db_transaction
from django.views.decorators.http import require_POST
from django.urls import reverse
from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger

BANK_NAMES_LIST = sorted(list(BANK_KEYWORDS.keys()))

# -----------------------------------------------------------
# 1. PERMISSION HELPERS
# -----------------------------------------------------------
def is_superadmin(user):
    return user.is_authenticated and hasattr(user, 'profile') and user.profile.role == 'SUPERADMIN'

def is_permitted_user(user):
    return user.is_authenticated and hasattr(user, 'profile') and user.profile.role in ['SUPERADMIN', 'REGULAR_USER']

# -----------------------------------------------------------
# 2. DATA INGESTION & DECLARATION MGMT
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
def upload_statement(request):
    if request.method == 'POST':
        form = StatementUploadForm(request.POST, request.FILES)
        if form.is_valid():
            client_name = form.cleaned_data['client_name']
            first_name = form.cleaned_data['first_name']
            last_name = form.cleaned_data['last_name']
            year = form.cleaned_data['year']
            uploaded_files = request.FILES.getlist('statement_files')
            period_start = date(year, 1, 1)
            period_end = date(year, 12, 31)
            declaration_name = f"{year} Հայտարարագիր - {client_name}"
            try:
                declaration_obj = Declaration.objects.create(
                    name=declaration_name,
                    tax_period_start=period_start,
                    tax_period_end=period_end,
                    client_reference=client_name,
                    first_name=first_name,
                    last_name=last_name,
                    status='DRAFT',
                    created_by=request.user,
                )
                messages.success(request, f"Նոր Հայտարարագիր '{declaration_name}' ստեղծված է։")
            except IntegrityError:
                messages.error(
                    request,
                    f"'{declaration_name}' անունով Հայտարարագիր արդեն գոյություն ունի։ "
                    f"Լրացուցիչ քաղվածքներ ավելացնելու համար խնդրում ենք գնալ համապատասխան "
                    f"<a href='{reverse('user_dashboard')}'>Հայտարարագրի մանրամասների էջ</a>։"
                )
                return render(request, 'tax_processor/upload_statement.html', {'form': form})
            total_imported = 0
            if uploaded_files:
                for uploaded_file in uploaded_files:
                    count, message = import_statement_service(uploaded_file=uploaded_file, declaration_obj=declaration_obj, user=request.user)
                    if count > 0: total_imported += count; messages.success(request, f"Ֆայլ {uploaded_file.name}: {message}")
                    else: messages.error(request, f"Ֆայլ {uploaded_file.name}: {message}")
                if total_imported > 0:
                    messages.success(request, f"Բեռնումն ավարտված է։ Ընդհանուր {total_imported} գործարք մշակվել և պահպանվել է '{declaration_name}'-ում։")
                    return redirect('declaration_detail', declaration_id=declaration_obj.pk)
                else:
                    messages.warning(request, "Ֆայլերը վերբեռնվեցին, բայց գործարքներ չեն մշակվել։ Հայտարարագիրը ստեղծված է։")
                    return redirect('declaration_detail', declaration_id=declaration_obj.pk)
            else:
                messages.warning(request, "Ֆայլեր ընտրված չեն։ Հայտարարագիրը ստեղծված է։")
                return redirect('declaration_detail', declaration_id=declaration_obj.pk)
    else: # GET request
        form = StatementUploadForm()
    return render(request, 'tax_processor/upload_statement.html', {'form': form, 'is_admin': is_superadmin(request.user)})

@user_passes_test(is_permitted_user)
def add_statements_to_declaration(request, declaration_id):
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք այս Հայտարարագրին քաղվածքներ ավելացնելու։")
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
                        declaration_obj=declaration,
                        user=request.user
                    )
                    if count > 0:
                        total_imported += count
                        messages.success(request, f"Ֆայլ {uploaded_file.name}: {message}")
                    else:
                        messages.error(request, f"Ֆայլ {uploaded_file.name}: {message}")
                if total_imported > 0:
                    messages.success(request, f"Բեռնումն ավարտված է։ Ընդհանուր {total_imported} նոր գործարք ավելացվել է '{declaration.name}'-ին։")
                elif files_processed > 0:
                     messages.warning(request, "Ֆայլ(եր) մշակվեցին, բայց նոր գործարքներ չավելացվեցին։")
                else:
                     messages.error(request, "Վերբեռնման ընթացքում սխալ տեղի ունեցավ։")
                return redirect('declaration_detail', declaration_id=declaration.pk)
            else:
                 messages.warning(request, "Ֆայլեր ընտրված չեն։")
    else: # GET request
        form = AddStatementsForm()
    context = {'form': form, 'declaration': declaration, 'is_admin': is_superadmin(request.user)}
    return render(request, 'tax_processor/add_statements_form.html', context)


def filter_declarations_by_user(user):
    if is_superadmin(user): return Declaration.objects.all()
    else: return Declaration.objects.filter(created_by=user)

@user_passes_test(is_permitted_user)
def declaration_detail(request, declaration_id):
    declaration_qs = filter_declarations_by_user(request.user); declaration = get_object_or_404(declaration_qs, pk=declaration_id)
    total_statements = declaration.statements.count(); total_transactions = Transaction.objects.filter(statement__declaration=declaration).count(); unassigned_transactions = Transaction.objects.filter(statement__declaration=declaration, declaration_point__isnull=True).count()
    context = {'declaration': declaration, 'total_statements': total_statements, 'total_transactions': total_transactions, 'unassigned_transactions': unassigned_transactions, 'is_admin': is_superadmin(request.user)}
    return render(request, 'tax_processor/declaration_detail.html', context)


@user_passes_test(is_permitted_user)
@require_POST
def run_declaration_analysis(request, declaration_id):
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "You do not have permission to analyze this declaration.")
        return redirect('user_dashboard')
    assigned_user = request.user
    try:
        messages.info(request, "Running Entity Type analysis...")
        entity_engine = EntityTypeRulesEngine(declaration_id=declaration.pk)
        entity_matched = entity_engine.run_analysis(run_all=True)
        messages.success(request, f"Entity Type Engine: Matched {entity_matched} transactions.")
        messages.info(request, "Running Transaction Scope analysis...")
        scope_engine = TransactionScopeRulesEngine(declaration_id=declaration.pk)
        scope_matched = scope_engine.run_analysis(run_all=True)
        messages.success(request, f"Transaction Scope Engine: Matched {scope_matched} transactions.")
        messages.info(request, "Running Main Category analysis...")
        engine = RulesEngine(declaration_id=declaration.pk)
        matched, new_unmatched, cleared_unmatched = engine.run_analysis(assigned_user=assigned_user)
        messages.success(request, f"Analysis complete for '{declaration.name}'.")
        total_processed = matched + new_unmatched + cleared_unmatched
        messages.info(request, f"Total main category transactions processed: {total_processed}")
        messages.info(request, f"Matched {matched} new/re-evaluated categories. Cleared {cleared_unmatched} existing review items.")
        messages.info(request, f"Found {new_unmatched} new transactions requiring manual review.")
    except Exception as e:
        messages.error(request, f"A critical error occurred during analysis: {e}")
        traceback.print_exc()
    return redirect('declaration_detail', declaration_id=declaration.pk)

@user_passes_test(is_permitted_user)
@require_POST
def run_analysis_pending(request, declaration_id):
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Permission denied.")
        return redirect('user_dashboard')
    assigned_user = request.user
    try:
        messages.info(request, "Running Entity Type analysis (Pending)...")
        entity_engine = EntityTypeRulesEngine(declaration_id=declaration.pk)
        entity_matched = entity_engine.run_analysis(run_all=False)
        messages.success(request, f"Entity Type Engine: Matched {entity_matched} transactions.")
        messages.info(request, "Running Transaction Scope analysis (Pending)...")
        scope_engine = TransactionScopeRulesEngine(declaration_id=declaration.pk)
        scope_matched = scope_engine.run_analysis(run_all=False)
        messages.success(request, f"Transaction Scope Engine: Matched {scope_matched} transactions.")
        messages.info(request, "Running Main Category analysis (New & Pending)...")
        engine = RulesEngine(declaration_id=declaration.pk)
        matched, new_unmatched, cleared_unmatched = engine.run_analysis_pending_only(assigned_user=assigned_user)
        messages.success(request, f"Վերլուծություն (Նոր և Սպասվող) ավարտվեց «{declaration.name}»-ի համար։")
        total_processed = matched + new_unmatched
        messages.info(request, f"Ընդհանուր մշակված գործարքներ՝ {total_processed}")
        messages.info(request, f"Համընկել է {matched} նոր/սպասվող գործարք։")
        if cleared_unmatched > 0:
            messages.info(request, f"Մաքրվել է {cleared_unmatched} գործարք 'Սպասում է Վերանայման' հերթից։")
        messages.info(request, f"Հայտնաբերվել է {new_unmatched} նոր գործարք, որոնք պահանջում են ձեռքով վերանայում։")
    except Exception as e:
        messages.error(request, f"A critical error occurred during pending analysis: {e}")
        traceback.print_exc()
    return redirect('declaration_detail', declaration_id=declaration.pk)

# -----------------------------------------------------------
# 3. GLOBAL (CATEGORY) RULE MANAGEMENT
# -----------------------------------------------------------

@user_passes_test(is_superadmin)
def rule_list_global(request):
    queryset = TaxRule.objects.filter(declaration__isnull=True).select_related('declaration_point', 'created_by')
    search_query = request.GET.get('q', '').strip()
    if search_query:
        queryset = queryset.filter(
            Q(rule_name__icontains=search_query) |
            Q(declaration_point__name__icontains=search_query)
        )
    filter_active = request.GET.get('filter_active', '')
    if filter_active:
        queryset = queryset.filter(is_active=(filter_active == 'true'))
    filter_proposal = request.GET.get('filter_proposal', '')
    if filter_proposal:
        queryset = queryset.filter(proposal_status=filter_proposal)
    sort_by = request.GET.get('sort', 'priority')
    valid_sort_fields = [
        'priority', '-priority', 'rule_name', '-rule_name', 'declaration_point__name', '-declaration_point__name',
        'is_active', '-is_active', 'proposal_status', '-proposal_status', 'created_at', '-created_at'
    ]
    if sort_by not in valid_sort_fields:
        sort_by = 'priority'
    queryset = queryset.order_by(sort_by)
    paginator = Paginator(queryset, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    get_params = request.GET.copy()
    if 'page' in get_params:
        del get_params['page']
    context = {
        'rules': page_obj, 'page_obj': page_obj, 'is_global_list': True, 'list_title': "Գլոբալ Կանոններ",
        'is_admin': True, 'search_query': search_query, 'filter_active': filter_active,
        'filter_proposal': filter_proposal, 'current_sort': sort_by, 'get_params': get_params.urlencode()
    }
    return render(request, 'tax_processor/rule_list.html', context)


@user_passes_test(is_superadmin)
def rule_create_or_update(request, rule_id=None, declaration_id=None):
    is_specific_rule = declaration_id is not None
    declaration = None
    rule = None
    title = ""
    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "You don't have permission to manage rules for this declaration.")
             return redirect('user_dashboard')
        if rule_id:
             rule = get_object_or_404(TaxRule, pk=rule_id, declaration=declaration)
             title = f"Update Specific Rule: {rule.rule_name}"
        else:
             title = f"Create New Rule for {declaration.name}"
        list_url_name = 'declaration_rule_list'
        url_kwargs = {'declaration_id': declaration_id}
    else:
        if not is_superadmin(request.user):
            messages.error(request, "You need superadmin rights to manage global rules.")
            return redirect('user_dashboard')
        if rule_id:
            rule = get_object_or_404(TaxRule, pk=rule_id, declaration__isnull=True)
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
            if is_specific_rule:
                new_rule.declaration = declaration
            else:
                 new_rule.declaration = None
            checks = []
            for check_form in formset.cleaned_data:
                if check_form and not check_form.get('DELETE'): checks.append({'field': check_form['field'], 'type': check_form['condition_type'], 'value': check_form['value']})
            new_rule.conditions_json = [{'logic': form.cleaned_data['logic'], 'checks': checks}]
            if rule and rule.proposal_status == 'PENDING_GLOBAL' and not is_superadmin(request.user):
                 new_rule.proposal_status = 'NONE'
                 messages.info(request, "Rule edited, global proposal status reset.")
            try:
                 new_rule.save()
                 messages.success(request, f"Tax Rule '{new_rule.rule_name}' saved successfully.")
                 return redirect(list_url_name, **url_kwargs)
            except IntegrityError:
                 messages.error(request, f"A rule named '{new_rule.rule_name}' already exists for this scope (global or specific declaration). Please choose a different name.")
                 context = {
                     'form': form, 'formset': formset, 'title': title, 'rule': rule, 'declaration': declaration,
                     'is_specific_rule': is_specific_rule, 'list_url_name': list_url_name, 'url_kwargs': url_kwargs,
                     'is_admin': is_superadmin(request.user), 'bank_names': BANK_NAMES_LIST
                 }
                 return render(request, 'tax_processor/rule_form.html', context)
    else:
        initial_form_data = {}; initial_formset_data = []
        if rule:
            if rule.conditions_json and isinstance(rule.conditions_json, list) and len(rule.conditions_json) > 0 and rule.conditions_json[0]:
                logic_block = rule.conditions_json[0]; initial_form_data['logic'] = logic_block.get('logic', 'AND')
                for check in logic_block.get('checks', []): initial_formset_data.append({'field': check.get('field'), 'condition_type': check.get('type'), 'value': check.get('value')})
        else: initial_form_data['logic'] = 'AND'
        form = TaxRuleForm(instance=rule, initial=initial_form_data)
        formset = BaseConditionFormSet(initial=initial_formset_data, prefix=formset_prefix)
    context = {
        'form': form, 'formset': formset, 'title': title, 'rule': rule,
        'declaration': declaration,
        'is_specific_rule': is_specific_rule,
        'list_url_name': list_url_name,
        'url_kwargs': url_kwargs,
        'bank_names': BANK_NAMES_LIST,
        'is_admin': is_superadmin(request.user)
    }
    return render(request, 'tax_processor/rule_form.html', context)


@user_passes_test(is_permitted_user)
@require_POST
def rule_delete(request, rule_id, declaration_id=None):
    is_specific_rule = declaration_id is not None
    rule = None
    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(TaxRule, pk=rule_id, declaration=declaration)
        list_url_name = 'declaration_rule_list'; url_kwargs = {'declaration_id': declaration_id}
    else:
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(TaxRule, pk=rule_id, declaration__isnull=True)
        list_url_name = 'rule_list_global'; url_kwargs = {}
    rule_name = rule.rule_name
    rule.delete()
    messages.success(request, f"Tax Rule '{rule_name}' successfully deleted.")
    return redirect(list_url_name, **url_kwargs)


# -----------------------------------------------------------
# 4. DECLARATION-SPECIFIC (CATEGORY) RULE LIST VIEW
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
def declaration_rule_list(request, declaration_id):
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "You don't have permission to view rules for this declaration.")
        return redirect('user_dashboard')
    queryset = TaxRule.objects.filter(declaration=declaration).select_related('declaration_point', 'created_by')
    search_query = request.GET.get('q', '').strip()
    if search_query:
        queryset = queryset.filter(
            Q(rule_name__icontains=search_query) |
            Q(declaration_point__name__icontains=search_query)
        )
    filter_active = request.GET.get('filter_active', '')
    if filter_active:
        queryset = queryset.filter(is_active=(filter_active == 'true'))
    filter_proposal = request.GET.get('filter_proposal', '')
    if filter_proposal:
        queryset = queryset.filter(proposal_status=filter_proposal)
    sort_by = request.GET.get('sort', 'priority')
    valid_sort_fields = [
        'priority', '-priority', 'rule_name', '-rule_name', 'declaration_point__name', '-declaration_point__name',
        'is_active', '-is_active', 'proposal_status', '-proposal_status', 'created_at', '-created_at'
    ]
    if sort_by not in valid_sort_fields:
        sort_by = 'priority'
    queryset = queryset.order_by(sort_by)
    paginator = Paginator(queryset, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    get_params = request.GET.copy()
    if 'page' in get_params:
        del get_params['page']
    context = {
        'rules': page_obj, 'page_obj': page_obj, 'declaration': declaration,
        'is_global_list': False, 'list_title': f"Կանոններ {declaration.name}-ի համար",
        'is_admin': is_superadmin(request.user), 'search_query': search_query,
        'filter_active': filter_active, 'filter_proposal': filter_proposal,
        'current_sort': sort_by, 'get_params': get_params.urlencode()
    }
    return render(request, 'tax_processor/rule_list.html', context)


# -----------------------------------------------------------
# 5. GLOBAL RULE PROPOSAL WORKFLOW
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
@require_POST
def propose_rule_global(request, rule_id):
    rule_query = Q(pk=rule_id) & Q(declaration__isnull=False)
    if not is_superadmin(request.user):
        rule_query &= Q(declaration__created_by=request.user)
    rule = get_object_or_404(TaxRule, rule_query)
    if rule.proposal_status == 'NONE':
        rule.proposal_status = 'PENDING_GLOBAL'
        rule.save()
        messages.success(request, f"Rule '{rule.rule_name}' proposed for global use. A superadmin will review it.")
    else:
        messages.warning(request, f"Rule '{rule.rule_name}' has already been proposed or processed.")
    return redirect('declaration_rule_list', declaration_id=rule.declaration.pk)


@user_passes_test(is_superadmin)
def review_global_proposals(request):
    proposals = TaxRule.objects.filter(proposal_status='PENDING_GLOBAL').select_related('declaration', 'declaration_point', 'created_by')
    context = {
        'proposals': proposals,
        'title': "Review Proposed Global Rules",
        'is_admin': True
    }
    return render(request, 'tax_processor/review_global_proposals.html', context)


@user_passes_test(is_superadmin)
@require_POST
def approve_global_proposal(request, rule_id):
    rule = get_object_or_404(TaxRule, pk=rule_id, proposal_status='PENDING_GLOBAL')
    original_decl_id = rule.declaration_id
    new_name = rule.rule_name
    if TaxRule.objects.filter(declaration__isnull=True, rule_name=new_name).exists():
        messages.error(request, f"Cannot approve rule '{new_name}'. A global rule with this name already exists. Please edit the name before approving or reject the proposal.")
        return redirect('review_global_proposals')
    rule.declaration = None
    rule.proposal_status = 'NONE'
    rule.save()
    messages.success(request, f"Rule '{rule.rule_name}' (from Declaration {original_decl_id}) approved and converted to a global rule.")
    return redirect('review_global_proposals')


@user_passes_test(is_superadmin)
@require_POST
def reject_global_proposal(request, rule_id):
    rule = get_object_or_404(TaxRule, pk=rule_id, proposal_status='PENDING_GLOBAL')
    rule.proposal_status = 'NONE'
    rule.save()
    messages.warning(request, f"Proposal for rule '{rule.rule_name}' rejected. It remains a specific rule for Declaration {rule.declaration_id}.")
    return redirect('review_global_proposals')


# -----------------------------------------------------------
# 6. USER DASHBOARD & REVIEW QUEUES
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
def user_dashboard(request):
    user = request.user
    queryset = filter_declarations_by_user(user)
    queryset = queryset.annotate(
        statement_count=Count('statements', distinct=True),
        total_transactions=Count('statements__transactions', distinct=True),
        unmatched_count=Count('statements__transactions__unmatched_record', filter=Q(statements__transactions__unmatched_record__status='PENDING_REVIEW'), distinct=True)
    )
    search_query = request.GET.get('q', '').strip()
    if search_query:
        queryset = queryset.filter(
            Q(name__icontains=search_query) |
            Q(client_reference__icontains=search_query) |
            Q(first_name__icontains=search_query) |
            Q(last_name__icontains=search_query)
        )
    filter_status = request.GET.get('filter_status', '')
    if filter_status:
        queryset = queryset.filter(status=filter_status)
    sort_by = request.GET.get('sort', '-tax_period_start')
    valid_sort_fields = [
        'name', '-name', 'tax_period_start', '-tax_period_start', 'statement_count', '-statement_count',
        'total_transactions', '-total_transactions', 'unmatched_count', '-unmatched_count',
    ]
    if sort_by not in valid_sort_fields:
        sort_by = '-tax_period_start'
    queryset = queryset.order_by(sort_by)
    paginator = Paginator(queryset, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    get_params = request.GET.copy()
    if 'page' in get_params:
        del get_params['page']
    context = {
        'declarations': page_obj, 'page_obj': page_obj, 'is_admin': is_superadmin(user),
        'search_query': search_query, 'filter_status': filter_status,
        'current_sort': sort_by, 'get_params': get_params.urlencode()
    }
    return render(request, 'tax_processor/user_dashboard.html', context)


@user_passes_test(is_permitted_user)
def review_queue(request, declaration_id=None):
    user = request.user
    queryset = UnmatchedTransaction.objects.filter(status='PENDING_REVIEW')
    is_filtered_by_declaration = False
    title = ""
    current_declaration = None
    if declaration_id:
        current_declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(user) or current_declaration.created_by == user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        queryset = queryset.filter(transaction__statement__declaration_id=declaration_id)
        title = f"Review Queue - {current_declaration.name}"
        is_filtered_by_declaration = True
    elif is_superadmin(user):
        title = "SUPERADMIN Review Queue (All Pending)"
    else:
        queryset = queryset.filter(assigned_user=user)
        title = f"{user.username}'s Pending Reviews"
    queryset = queryset.select_related(
        'transaction__statement__declaration',
        'transaction__matched_rule',
        'assigned_user'
    )
    search_query = request.GET.get('q', '').strip()
    if search_query:
        queryset = queryset.filter(
            Q(transaction__description__icontains=search_query) |
            Q(transaction__sender__icontains=search_query)
        )
    filter_user = request.GET.get('filter_user', '')
    if is_superadmin(user) and not declaration_id and filter_user:
        queryset = queryset.filter(assigned_user_id=filter_user)
    sort_by = request.GET.get('sort', '-transaction__transaction_date')
    valid_sort_fields = [
        'transaction__transaction_date', '-transaction__transaction_date', 'transaction__amount', '-transaction__amount',
        'transaction__description', '-transaction__description', 'transaction__sender', '-transaction__sender',
        'transaction__statement__declaration__name', '-transaction__statement__declaration__name',
    ]
    if sort_by not in valid_sort_fields:
        sort_by = '-transaction__transaction_date'
    queryset = queryset.order_by(sort_by)
    paginator = Paginator(queryset, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    get_params = request.GET.copy()
    if 'page' in get_params:
        del get_params['page']
    context = {
        'title': title, 'unmatched_items': page_obj, 'page_obj': page_obj,
        'is_admin': is_superadmin(user), 'is_filtered': is_filtered_by_declaration,
        'current_declaration': current_declaration, 'search_query': search_query,
        'filter_user': filter_user, 'current_sort': sort_by, 'get_params': get_params.urlencode()
    }
    if is_superadmin(user) and not declaration_id:
        context['all_users'] = User.objects.filter(assigned_reviews__status='PENDING_REVIEW').distinct()
    return render(request, 'tax_processor/review_queue.html', context)


@user_passes_test(is_permitted_user)
def resolve_transaction(request, unmatched_id):
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)
    tx = unmatched_item.transaction
    declaration = tx.statement.declaration
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք լուծելու այս գործարքը։")
        return redirect('user_dashboard')
    resolution_form_prefix = 'res'; rule_form_prefix = 'rule'; condition_formset_prefix = 'cond'
    if request.method != 'POST':
        resolution_form = ResolutionForm(initial={'unmatched_id': unmatched_id}, prefix=resolution_form_prefix)
        suggested_rule_name = f"Rule based on: {tx.description[:50]}..."
        initial_rule_data = {'rule_name': suggested_rule_name, 'priority': 50, 'is_active': True, 'logic': 'AND'}
        rule_form = TaxRuleForm(initial=initial_rule_data, prefix=rule_form_prefix)
        initial_condition_data = [{'field': 'description', 'condition_type': 'CONTAINS_KEYWORD', 'value': tx.description[:200]}]
        condition_formset = BaseConditionFormSet(initial=initial_condition_data, prefix=condition_formset_prefix)
    else:
        resolution_form = ResolutionForm(request.POST, prefix=resolution_form_prefix)
        rule_form = TaxRuleForm(request.POST, prefix=rule_form_prefix)
        condition_formset = BaseConditionFormSet(request.POST, prefix=condition_formset_prefix)
        if resolution_form.is_valid():
            resolved_point_obj = resolution_form.cleaned_data['resolved_point']
            action = resolution_form.cleaned_data['rule_action']
            forms_are_valid = True
            try:
                if action == 'create_specific':
                    rule_name = request.POST.get(f'{rule_form_prefix}-rule_name', '').strip()
                    priority_str = request.POST.get(f'{rule_form_prefix}-priority', '').strip()
                    declaration_point_obj_for_rule = resolved_point_obj
                    logic = request.POST.get(f'{rule_form_prefix}-logic', '').strip()
                    if not rule_name:
                        rule_form.add_error('rule_name', 'Կանոնի անվանումը պարտադիր է։')
                        forms_are_valid = False
                    if not priority_str:
                         rule_form.add_error('priority', 'Առաջնահերթությունը պարտադիր է։')
                         forms_are_valid = False
                    if declaration_point_obj_for_rule is None:
                         messages.error(request, "Հայտարարագրման կետը պարտադիր է կանոնի համար։")
                         forms_are_valid = False
                    if not logic:
                         rule_form.add_error('logic', 'Կանոնի տրամաբանությունը պարտադիր է։')
                         forms_are_valid = False
                    if not condition_formset.is_valid():
                         messages.error(request, "Խնդրում ենք ուղղել սխալները կանոնի պայմաններում։")
                         forms_are_valid = False
                    elif not any(form and not form.get('DELETE', False) for form in condition_formset.cleaned_data):
                         messages.error(request, "Կանոն ստեղծելու համար պետք է ավելացնել առնվազն մեկ պայման։")
                         forms_are_valid = False
                    temp_data = rule_form.data.copy()
                    temp_data[f'{rule_form_prefix}-declaration_point'] = declaration_point_obj_for_rule.pk if declaration_point_obj_for_rule else ''
                    temp_rule_form_for_validation = TaxRuleForm(temp_data, prefix=rule_form_prefix)
                    if forms_are_valid and not temp_rule_form_for_validation.is_valid():
                         rule_form._errors = temp_rule_form_for_validation.errors
                         forms_are_valid = False
                if forms_are_valid:
                    with db_transaction.atomic():
                        tx.declaration_point = resolved_point_obj; tx.matched_rule = None
                        new_rule = None
                        if action == 'create_specific':
                            temp_data = rule_form.data.copy()
                            temp_data[f'{rule_form_prefix}-declaration_point'] = resolved_point_obj.pk
                            final_rule_form = TaxRuleForm(temp_data, prefix=rule_form_prefix)
                            if final_rule_form.is_valid() and condition_formset.is_valid():
                                new_rule = final_rule_form.save(commit=False)
                                new_rule.created_by = request.user
                                new_rule.declaration = declaration
                                new_rule.proposal_status = 'NONE'
                                checks = [{'field': f['field'], 'type': f['condition_type'], 'value': f['value']} for f in condition_formset.cleaned_data if f and not f.get('DELETE')]
                                new_rule.conditions_json = [{'logic': final_rule_form.cleaned_data['logic'], 'checks': checks}]
                                new_rule.save()
                                tx.matched_rule = new_rule
                                messages.success(request, f"Գործարքը լուծված է։ Նոր հատուկ կանոն '{new_rule.rule_name}' ստեղծված է։")
                            else:
                                print("ERROR: Rule form/formset invalid during save attempt.")
                                messages.error(request, "Internal validation error during rule save.")
                                raise ValueError("Rule form/formset invalid during save.")
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
                        tx.save()
                    return redirect('declaration_detail', declaration_id=declaration.pk)
                else:
                     messages.error(request, "Խնդրում ենք ուղղել նշված սխալները։")
            except IntegrityError:
                 messages.error(request, f"Այս անունով կանոն արդեն գոյություն ունի այս հայտարարագրի համար։ Խնդրում ենք ընտրել այլ անուն։")
            except Exception as e:
                 messages.error(request, f"An unexpected error occurred: {e}")
                 traceback.print_exc()
        else:
             messages.error(request, "Խնդրում ենք ուղղել լուծման ձևի սխալները։")
    context = {
        'unmatched_item': unmatched_item,
        'transaction': tx,
        'resolution_form': resolution_form,
        'rule_form': rule_form,
        'condition_formset': condition_formset,
        'bank_names': BANK_NAMES_LIST,
        'is_admin': is_superadmin(request.user)
    }
    return render(request, 'tax_processor/resolve_transaction.html', context)


@user_passes_test(is_permitted_user)
def tax_report(request, declaration_id):
    declaration_qs = filter_declarations_by_user(request.user); declaration = get_object_or_404(declaration_qs, pk=declaration_id)
    transactions_qs = Transaction.objects.filter(statement__declaration=declaration, declaration_point__isnull=False)
    report_data = transactions_qs.values('declaration_point__name', 'declaration_point__description', 'declaration_point__is_income','currency').annotate(total_amount=Sum('amount'), transaction_count=Count('pk')).order_by('declaration_point__is_income', 'declaration_point__name','currency')
    currency_totals = transactions_qs.values('currency').annotate(total_amount=Sum('amount')).order_by('currency')
    context = {
        'declaration': declaration,
        'report_data': report_data,
        'currency_totals': currency_totals,
        'is_admin': is_superadmin(request.user)
    }
    return render(request, 'tax_processor/tax_report.html', context)


# -----------------------------------------------------------
# 7. SUPERADMIN MANUAL PROPOSAL WORKFLOW
# -----------------------------------------------------------
@user_passes_test(is_superadmin)
def review_proposals(request):
    proposals = UnmatchedTransaction.objects.filter(status='NEW_RULE_PROPOSED').select_related('transaction__statement__declaration', 'assigned_user').order_by('-resolution_date')
    context = {
        'proposals': proposals,
        'title': 'New Manual Rule Proposals Awaiting Review',
        'is_admin': True
    }
    return render(request, 'tax_processor/review_proposals.html', context)


@user_passes_test(is_superadmin)
def finalize_rule(request, unmatched_id):
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id); transaction = unmatched_item.transaction
    if unmatched_item.status != 'NEW_RULE_PROPOSED': messages.error(request, "Not a pending proposal."); return redirect('review_proposals')
    proposal_data = unmatched_item.rule_proposal_json; proposed_category_name = proposal_data.get('resolved_point_name', 'N/A')
    if request.method == 'POST':
        form = TaxRuleForm(request.POST, instance=None); formset = BaseConditionFormSet(request.POST, prefix='conditions')
        if form.is_valid() and formset.is_valid():
            with db_transaction.atomic():
                new_rule = form.save(commit=False); new_rule.created_by = request.user; new_rule.declaration = None
                checks = [];
                for check_form in formset.cleaned_data:
                     if check_form and not check_form.get('DELETE'): checks.append({'field': check_form['field'], 'type': check_form['condition_type'], 'value': check_form['value']})
                new_rule.conditions_json = [{'logic': form.cleaned_data['logic'], 'checks': checks}]
                try:
                    new_rule.save()
                    unmatched_item.status = 'RESOLVED'; unmatched_item.save()
                    messages.success(request, f"New Global Rule '{new_rule.rule_name}' created from proposal.")
                    return redirect('review_proposals')
                except IntegrityError:
                     messages.error(request, f"Cannot save rule '{new_rule.rule_name}'. A global rule with this name already exists.")
                     context = {
                         'form': form, 'formset': formset, 'unmatched_item': unmatched_item, 'transaction': transaction,
                         'title': f"Finalize Rule Proposal #{unmatched_id}", 'proposal_notes': proposal_data.get('notes'),
                         'proposed_category_name': proposed_category_name, 'is_admin': True
                    }
                     return render(request, 'tax_processor/finalize_rule.html', context)
        else:
             context = {
                 'form': form, 'formset': formset, 'unmatched_item': unmatched_item, 'transaction': transaction,
                 'title': f"Finalize Rule Proposal #{unmatched_id}", 'proposal_notes': proposal_data.get('notes'),
                 'proposed_category_name': proposed_category_name, 'is_admin': True
            }
             return render(request, 'tax_processor/finalize_rule.html', context)
    else: # GET
        initial_form_data = {'rule_name': f"AUTO_RULE: {unmatched_item.pk} - {proposed_category_name}", 'declaration_point': proposal_data.get('resolved_point_id'), 'priority': 50, 'is_active': True, 'logic': 'AND'}
        initial_formset_data = [{'field': 'description', 'condition_type': 'CONTAINS_KEYWORD', 'value': proposal_data.get('sample_description', '')}]
        form = TaxRuleForm(initial=initial_form_data); formset = BaseConditionFormSet(initial=initial_formset_data, prefix='conditions')
        context = {
            'form': form, 'formset': formset, 'unmatched_item': unmatched_item, 'transaction': transaction,
            'title': f"Finalize Rule Proposal #{unmatched_id}", 'proposal_notes': proposal_data.get('notes'),
            'proposed_category_name': proposed_category_name, 'is_admin': True
        }
        return render(request, 'tax_processor/finalize_rule.html', context)


@require_POST
@user_passes_test(is_superadmin)
def reject_proposal(request, unmatched_id):
    unmatched_item = get_object_or_404(UnmatchedTransaction, pk=unmatched_id)
    if unmatched_item.status == 'NEW_RULE_PROPOSED':
        unmatched_item.status = 'PENDING_REVIEW'; unmatched_item.rule_proposal_json = None; unmatched_item.save()
        messages.warning(request, f"Manual proposal #{unmatched_id} rejected. Transaction returned to 'Pending Review'.")
    else: messages.error(request, "Proposal could not be rejected.")
    return redirect('review_proposals')

# -----------------------------------------------------------
# 8. ALL TRANSACTIONS & EDIT
# -----------------------------------------------------------
@user_passes_test(is_permitted_user)
def all_transactions_list(request, declaration_id):
    declaration = get_object_or_404(Declaration, pk=declaration_id)
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք դիտելու այս հայտարարագրի գործարքները։")
        return redirect('user_dashboard')
    queryset = Transaction.objects.filter(statement__declaration=declaration).select_related(
        'declaration_point', 'matched_rule', 'unmatched_record'
    )
    search_query = request.GET.get('q', '').strip()
    if search_query:
        queryset = queryset.filter(
            Q(description__icontains=search_query) |
            Q(sender__icontains=search_query)
        )
    filter_type = request.GET.get('filter_type', '')
    if filter_type:
        queryset = queryset.filter(is_expense=(filter_type == 'expense'))
    filter_entity = request.GET.get('filter_entity', '')
    if filter_entity:
        queryset = queryset.filter(entity_type=filter_entity)
    filter_scope = request.GET.get('filter_scope', '')
    if filter_scope:
        queryset = queryset.filter(transaction_scope=filter_scope)
    filter_status = request.GET.get('filter_status', '')
    if filter_status:
        if filter_status == 'ASSIGNED':
            queryset = queryset.filter(declaration_point__isnull=False)
        elif filter_status == 'PENDING':
            queryset = queryset.filter(Q(declaration_point__isnull=True) & Q(unmatched_record__status='PENDING_REVIEW'))
        elif filter_status == 'PROPOSED':
            queryset = queryset.filter(unmatched_record__status='NEW_RULE_PROPOSED')
        elif filter_status == 'UNPROCESSED':
             queryset = queryset.filter(declaration_point__isnull=True, unmatched_record__isnull=True)
    sort_by = request.GET.get('sort', '-transaction_date')
    valid_sort_fields = [
        'transaction_date', '-transaction_date', 'amount', '-amount', 'currency', '-currency',
        'declaration_point__name', '-declaration_point__name', 'sender', '-sender',
        'is_expense', '-is_expense', 'entity_type', '-entity_type', 'transaction_scope', '-transaction_scope'
    ]
    if sort_by not in valid_sort_fields:
        sort_by = '-transaction_date'
    queryset = queryset.order_by(sort_by)
    paginator = Paginator(queryset, 50)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    get_params = request.GET.copy()
    if 'page' in get_params:
        del get_params['page']
    context = {
        'declaration': declaration, 'page_obj': page_obj, 'search_query': search_query, 'current_sort': sort_by,
        'is_admin': is_superadmin(request.user), 'get_params': get_params.urlencode(),
        'filter_type': filter_type, 'filter_entity': filter_entity, 'filter_scope': filter_scope,
        'filter_status': filter_status, 'entity_choices': Transaction.ENTITY_CHOICES,
        'scope_choices': Transaction.SCOPE_CHOICES,
    }
    return render(request, 'tax_processor/all_transactions_list.html', context)

@user_passes_test(is_permitted_user)
def edit_transaction(request, transaction_id):
    transaction_obj = get_object_or_404(Transaction.objects.select_related(
        'statement__declaration', 'declaration_point', 'matched_rule', 'unmatched_record'
    ), pk=transaction_id)
    declaration = transaction_obj.statement.declaration
    if not (is_superadmin(request.user) or declaration.created_by == request.user):
        messages.error(request, "Դուք իրավասու չեք խմբագրելու այս գործարքը։")
        return redirect('user_dashboard')
    if request.method == 'POST':
        form = TransactionEditForm(request.POST)
        if form.is_valid():
            new_declaration_point = form.cleaned_data['declaration_point']
            revert = form.cleaned_data['revert_to_pending']
            with db_transaction.atomic():
                if revert:
                    transaction_obj.declaration_point = None
                    transaction_obj.matched_rule = None
                    transaction_obj.save()
                    unmatched, created = UnmatchedTransaction.objects.get_or_create(
                        transaction=transaction_obj,
                        defaults={'assigned_user': request.user}
                    )
                    unmatched.status = 'PENDING_REVIEW'
                    unmatched.resolved_point = None
                    unmatched.resolution_date = None
                    unmatched.rule_proposal_json = None
                    unmatched.save()
                    messages.info(request, "Գործարքը վերադարձվել է 'Սպասում է Վերանայման' կարգավիճակին։")
                elif new_declaration_point != transaction_obj.declaration_point:
                    transaction_obj.declaration_point = new_declaration_point
                    transaction_obj.matched_rule = None
                    transaction_obj.save()
                    if hasattr(transaction_obj, 'unmatched_record'):
                        unmatched = transaction_obj.unmatched_record
                        unmatched.status = 'RESOLVED'
                        unmatched.resolved_point = new_declaration_point.name if new_declaration_point else "Reverted"
                        unmatched.resolution_date = timezone.now()
                        unmatched.rule_proposal_json = None
                        unmatched.save()
                    messages.success(request, f"Գործարքի հայտարարագրման կետը փոխվել է '{new_declaration_point.name if new_declaration_point else 'None'}'-ի։")
                else:
                    messages.warning(request, "Փոփոխություններ չեն կատարվել։")
            return redirect('all_transactions_list', declaration_id=declaration.pk)
        else:
             messages.error(request, "Խնդրում ենք ուղղել սխալները։")
    else: # GET request
        form = TransactionEditForm(initial={'declaration_point': transaction_obj.declaration_point})
    context = {
        'form': form,
        'transaction': transaction_obj,
        'declaration': declaration,
        'is_admin': is_superadmin(request.user)
    }
    return render(request, 'tax_processor/edit_transaction.html', context)


# --- NEW: EntityTypeRule Views ---

@user_passes_test(is_superadmin)
def entity_rule_list(request, declaration_id=None):
    """Lists global or specific EntityTypeRules."""
    is_specific = declaration_id is not None
    declaration = None
    if is_specific:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        queryset = EntityTypeRule.objects.filter(declaration=declaration)
        list_title = f"Entity Rules for {declaration.name}"
    else:
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        queryset = EntityTypeRule.objects.filter(declaration__isnull=True)
        list_title = "Global Entity Type Rules"

    queryset = queryset.select_related('declaration', 'created_by')

    # --- Search, Filter, Sort, Paginate (re-using logic) ---
    search_query = request.GET.get('q', '').strip()
    if search_query:
        queryset = queryset.filter(rule_name__icontains=search_query)
    filter_active = request.GET.get('filter_active', '')
    if filter_active:
        queryset = queryset.filter(is_active=(filter_active == 'true'))
    sort_by = request.GET.get('sort', 'priority')
    valid_sort_fields = ['priority', '-priority', 'rule_name', '-rule_name', 'entity_type_result', '-entity_type_result', 'is_active', '-is_active', 'created_at', '-created_at']
    if sort_by not in valid_sort_fields: sort_by = 'priority'
    queryset = queryset.order_by(sort_by)
    paginator = Paginator(queryset, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    get_params = request.GET.copy()
    if 'page' in get_params: del get_params['page']

    context = {
        'rules': page_obj, 'page_obj': page_obj, 'declaration': declaration,
        'is_global_list': not is_specific, 'list_title': list_title,
        'is_admin': is_superadmin(request.user), 'search_query': search_query,
        'filter_active': filter_active, 'current_sort': sort_by, 'get_params': get_params.urlencode(),
        'rule_type': 'entity' # For template URLs
    }
    # We'll create 'entity_rule_list.html' next
    return render(request, 'tax_processor/entity_rule_list.html', context)

@user_passes_test(is_superadmin)
def entity_rule_create_or_update(request, rule_id=None, declaration_id=None):
    """Creates or updates an EntityTypeRule (global or specific)."""
    is_specific_rule = declaration_id is not None
    declaration = None
    rule = None
    title = ""

    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "Permission denied."); return redirect('user_dashboard')
        if rule_id:
             rule = get_object_or_404(EntityTypeRule, pk=rule_id, declaration=declaration)
             title = f"Update Specific Entity Rule: {rule.rule_name}"
        else:
             title = f"Create New Entity Rule for {declaration.name}"
        list_url_name = 'entity_rule_list_specific'
        url_kwargs = {'declaration_id': declaration_id}
    else:
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        if rule_id:
            rule = get_object_or_404(EntityTypeRule, pk=rule_id, declaration__isnull=True)
            title = f"Update Global Entity Rule: {rule.rule_name}"
        else:
            title = "Create New Global Entity Rule"
        list_url_name = 'entity_rule_list_global'
        url_kwargs = {}

    formset_prefix = 'conditions'
    if request.method == 'POST':
        form = EntityTypeRuleForm(request.POST, instance=rule)
        formset = BaseConditionFormSet(request.POST, prefix=formset_prefix)
        if form.is_valid() and formset.is_valid():
            new_rule = form.save(commit=False)
            if not new_rule.pk: new_rule.created_by = request.user
            if is_specific_rule:
                new_rule.declaration = declaration
            else:
                 new_rule.declaration = None
            checks = []
            for check_form in formset.cleaned_data:
                if check_form and not check_form.get('DELETE'): checks.append({'field': check_form['field'], 'type': check_form['condition_type'], 'value': check_form['value']})
            new_rule.conditions_json = [{'logic': form.cleaned_data['logic'], 'checks': checks}]
            try:
                 new_rule.save()
                 messages.success(request, f"Entity Rule '{new_rule.rule_name}' saved successfully.")
                 return redirect(list_url_name, **url_kwargs)
            except IntegrityError:
                 messages.error(request, f"An entity rule named '{new_rule.rule_name}' already exists for this scope.")
                 # Re-render context
        # else:
             # Fall through to re-render context on invalid form

    else: # GET request
        initial_form_data = {}; initial_formset_data = []
        if rule:
            if rule.conditions_json and isinstance(rule.conditions_json, list) and len(rule.conditions_json) > 0 and rule.conditions_json[0]:
                logic_block = rule.conditions_json[0]; initial_form_data['logic'] = logic_block.get('logic', 'AND')
                for check in logic_block.get('checks', []): initial_formset_data.append({'field': check.get('field'), 'condition_type': check.get('type'), 'value': check.get('value')})
        else: initial_form_data['logic'] = 'AND'
        form = EntityTypeRuleForm(instance=rule, initial=initial_form_data)
        formset = BaseConditionFormSet(initial=initial_formset_data, prefix=formset_prefix)

    context = {
        'form': form, 'formset': formset, 'title': title, 'rule': rule,
        'declaration': declaration, 'is_specific_rule': is_specific_rule,
        'list_url_name': list_url_name, 'url_kwargs': url_kwargs,
        'bank_names': BANK_NAMES_LIST, 'is_admin': is_superadmin(request.user),
        'rule_type': 'entity' # For template URLs
    }
    # Re-use the main rule_form.html template
    return render(request, 'tax_processor/rule_form.html', context)

@user_passes_test(is_permitted_user)
@require_POST
def entity_rule_delete(request, rule_id, declaration_id=None):
    is_specific_rule = declaration_id is not None
    rule = None
    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(EntityTypeRule, pk=rule_id, declaration=declaration)
        list_url_name = 'entity_rule_list_specific'; url_kwargs = {'declaration_id': declaration_id}
    else:
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(EntityTypeRule, pk=rule_id, declaration__isnull=True)
        list_url_name = 'entity_rule_list_global'; url_kwargs = {}

    rule_name = rule.rule_name
    rule.delete()
    messages.success(request, f"Entity Rule '{rule_name}' successfully deleted.")
    return redirect(list_url_name, **url_kwargs)


# --- NEW: TransactionScopeRule Views ---

@user_passes_test(is_superadmin)
def scope_rule_list(request, declaration_id=None):
    """Lists global or specific TransactionScopeRules."""
    is_specific = declaration_id is not None
    declaration = None
    if is_specific:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        queryset = TransactionScopeRule.objects.filter(declaration=declaration)
        list_title = f"Scope Rules for {declaration.name}"
    else:
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        queryset = TransactionScopeRule.objects.filter(declaration__isnull=True)
        list_title = "Global Transaction Scope Rules"

    queryset = queryset.select_related('declaration', 'created_by')

    # --- Search, Filter, Sort, Paginate (re-using logic) ---
    search_query = request.GET.get('q', '').strip()
    if search_query:
        queryset = queryset.filter(rule_name__icontains=search_query)
    filter_active = request.GET.get('filter_active', '')
    if filter_active:
        queryset = queryset.filter(is_active=(filter_active == 'true'))
    sort_by = request.GET.get('sort', 'priority')
    valid_sort_fields = ['priority', '-priority', 'rule_name', '-rule_name', 'scope_result', '-scope_result', 'is_active', '-is_active', 'created_at', '-created_at']
    if sort_by not in valid_sort_fields: sort_by = 'priority'
    queryset = queryset.order_by(sort_by)
    paginator = Paginator(queryset, 25)
    page_number = request.GET.get('page')
    page_obj = paginator.get_page(page_number)
    get_params = request.GET.copy()
    if 'page' in get_params: del get_params['page']

    context = {
        'rules': page_obj, 'page_obj': page_obj, 'declaration': declaration,
        'is_global_list': not is_specific, 'list_title': list_title,
        'is_admin': is_superadmin(request.user), 'search_query': search_query,
        'filter_active': filter_active, 'current_sort': sort_by, 'get_params': get_params.urlencode(),
        'rule_type': 'scope' # For template URLs
    }
    # We'll create 'scope_rule_list.html' next
    return render(request, 'tax_processor/scope_rule_list.html', context)

@user_passes_test(is_superadmin)
def scope_rule_create_or_update(request, rule_id=None, declaration_id=None):
    """Creates or updates a TransactionScopeRule (global or specific)."""
    is_specific_rule = declaration_id is not None
    declaration = None
    rule = None
    title = ""

    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "Permission denied."); return redirect('user_dashboard')
        if rule_id:
             rule = get_object_or_404(TransactionScopeRule, pk=rule_id, declaration=declaration)
             title = f"Update Specific Scope Rule: {rule.rule_name}"
        else:
             title = f"Create New Scope Rule for {declaration.name}"
        list_url_name = 'scope_rule_list_specific'
        url_kwargs = {'declaration_id': declaration_id}
    else:
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        if rule_id:
            rule = get_object_or_404(TransactionScopeRule, pk=rule_id, declaration__isnull=True)
            title = f"Update Global Scope Rule: {rule.rule_name}"
        else:
            title = "Create New Global Scope Rule"
        list_url_name = 'scope_rule_list_global'
        url_kwargs = {}

    formset_prefix = 'conditions'
    if request.method == 'POST':
        form = TransactionScopeRuleForm(request.POST, instance=rule)
        formset = BaseConditionFormSet(request.POST, prefix=formset_prefix)
        if form.is_valid() and formset.is_valid():
            new_rule = form.save(commit=False)
            if not new_rule.pk: new_rule.created_by = request.user
            if is_specific_rule:
                new_rule.declaration = declaration
            else:
                 new_rule.declaration = None
            checks = []
            for check_form in formset.cleaned_data:
                if check_form and not check_form.get('DELETE'): checks.append({'field': check_form['field'], 'type': check_form['condition_type'], 'value': check_form['value']})
            new_rule.conditions_json = [{'logic': form.cleaned_data['logic'], 'checks': checks}]
            try:
                 new_rule.save()
                 messages.success(request, f"Scope Rule '{new_rule.rule_name}' saved successfully.")
                 return redirect(list_url_name, **url_kwargs)
            except IntegrityError:
                 messages.error(request, f"A scope rule named '{new_rule.rule_name}' already exists for this scope.")
                 # Re-render context
        # else:
             # Fall through to re-render context on invalid form

    else: # GET request
        initial_form_data = {}; initial_formset_data = []
        if rule:
            if rule.conditions_json and isinstance(rule.conditions_json, list) and len(rule.conditions_json) > 0 and rule.conditions_json[0]:
                logic_block = rule.conditions_json[0]; initial_form_data['logic'] = logic_block.get('logic', 'AND')
                for check in logic_block.get('checks', []): initial_formset_data.append({'field': check.get('field'), 'condition_type': check.get('type'), 'value': check.get('value')})
        else: initial_form_data['logic'] = 'AND'
        form = TransactionScopeRuleForm(instance=rule, initial=initial_form_data)
        formset = BaseConditionFormSet(initial=initial_formset_data, prefix=formset_prefix)

    context = {
        'form': form, 'formset': formset, 'title': title, 'rule': rule,
        'declaration': declaration, 'is_specific_rule': is_specific_rule,
        'list_url_name': list_url_name, 'url_kwargs': url_kwargs,
        'bank_names': BANK_NAMES_LIST, 'is_admin': is_superadmin(request.user),
        'rule_type': 'scope' # For template URLs
    }
    # Re-use the main rule_form.html template
    return render(request, 'tax_processor/rule_form.html', context)

@user_passes_test(is_permitted_user)
@require_POST
def scope_rule_delete(request, rule_id, declaration_id=None):
    is_specific_rule = declaration_id is not None
    rule = None
    if is_specific_rule:
        declaration = get_object_or_404(Declaration, pk=declaration_id)
        if not (is_superadmin(request.user) or declaration.created_by == request.user):
             messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(TransactionScopeRule, pk=rule_id, declaration=declaration)
        list_url_name = 'scope_rule_list_specific'; url_kwargs = {'declaration_id': declaration_id}
    else:
        if not is_superadmin(request.user):
            messages.error(request, "Permission denied."); return redirect('user_dashboard')
        rule = get_object_or_404(TransactionScopeRule, pk=rule_id, declaration__isnull=True)
        list_url_name = 'scope_rule_list_global'; url_kwargs = {}

    rule_name = rule.rule_name
    rule.delete()
    messages.success(request, f"Scope Rule '{rule_name}' successfully deleted.")
    return redirect(list_url_name, **url_kwargs)
