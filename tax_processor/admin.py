# tax_processor/admin.py

from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from django.utils.html import format_html
from django.urls import reverse
from .models import (
    UserProfile, Declaration, Statement, TaxRule,
    Transaction, UnmatchedTransaction, DeclarationPoint,
    EntityTypeRule, TransactionScopeRule # NEW: Import new models
)

# -----------------------------------------------------------
# 1. User/Role Admin Setup
#    (No changes here)
# -----------------------------------------------------------

class UserProfileInline(admin.StackedInline):
    model = UserProfile
    can_delete = False
    fields = ('role',)

class UserAdmin(BaseUserAdmin):
    inlines = (UserProfileInline,)
    list_display = BaseUserAdmin.list_display + ('get_role',)

    def get_role(self, obj):
        try:
            return obj.profile.role
        except UserProfile.DoesNotExist:
            return 'N/A'
    get_role.short_description = 'Role'

    def save_related(self, request, form, formsets, change):
        super().save_related(request, form, formsets, change)
        user = form.instance
        UserProfile.objects.get_or_create(user=user)

admin.site.unregister(User)
admin.site.register(User, UserAdmin)

# -----------------------------------------------------------
# 2. Declaration and Statement Administration
#    (MODIFIED: DeclarationAdmin)
# -----------------------------------------------------------

class StatementInline(admin.TabularInline):
    model = Statement
    extra = 0
    fields = ('file_name', 'bank_name', 'upload_date', 'status')
    readonly_fields = ('file_name', 'bank_name', 'upload_date', 'status')
    show_change_link = True
    verbose_name = 'Uploaded Statement'
    verbose_name_plural = 'Uploaded Statements'


@admin.register(Declaration)
class DeclarationAdmin(admin.ModelAdmin):
    # NEW: Added first_name, last_name
    list_display = ('name', 'first_name', 'last_name', 'tax_period_start', 'tax_period_end', 'status', 'run_analysis_action')
    list_filter = ('status',)
    search_fields = ('name', 'client_reference', 'first_name', 'last_name') # NEW: Added search fields
    inlines = [StatementInline]

    # NEW: Added fields to fieldset
    fieldsets = (
        (None, {
            'fields': ('name', 'client_reference', 'first_name', 'last_name')
        }),
        ('Period and Status', {
            'fields': ('tax_period_start', 'tax_period_end', 'status')
        }),
        ('Ownership', {
            'fields': ('created_by',),
        }),
    )
    readonly_fields = ('created_by',)

    def save_model(self, request, obj, form, change):
        if not obj.pk:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

    # Method to count unassigned transactions and link to the detail/analysis view
    def run_analysis_action(self, obj):
        # (No change to this method)
        unassigned_count = Transaction.objects.filter(
            statement__declaration=obj,
            declaration_point__isnull=True
        ).count()
        url = reverse('declaration_detail', args=[obj.pk])
        return format_html(
            '<a href="{}" style="background-color: #007bff; color: white; padding: 5px 10px; text-decoration: none; border-radius: 3px;">Analyze ({})</a>',
            url,
            unassigned_count
        )
    run_analysis_action.short_description = 'Analysis (Unassigned)'


@admin.register(Statement)
class StatementAdmin(admin.ModelAdmin):
    list_display = ('file_name', 'declaration', 'bank_name', 'upload_date', 'status')
    list_filter = ('bank_name', 'status', 'declaration')
    search_fields = ('file_name', 'bank_name')
    readonly_fields = ('declaration', 'file_name', 'bank_name', 'upload_date')


# -----------------------------------------------------------
# 3. Transaction and Rules Administration
#    (MODIFIED: DeclarationPointAdmin, TransactionAdmin)
#    (NEW: EntityTypeRuleAdmin, TransactionScopeRuleAdmin)
# -----------------------------------------------------------

@admin.register(DeclarationPoint)
class DeclarationPointAdmin(admin.ModelAdmin):
    # NEW: Added is_auto_filled
    list_display = ('name', 'is_income', 'is_auto_filled', 'description')
    list_filter = ('is_income', 'is_auto_filled') # NEW: Added filter
    search_fields = ('name',)
    fields = ('name', 'is_income', 'is_auto_filled', 'description') # NEW: Added field


@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    # NEW: Added entity_type, transaction_scope
    list_display = ('__str__', 'statement', 'declaration_point', 'entity_type', 'transaction_scope', 'matched_rule', 'amount')
    list_filter = ('declaration_point', 'currency', 'statement__bank_name', 'entity_type', 'transaction_scope') # NEW: Added filters
    search_fields = ('description', 'sender', 'sender_account')
    readonly_fields = ('statement', 'transaction_date', 'amount', 'currency', 'description', 'sender', 'sender_account', 'matched_rule')
    # NEW: Added new fields to fieldset for editing
    fieldsets = (
        ('Core Info', {
            'fields': ('statement', 'transaction_date', 'amount', 'currency', 'description', 'sender', 'sender_account')
        }),
        ('Categorization', {
            'fields': ('declaration_point', 'matched_rule', 'entity_type', 'transaction_scope')
        }),
    )


@admin.register(TaxRule)
class TaxRuleAdmin(admin.ModelAdmin):
    # (No changes here, just for context)
    list_display = ('rule_name', 'priority', 'declaration_point', 'is_active', 'created_by', 'created_at', 'declaration')
    list_filter = ('is_active', 'declaration_point', 'priority', 'declaration')
    search_fields = ('rule_name', 'declaration_point__name')
    ordering = ('priority',)
    readonly_fields = ('created_by', 'created_at')
    fieldsets = (
        (None, {
            'fields': ('rule_name', 'priority', 'declaration_point', 'conditions_json', 'is_active', 'declaration'),
        }),
        ('Audit', {
            'fields': ('created_by', 'created_at'),
            'classes': ('collapse',),
        }),
    )
    def save_model(self, request, obj, form, change):
        if not obj.pk:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

# --- NEW: Admin registration for new rule models ---
class BaseRuleAdmin(admin.ModelAdmin):
    """Base class for new rule admins to reduce repetition."""
    list_display = ('rule_name', 'priority', 'is_active', 'created_by', 'created_at', 'declaration')
    list_filter = ('is_active', 'priority', 'declaration')
    search_fields = ('rule_name',)
    ordering = ('priority',)
    readonly_fields = ('created_by', 'created_at')

    def save_model(self, request, obj, form, change):
        if not obj.pk:
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

@admin.register(EntityTypeRule)
class EntityTypeRuleAdmin(BaseRuleAdmin):
    list_display = BaseRuleAdmin.list_display + ('entity_type_result',)
    list_filter = BaseRuleAdmin.list_filter + ('entity_type_result',)
    fieldsets = (
        (None, {
            'fields': ('rule_name', 'priority', 'entity_type_result', 'conditions_json', 'is_active', 'declaration'),
        }),
        ('Audit', {
            'fields': ('created_by', 'created_at'),
            'classes': ('collapse',),
        }),
    )

@admin.register(TransactionScopeRule)
class TransactionScopeRuleAdmin(BaseRuleAdmin):
    list_display = BaseRuleAdmin.list_display + ('scope_result',)
    list_filter = BaseRuleAdmin.list_filter + ('scope_result',)
    fieldsets = (
        (None, {
            'fields': ('rule_name', 'priority', 'scope_result', 'conditions_json', 'is_active', 'declaration'),
        }),
        ('Audit', {
            'fields': ('created_by', 'created_at'),
            'classes': ('collapse',),
        }),
    )
# --- END NEW ---

@admin.register(UnmatchedTransaction)
class UnmatchedTransactionAdmin(admin.ModelAdmin):
    list_display = ('transaction', 'status', 'assigned_user', 'resolved_point', 'resolution_date')
    list_filter = ('status', 'assigned_user')
    search_fields = ('transaction__description', 'transaction__sender')
    raw_id_fields = ('transaction', 'assigned_user')
