from django.contrib import admin
from django.contrib.auth.admin import UserAdmin as BaseUserAdmin
from django.contrib.auth.models import User
from django.utils.html import format_html
from django.urls import reverse
from .models import (
    UserProfile, Declaration, Statement, TaxRule,
    Transaction, UnmatchedTransaction
)

# -----------------------------------------------------------
# 1. User/Role Admin Setup
# -----------------------------------------------------------

class UserProfileInline(admin.StackedInline):
    """Inline for UserProfile within the standard User admin."""
    model = UserProfile
    can_delete = False
    fields = ('role',)

class UserAdmin(BaseUserAdmin):
    """
    Custom User admin to include UserProfile inline and auto-create the profile
    when a user is added via the standard Admin form.
    """
    inlines = (UserProfileInline,)
    list_display = BaseUserAdmin.list_display + ('get_role',)

    def get_role(self, obj):
        """Displays the custom role in the user list."""
        try:
            return obj.profile.role
        except UserProfile.DoesNotExist:
            return 'N/A'
    get_role.short_description = 'Role'

    # CRITICAL FIX: Override save_model to ensure UserProfile creation
    def save_model(self, request, obj, form, change):
        """
        Calls the standard save logic and then ensures a UserProfile exists.
        """
        super().save_model(request, obj, form, change)

        # Ensures the profile is created if the user was just created or if
        # the profile was missing during an update.
        UserProfile.objects.get_or_create(user=obj)

    # NOTE: The UserProfile role field is editable via the inline form itself.

# Re-register User to use the custom UserAdmin
admin.site.unregister(User)
admin.site.register(User, UserAdmin)

# -----------------------------------------------------------
# 2. Declaration and Statement Administration
# -----------------------------------------------------------

class StatementInline(admin.TabularInline):
    """Inline for Statements within the Declaration detail view."""
    model = Statement
    extra = 0
    fields = ('file_name', 'bank_name', 'upload_date', 'status')
    readonly_fields = ('file_name', 'bank_name', 'upload_date', 'status')
    show_change_link = True
    verbose_name = 'Uploaded Statement'
    verbose_name_plural = 'Uploaded Statements'


@admin.register(Declaration)
class DeclarationAdmin(admin.ModelAdmin):
    list_display = ('name', 'tax_period_start', 'tax_period_end', 'status', 'run_analysis_action')
    list_filter = ('status',)
    search_fields = ('name', 'client_reference')
    inlines = [StatementInline] # Show associated statements on the detail page

    # Method to count unassigned transactions and link to the detail/analysis view
    def run_analysis_action(self, obj):
        # Safely count the number of unassigned transactions
        unassigned_count = Transaction.objects.filter(
            statement__declaration=obj,
            declaration_point__isnull=True
        ).count()

        # Link to the detail view (which contains the actual 'Run Analysis' button)
        url = reverse('declaration_detail', args=[obj.pk])

        # Display the count and link
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
# -----------------------------------------------------------

@admin.register(Transaction)
class TransactionAdmin(admin.ModelAdmin):
    list_display = ('__str__', 'statement', 'declaration_point', 'matched_rule', 'amount')
    list_filter = ('declaration_point', 'currency', 'statement__bank_name')
    search_fields = ('description', 'sender', 'sender_account')
    # Fields that should not be edited manually after import
    readonly_fields = ('statement', 'transaction_date', 'amount', 'currency', 'description', 'sender', 'sender_account', 'matched_rule')

@admin.register(TaxRule)
class TaxRuleAdmin(admin.ModelAdmin):
    list_display = ('rule_name', 'priority', 'declaration_point', 'is_active', 'created_by', 'created_at')
    list_filter = ('is_active', 'declaration_point', 'priority')
    search_fields = ('rule_name', 'declaration_point')
    ordering = ('priority',)
    # Use fieldsets for better organization
    fieldsets = (
        (None, {
            'fields': ('rule_name', 'priority', 'declaration_point', 'conditions_json', 'is_active'),
        }),
        ('Audit', {
            'fields': ('created_by', 'created_at'),
            'classes': ('collapse',),
        }),
    )
    # Automatically set the creator and timestamp
    def save_model(self, request, obj, form, change):
        if not obj.pk:
            # Ensure created_by is set to the current Superuser on creation
            obj.created_by = request.user
        super().save_model(request, obj, form, change)

@admin.register(UnmatchedTransaction)
class UnmatchedTransactionAdmin(admin.ModelAdmin):
    list_display = ('transaction', 'status', 'assigned_user', 'resolved_point', 'resolution_date')
    list_filter = ('status', 'assigned_user')
    search_fields = ('transaction__description', 'transaction__sender')
    # Use raw_id_fields for FKs to prevent long dropdowns
    raw_id_fields = ('transaction', 'assigned_user')
