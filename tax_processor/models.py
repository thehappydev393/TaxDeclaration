# tax_processor/models.py

from django.db import models
from django.contrib.auth.models import User
from django.db.models import F # Used for self-referential priority check (optional but good practice)

# ====================================================================
# 1. USER AND ROLE MANAGEMENT
#    (No changes here)
# ====================================================================

class UserProfile(models.Model):
    """
    Extends the built-in Django User model to add a specific role field.
    """

    ROLE_CHOICES = (
        ('SUPERADMIN', 'Superadmin'),
        ('REGULAR_USER', 'Regular User'),
    )

    user = models.OneToOneField(
        User,
        on_delete=models.CASCADE,
        related_name='profile',
        verbose_name="System User"
    )
    role = models.CharField(
        max_length=50,
        choices=ROLE_CHOICES,
        default='REGULAR_USER',
        verbose_name="User Role"
    )

    def is_superadmin(self):
        """Helper method for permission checks."""
        return self.role == 'SUPERADMIN'

    def __str__(self):
        return f"{self.user.username} - {self.role}"

    class Meta:
        verbose_name = "User Profile"
        verbose_name_plural = "User Profiles"


# ====================================================================
# 2. DECLARATION ENTITY
#    (MODIFIED: Added first_name and last_name)
# ====================================================================

class Declaration(models.Model):
    """
    Defines a single tax filing entity (e.g., Q3 2025 for Client A).
    """

    created_by = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='created_declarations',
        verbose_name="Created By"
    )

    STATUS_CHOICES = (
        ('DRAFT', 'Draft'),
        ('ANALYSIS_COMPLETE', 'Analysis Complete'),
        ('FILED', 'Filed'),
    )

    name = models.CharField(max_length=255, unique=True, help_text="e.g., 2024 - Client A Հայտարարագիր")
    tax_period_start = models.DateField()
    tax_period_end = models.DateField()
    client_reference = models.CharField(max_length=100, blank=True, help_text="External Client ID or Business Name")

    # --- NEW FIELDS ---
    first_name = models.CharField(max_length=150, verbose_name="Client First Name", blank=True)
    last_name = models.CharField(max_length=150, verbose_name="Client Last Name", blank=True)
    # --- END NEW FIELDS ---

    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='DRAFT')

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Declaration Entity"
        verbose_name_plural = "Declaration Entities"


# ====================================================================
# 3. STATEMENT METADATA
#    (No changes here)
# ====================================================================

class Statement(models.Model):
    """
    Stores metadata for each uploaded source file (Excel or PDF).
    """

    declaration = models.ForeignKey(
        Declaration,
        on_delete=models.CASCADE,
        related_name='statements',
        verbose_name="Tax Declaration"
    )
    file_name = models.CharField(max_length=255)
    bank_name = models.CharField(max_length=100)
    upload_date = models.DateTimeField(auto_now_add=True)
    status = models.CharField(max_length=50, default='PROCESSED')

    def __str__(self):
        return f"[{self.declaration.name}] {self.file_name}"

    class Meta:
        verbose_name = "Statement File"
        verbose_name_plural = "Statement Files"
        ordering = ['-upload_date']

# ====================================================================
# 4. DECLARATION POINT
#    (MODIFIED: Added is_auto_filled)
# ====================================================================
class DeclarationPoint(models.Model):
    """
    Master data table for consistent tax declaration categories/points.
    """
    name = models.CharField(max_length=255, unique=True, verbose_name="Tax Category Name")
    description = models.TextField(blank=True, verbose_name="Notes/Tax Instruction")
    is_income = models.BooleanField(default=True, verbose_name="Is this an Income Category?")

    # --- NEW FIELD ---
    is_auto_filled = models.BooleanField(
        default=False,
        verbose_name="Automatically filled by tax authority",
        help_text="Check if this point is pre-filled by tax authorities and not based on transactions."
    )
    # --- END NEW FIELD ---

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Declaration Point (Category)"
        verbose_name_plural = "Declaration Points (Categories)"
        ordering = ['name']


# ====================================================================
# 5. TAX RULES
#    (No changes here - this is for the *main* categorization)
# ====================================================================

class TaxRule(models.Model):
    """
    Defines a single rule for routing transactions to a declaration point.
    Can be global (declaration=NULL) or specific to one declaration.
    """

    rule_name = models.CharField(max_length=255) # Consider removing unique=True if names can repeat across declarations
    priority = models.IntegerField(default=100, help_text="Lower number means higher priority (processed first).")
    declaration_point = models.ForeignKey(DeclarationPoint, on_delete=models.SET_NULL, null=True, help_text="The final tax category.")
    conditions_json = models.JSONField(help_text="JSON array defining field checks and logic.")

    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='created_rules')
    created_at = models.DateTimeField(auto_now_add=True)

    # --- Link to Declaration (Optional) ---
    declaration = models.ForeignKey(
        Declaration,
        on_delete=models.CASCADE, # If declaration is deleted, delete its specific rules
        null=True,                # Allow NULL for global rules
        blank=True,               # Allow empty in forms/admin
        related_name='specific_rules',
        verbose_name="Specific Declaration (if not global)"
    )

    # --- Proposal Status ---
    PROPOSAL_STATUS_CHOICES = (
        ('NONE', 'Not Proposed'),
        ('PENDING_GLOBAL', 'Pending Global Approval'),
    )
    proposal_status = models.CharField(
        max_length=20,
        choices=PROPOSAL_STATUS_CHOICES,
        default='NONE',
        verbose_name="Global Proposal Status"
    )


    def __str__(self):
        scope = f"Decl: {self.declaration.pk}" if self.declaration else "Global"
        return f"P{self.priority}: {self.rule_name} ({scope})"

    class Meta:
        verbose_name = "Tax Rule (Category)"
        verbose_name_plural = "Tax Rules (Category)"
        # Ensure uniqueness for global rules OR within a specific declaration
        unique_together = ('declaration', 'rule_name') # Rule names must be unique within a declaration or globally
        ordering = ['priority', 'rule_name']


# ====================================================================
# 6. TRANSACTION DATA
#    (MODIFIED: Added entity_type and transaction_scope)
# ====================================================================

class Transaction(models.Model):
    """
    Universal table storing all incoming financial transactions.
    """

    # --- NEW: Choices for new fields ---
    ENTITY_CHOICES = (
        ('UNDETERMINED', 'Not Determined'),
        ('INDIVIDUAL', 'Individual (Not Legal Entity)'),
        ('LEGAL', 'Legal Entity'),
    )

    SCOPE_CHOICES = (
        ('UNDETERMINED', 'Not Determined'),
        ('LOCAL', 'Local'),
        ('INTERNATIONAL', 'International'),
    )
    # --- END NEW ---

    statement = models.ForeignKey(Statement, on_delete=models.CASCADE, related_name='transactions')

    # Core Data
    transaction_date = models.DateTimeField()
    provision_date = models.DateTimeField(null=True, blank=True)
    amount = models.DecimalField(max_digits=10, decimal_places=2)
    currency = models.CharField(max_length=10)
    description = models.TextField()
    sender = models.CharField(max_length=255)
    sender_account = models.CharField(max_length=50, blank=True, null=True)

    # Analysis Result
    matched_rule = models.ForeignKey(TaxRule, on_delete=models.SET_NULL, null=True, blank=True, related_name='matched_transactions')
    declaration_point = models.ForeignKey(
        DeclarationPoint,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        help_text="The tax category assigned by a rule or manual resolution."
    )

    # --- NEW FIELDS ---
    entity_type = models.CharField(
        max_length=20,
        choices=ENTITY_CHOICES,
        default='UNDETERMINED',
        verbose_name="Entity Type"
    )
    transaction_scope = models.CharField(
        max_length=20,
        choices=SCOPE_CHOICES,
        default='UNDETERMINED',
        verbose_name="Transaction Scope"
    )
    # --- END NEW FIELDS ---

    def __str__(self):
        return f"{self.transaction_date.date()} - {self.amount} {self.currency}"

    class Meta:
        verbose_name = "Transaction Record"
        verbose_name_plural = "Transaction Records"
        ordering = ['-transaction_date']


# ====================================================================
# 7. UNMATCHED TRANSACTIONS
#    (No changes here)
# ====================================================================

class UnmatchedTransaction(models.Model):
    """
    Transactions that failed to match any rule, requiring user intervention.
    """

    transaction = models.OneToOneField(
        Transaction,
        on_delete=models.CASCADE,
        related_name='unmatched_record',
        verbose_name="Original Transaction"
    ) # Links directly to the unmatched row

    STATUS_CHOICES = (
        ('PENDING_REVIEW', 'Pending Review'),
        ('RESOLVED', 'Resolved'),
        ('NEW_RULE_PROPOSED', 'New Rule Proposed'),
    )
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='PENDING_REVIEW')

    assigned_user = models.ForeignKey(
        User,
        on_delete=models.SET_NULL,
        null=True,
        related_name='assigned_reviews',
        verbose_name="Assigned Reviewer"
    )
    resolution_date = models.DateTimeField(null=True, blank=True)
    resolved_point = models.CharField(max_length=255, blank=True, null=True, help_text="Final category chosen by the reviewer.")
    rule_proposal_json = models.JSONField(null=True, blank=True, help_text="Suggested rule structure based on manual resolution.")

    def __str__(self):
        return f"Review #{self.pk}: {self.transaction}"

    class Meta:
        verbose_name = "Unmatched Transaction"
        verbose_name_plural = "Unmatched Transactions"
        ordering = ['status']


# ====================================================================
# 8. NEW RULE MODELS
#    (NEW: As requested for separate rule engines)
# ====================================================================

class EntityTypeRule(models.Model):
    """
    Defines a rule for flagging a transaction's Entity Type (Legal/Individual).
    """
    rule_name = models.CharField(max_length=255)
    priority = models.IntegerField(default=100, help_text="Lower number means higher priority.")

    # The result this rule will apply
    entity_type_result = models.CharField(
        max_length=20,
        choices=Transaction.ENTITY_CHOICES,
        default='UNDETERMINED',
        verbose_name="Resulting Entity Type"
    )

    conditions_json = models.JSONField(help_text="JSON array defining field checks and logic.")
    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='created_entity_rules')
    created_at = models.DateTimeField(auto_now_add=True)

    declaration = models.ForeignKey(
        Declaration,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='specific_entity_rules',
        verbose_name="Specific Declaration (if not global)"
    )

    def __str__(self):
        scope = f"Decl: {self.declaration.pk}" if self.declaration else "Global"
        return f"P{self.priority}: {self.rule_name} -> {self.entity_type_result} ({scope})"

    class Meta:
        verbose_name = "Rule (Entity Type)"
        verbose_name_plural = "Rules (Entity Type)"
        unique_together = ('declaration', 'rule_name')
        ordering = ['priority', 'rule_name']


class TransactionScopeRule(models.Model):
    """
    Defines a rule for flagging a transaction's Scope (Local/International).
    """
    rule_name = models.CharField(max_length=255)
    priority = models.IntegerField(default=100, help_text="Lower number means higher priority.")

    # The result this rule will apply
    scope_result = models.CharField(
        max_length=20,
        choices=Transaction.SCOPE_CHOICES,
        default='UNDETERMINED',
        verbose_name="Resulting Scope"
    )

    conditions_json = models.JSONField(help_text="JSON array defining field checks and logic.")
    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='created_scope_rules')
    created_at = models.DateTimeField(auto_now_add=True)

    declaration = models.ForeignKey(
        Declaration,
        on_delete=models.CASCADE,
        null=True,
        blank=True,
        related_name='specific_scope_rules',
        verbose_name="Specific Declaration (if not global)"
    )

    def __str__(self):
        scope = f"Decl: {self.declaration.pk}" if self.declaration else "Global"
        return f"P{self.priority}: {self.rule_name} -> {self.scope_result} ({scope})"

    class Meta:
        verbose_name = "Rule (Transaction Scope)"
        verbose_name_plural = "Rules (Transaction Scope)"
        unique_together = ('declaration', 'rule_name')
        ordering = ['priority', 'rule_name']
