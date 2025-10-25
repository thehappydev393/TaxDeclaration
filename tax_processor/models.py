# tax_processor/models.py

from django.db import models
from django.contrib.auth.models import User
from django.db.models import F # Used for self-referential priority check (optional but good practice)

# ====================================================================
# 1. USER AND ROLE MANAGEMENT
#    We extend Django's built-in User model for custom roles.
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
#    The top-level grouping for a tax filing period/client.
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
    status = models.CharField(max_length=50, choices=STATUS_CHOICES, default='DRAFT')

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Declaration Entity"
        verbose_name_plural = "Declaration Entities"


# ====================================================================
# 3. STATEMENT METADATA
#    Tracks individual files, linked to a Declaration.
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

class DeclarationPoint(models.Model):
    """
    Master data table for consistent tax declaration categories/points.
    """
    name = models.CharField(max_length=255, unique=True, verbose_name="Tax Category Name")
    description = models.TextField(blank=True, verbose_name="Notes/Tax Instruction")
    is_income = models.BooleanField(default=True, verbose_name="Is this an Income Category?")

    def __str__(self):
        return self.name

    class Meta:
        verbose_name = "Declaration Point (Category)"
        verbose_name_plural = "Declaration Points (Categories)"
        ordering = ['name']


# ====================================================================
# 4. TAX RULES
#    Stores the dynamic conditions for automated matching.
# ====================================================================

class TaxRule(models.Model):
    """
    Defines a single rule for routing transactions to a declaration point.
    """

    rule_name = models.CharField(max_length=255, unique=True)
    priority = models.IntegerField(
        default=100,
        help_text="Lower number means higher priority (processed first)."
    )
    declaration_point = models.ForeignKey( # CHANGED
        DeclarationPoint,                  # CHANGED
        on_delete=models.SET_NULL,         # CHANGED
        null=True,                         # CHANGED
        help_text="The final tax category."
    )
    # Stores the complex conditions (JSON array structure)
    conditions_json = models.JSONField(help_text="JSON array defining field checks and logic.")

    is_active = models.BooleanField(default=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, related_name='created_rules')
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self):
        return f"P{self.priority}: {self.rule_name}"

    class Meta:
        verbose_name = "Tax Rule"
        verbose_name_plural = "Tax Rules"
        ordering = ['priority', 'rule_name']


# ====================================================================
# 5. TRANSACTION DATA
#    Stores every incoming transaction record.
# ====================================================================

class Transaction(models.Model):
    """
    Universal table storing all incoming financial transactions.
    """

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
    declaration_point = models.ForeignKey( # CHANGED
        DeclarationPoint,                  # CHANGED
        on_delete=models.SET_NULL,         # CHANGED
        null=True,                         # CHANGED
        blank=True,
        help_text="The tax category assigned by a rule or manual resolution."
    )

    def __str__(self):
        return f"{self.transaction_date.date()} - {self.amount} {self.currency}"

    class Meta:
        verbose_name = "Transaction Record"
        verbose_name_plural = "Transaction Records"
        ordering = ['-transaction_date']


# ====================================================================
# 6. UNMATCHED TRANSACTIONS
#    Queue for manual user review and system learning.
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
