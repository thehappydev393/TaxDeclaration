# tax_processor/context_processors.py

from .models import UnmatchedTransaction, TaxRule

def is_superadmin(user):
    """Helper function to check for superadmin."""
    return user.is_authenticated and hasattr(user, 'profile') and user.profile.role == 'SUPERADMIN'

def proposal_counts(request):
    """
    A context processor to add pending proposal counts to every template context.
    """
    pending_proposals_count = 0
    pending_global_rules_count = 0

    # Only run the queries if the user is a superadmin
    if is_superadmin(request.user):
        pending_proposals_count = UnmatchedTransaction.objects.filter(status='NEW_RULE_PROPOSED').count()
        pending_global_rules_count = TaxRule.objects.filter(proposal_status='PENDING_GLOBAL').count()

    return {
        'pending_proposals_count': pending_proposals_count,
        'pending_global_rules_count': pending_global_rules_count,
    }
