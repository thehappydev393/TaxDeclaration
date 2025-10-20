# tax_processor/rules_engine.py

from django.db import transaction
from django.db.models import Q
from .models import TaxRule, Transaction, UnmatchedTransaction, User
from decimal import Decimal
import re
import json
from django.utils import timezone

class RulesEngine:
    """
    Core engine that processes unassigned transactions against dynamic rules
    and populates the UnmatchedTransaction queue for review.
    """

    def __init__(self, declaration_id: int):
        self.declaration_id = declaration_id
        # Load all active rules, sorted by priority (lowest first)
        # We rely on the TaxRule model's Meta ordering: ['priority', 'rule_name']
        self.rules = list(TaxRule.objects.filter(is_active=True).order_by('priority'))
        self.unmatched_transactions = []
        self.matched_count = 0

    def _evaluate_condition(self, transaction: Transaction, condition: dict) -> bool:
        """Evaluates a single condition against a transaction field."""

        field = condition.get('field')
        condition_type = condition.get('type')
        value = condition.get('value')

        # Guard against malformed rules
        if not all([field, condition_type, value]):
            return False

        # Get the transaction field value (e.g., transaction.description)
        field_value = getattr(transaction, field, None)

        if field_value is None:
            # Cannot match if the transaction field is null
            return False

        # Convert value to string for consistent searching
        str_value = str(field_value)

        try:
            if condition_type == 'CONTAINS_KEYWORD':
                # Splits comma-separated keywords and checks if the field contains any of them
                keywords = [kw.strip().lower() for kw in str(value).split(',') if kw.strip()]
                return any(kw in str_value.lower() for kw in keywords)

            elif condition_type == 'EQUALS':
                return str_value.strip().lower() == str(value).strip().lower()

            elif condition_type == 'REGEX_MATCH':
                # Performs a case-insensitive regular expression search
                return bool(re.search(str(value), str_value, re.IGNORECASE))

            elif condition_type in ['GREATER_THAN', 'LESS_THAN', 'RANGE_AMOUNT']:
                # --- Numeric comparisons (always use transaction.amount for amount checks) ---

                # We cast the comparison value, and use the stored Decimal amount for the check
                num_value = Decimal(str(value))
                tx_amount = transaction.amount

                if condition_type == 'GREATER_THAN':
                    return tx_amount > num_value
                elif condition_type == 'LESS_THAN':
                    return tx_amount < num_value
                elif condition_type == 'RANGE_AMOUNT':
                    # Value format expected: "100.00, 500.00"
                    min_val, max_val = map(Decimal, str(value).split(','))
                    return min_val <= tx_amount <= max_val

            return False # Unrecognized condition type

        except Exception:
            # Handle conversion or regex errors safely (e.g., Decimal conversion failed)
            return False

    def _check_rule(self, transaction: Transaction, rule: TaxRule) -> bool:
        """Applies all logic blocks (AND/OR) within a single rule."""

        try:
            rule_conditions = rule.conditions_json
        except (TypeError, json.JSONDecodeError):
            # Rule has malformed JSON, skip it
            return False

        # If rule_conditions is empty, the rule should not match anything
        if not rule_conditions:
            return False

        # Each element in rule_conditions is a logic block (AND/OR)
        for logic_block in rule_conditions:
            logic = logic_block.get('logic', 'AND').upper()
            checks = logic_block.get('checks', [])

            if not checks: continue

            # Evaluate all individual checks in this block
            results = [self._evaluate_condition(transaction, check) for check in checks]

            if logic == 'AND':
                # If any check in an AND block fails, the rule is definitively NOT a match.
                if not all(results):
                    return False

            elif logic == 'OR':
                # If any check in an OR block succeeds, the rule IS a match.
                if any(results):
                    return True

        # If the function reaches this point, it means:
        # 1. All blocks were implicitly AND blocks (or blocks with logic 'AND').
        # 2. OR blocks were never triggered.
        # Since we only return False if an AND block failed, if we successfully
        # checked all blocks without returning False, the rule is a match.
        return True


    @transaction.atomic
    def run_analysis(self, assigned_user: User):
        """
        Main function to run the rules engine against:
        1. New, unassigned transactions (NULL declaration_point).
        2. Existing unmatched transactions (re-evaluation).
        """

        # 1. SELECT ALL TRANSACTIONS THAT NEED RE-EVALUATION

        # a) Newly imported transactions (never assigned a point)
        new_transactions_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            declaration_point__isnull=True
        ).select_related('statement')

        # b) Transactions currently in the review queue (needs re-evaluation against new rules)
        # We target the transactions linked to PENDING or PROPOSED unmatched records.
        re_evaluate_tx_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            unmatched_record__status__in=['PENDING_REVIEW', 'NEW_RULE_PROPOSED']
        ).select_related('statement', 'unmatched_record')

        # Combine the two QuerySets into a set of unique Transaction objects
        # We need to get the actual objects since QuerySets cannot be easily merged if they aren't unionable.
        transactions_for_analysis = list(set(list(new_transactions_qs) + list(re_evaluate_tx_qs)))

        transactions_to_update = []
        unmatched_records_to_clear = [] # Tracks UnmatchedTransaction objects that are now matched
        self.matched_count = 0
        self.unmatched_transactions = []


        # 2. APPLY RULES AND TAG TRANSACTIONS

        for tx in transactions_for_analysis:
            is_matched = False
            for rule in self.rules:
                if self._check_rule(tx, rule):
                    # Found a match!
                    tx.matched_rule = rule
                    tx.declaration_point = rule.declaration_point
                    transactions_to_update.append(tx)
                    self.matched_count += 1
                    is_matched = True

                    # If this transaction was sitting in the unmatched queue, mark it for clearance
                    if hasattr(tx, 'unmatched_record'):
                        unmatched_records_to_clear.append(tx.unmatched_record)

                    break # First Match Wins

            if not is_matched and not hasattr(tx, 'unmatched_record'):
                # Only add to the unmatched list if it failed all rules AND isn't already queued
                self.unmatched_transactions.append(tx)


        # 3. BULK DATABASE UPDATES (Matched Transactions)

        if transactions_to_update:
            Transaction.objects.bulk_update(
                transactions_to_update,
                ['matched_rule', 'declaration_point']
            )


        # 4. UPDATE UNMATCHED QUEUE STATUS (Clears resolved items)

        # Clear items that were previously unmatched but are now matched by a new rule
        cleared_unmatched_count = 0
        if unmatched_records_to_clear:
            for unmatched_record in unmatched_records_to_clear:
                unmatched_record.status = 'RESOLVED'
                unmatched_record.resolution_date = timezone.now()

            UnmatchedTransaction.objects.bulk_update(
                unmatched_records_to_clear,
                ['status', 'resolution_date']
            )
            cleared_unmatched_count = len(unmatched_records_to_clear)


        # 5. POPULATE NEW UNMATCHED QUEUE ITEMS (Items that failed and aren't already queued)

        unmatched_queue_objects = []
        for tx in self.unmatched_transactions:
            # We already filtered in step 2, so these transactions need a new UnmatchedTransaction record
            unmatched_queue_objects.append(
                UnmatchedTransaction(
                    transaction=tx,
                    assigned_user=assigned_user,
                    status='PENDING_REVIEW'
                )
            )

        UnmatchedTransaction.objects.bulk_create(unmatched_queue_objects)

        return self.matched_count, len(unmatched_queue_objects), cleared_unmatched_count
