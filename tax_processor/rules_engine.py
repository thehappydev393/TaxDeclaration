# tax_processor/rules_engine.py

from django.db import transaction
from django.db.models import Q
from .models import TaxRule, Transaction, UnmatchedTransaction, User
from decimal import Decimal, InvalidOperation # Import InvalidOperation
import re
import json
from django.utils import timezone
import traceback # Added for better error logging

class RulesEngine:
    """
    Core engine that processes unassigned transactions against dynamic rules
    and populates the UnmatchedTransaction queue for review.
    """

    def __init__(self, declaration_id: int):
        self.declaration_id = declaration_id # Keep declaration_id for potential future use or logging

        # --- THIS IS THE FIX ---
        # Reverted to only load active global rules, as the 'declaration'
        # field doesn't exist on TaxRule yet.
        self.rules = list(TaxRule.objects.filter(is_active=True).order_by('priority', 'rule_name'))
        # --- END FIX ---

        # Removed self.unmatched_transactions and self.matched_count initialization here

    def _evaluate_condition(self, transaction: Transaction, condition: dict) -> bool:
        """Evaluates a single condition against a transaction field."""

        field = condition.get('field')
        condition_type = condition.get('type')
        value = condition.get('value')

        if not all([field, condition_type]) or value is None:
             print(f"   [Rule Engine Warning] Malformed condition skipped: {condition}")
             return False

        try:
            field_value = getattr(transaction, field, None)
        except AttributeError:
             print(f"   [Rule Engine Warning] Invalid field '{field}' in condition: {condition}")
             return False

        if field_value is None:
            return False

        str_field_value = str(field_value)
        str_value_lower = str(value).lower()

        try:
            # --- Text Comparisons ---
            if condition_type == 'CONTAINS_KEYWORD':
                keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]
                return any(kw in str_field_value.lower() for kw in keywords)

            elif condition_type == 'DOES_NOT_CONTAIN_KEYWORD':
                 keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]
                 return not any(kw in str_field_value.lower() for kw in keywords)

            elif condition_type == 'EQUALS':
                return str_field_value.strip().lower() == str_value_lower.strip()

            elif condition_type == 'REGEX_MATCH':
                return bool(re.search(str(value), str_field_value, re.IGNORECASE))


            # --- Numeric Comparisons (Only apply to 'amount' field) ---
            elif field == 'amount' and condition_type in [
                'GREATER_THAN', 'LESS_THAN',
                'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL',
                'RANGE_AMOUNT'
            ]:
                try:
                    tx_amount = Decimal(str(field_value))

                    if condition_type == 'GREATER_THAN':
                        num_value = Decimal(str(value)); return tx_amount > num_value
                    elif condition_type == 'LESS_THAN':
                        num_value = Decimal(str(value)); return tx_amount < num_value
                    elif condition_type == 'GREATER_THAN_OR_EQUAL':
                         num_value = Decimal(str(value)); return tx_amount >= num_value
                    elif condition_type == 'LESS_THAN_OR_EQUAL':
                         num_value = Decimal(str(value)); return tx_amount <= num_value
                    elif condition_type == 'RANGE_AMOUNT':
                        min_val_str, max_val_str = map(str.strip, str(value).split(','))
                        min_val = Decimal(min_val_str); max_val = Decimal(max_val_str)
                        return min_val <= tx_amount <= max_val

                except (InvalidOperation, ValueError, TypeError):
                    print(f"   [Rule Engine Warning] Invalid number for comparison in condition: {condition}, Tx Value: {field_value}")
                    return False

            # --- Handle cases where numeric condition applied to non-amount field ---
            elif condition_type in ['GREATER_THAN', 'LESS_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL', 'RANGE_AMOUNT'] and field != 'amount':
                 print(f"   [Rule Engine Warning] Numeric comparison '{condition_type}' used on non-amount field '{field}'. Condition skipped: {condition}")
                 return False

            # --- Default: Unrecognized condition type ---
            else:
                print(f"   [Rule Engine Warning] Unrecognized condition type '{condition_type}' in condition: {condition}")
                return False

        except Exception as e:
            print(f"   [Rule Engine Error] Unexpected error evaluating condition: {condition}. Error: {e}")
            traceback.print_exc()
            return False

    def _check_rule(self, transaction: Transaction, rule: TaxRule) -> bool:
        """Applies all logic blocks (AND/OR) within a single rule."""
        try:
            rule_conditions = rule.conditions_json
            if not isinstance(rule_conditions, list):
                 print(f"   [Rule Engine Warning] Rule '{rule.rule_name}' (ID: {rule.pk}) has invalid JSON structure (not a list). Skipping rule.")
                 return False
        except (TypeError, json.JSONDecodeError):
             print(f"   [Rule Engine Warning] Rule '{rule.rule_name}' (ID: {rule.pk}) has malformed JSON. Skipping rule.")
             return False

        if not rule_conditions: return False # Empty list means no match

        # Assume structure: [{"logic": "AND/OR", "checks": [...]}]
        # Currently only supports one logic block per rule via the UI
        logic_block = rule_conditions[0]
        logic = logic_block.get('logic', 'AND').upper()
        checks = logic_block.get('checks', [])

        if not checks: return False # Rule has no actual checks

        results = [self._evaluate_condition(transaction, check) for check in checks]

        if logic == 'AND': return all(results)
        elif logic == 'OR': return any(results)
        else:
             print(f"   [Rule Engine Warning] Rule '{rule.rule_name}' (ID: {rule.pk}) uses unrecognized logic '{logic}'. Defaulting to AND.")
             return all(results)


    @transaction.atomic
    def run_analysis(self, assigned_user: User):
        """
        Main function to run the rules engine against:
        1. New, unassigned transactions (NULL declaration_point).
        2. Existing unmatched transactions (re-evaluation).
        """
        print(f"--- Running Analysis for Declaration ID: {self.declaration_id} ---")
        print(f"   -> Using {len(self.rules)} active rules.") # Now only shows global rule count

        # 1. SELECT TRANSACTIONS FOR ANALYSIS (Unchanged)
        new_transactions_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            declaration_point__isnull=True
        ).select_related('statement')
        re_evaluate_tx_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            unmatched_record__status__in=['PENDING_REVIEW', 'NEW_RULE_PROPOSED']
        ).select_related('statement', 'unmatched_record')
        transactions_for_analysis = list(new_transactions_qs) + list(re_evaluate_tx_qs)
        transactions_for_analysis = list({tx.pk: tx for tx in transactions_for_analysis}.values())
        print(f"   -> Found {len(transactions_for_analysis)} transactions to analyze.")

        # --- (Rest of the run_analysis function remains unchanged) ---
        transactions_to_update = []; unmatched_records_to_clear = []; newly_unmatched_transactions = []
        matched_count = 0

        # 2. APPLY RULES
        for tx in transactions_for_analysis:
            is_matched = False
            original_status = 'NEW' if not hasattr(tx, 'unmatched_record') else tx.unmatched_record.status
            for rule in self.rules:
                if self._check_rule(tx, rule):
                    tx.matched_rule = rule; tx.declaration_point = rule.declaration_point
                    transactions_to_update.append(tx); matched_count += 1; is_matched = True
                    if hasattr(tx, 'unmatched_record'): unmatched_records_to_clear.append(tx.unmatched_record)
                    break # First Match Wins
            if not is_matched:
                if not hasattr(tx, 'unmatched_record'): newly_unmatched_transactions.append(tx)
                elif tx.unmatched_record.status != 'PENDING_REVIEW':
                     tx.unmatched_record.status = 'PENDING_REVIEW'
                     unmatched_records_to_clear.append(tx.unmatched_record) # Use this list to update status
                if tx.matched_rule is not None or tx.declaration_point is not None:
                     tx.matched_rule = None; tx.declaration_point = None
                     transactions_to_update.append(tx)

        # 3. BULK UPDATES
        if transactions_to_update:
            updated_count = Transaction.objects.bulk_update(transactions_to_update, ['matched_rule', 'declaration_point'])
            print(f"   -> Updated {updated_count} transactions in database.")

        # 4. UPDATE UNMATCHED QUEUE STATUS
        cleared_unmatched_count = 0
        if unmatched_records_to_clear:
             to_resolve = [um for um in unmatched_records_to_clear if um.transaction.declaration_point is not None]
             to_revert = [um for um in unmatched_records_to_clear if um.transaction.declaration_point is None and um.status != 'PENDING_REVIEW']
             if to_resolve:
                 for um in to_resolve: um.status = 'RESOLVED'; um.resolution_date = timezone.now()
                 UnmatchedTransaction.objects.bulk_update(to_resolve, ['status', 'resolution_date'])
                 cleared_unmatched_count = len(to_resolve)
                 print(f"   -> Marked {len(to_resolve)} previously unmatched items as RESOLVED.")
             if to_revert:
                 for um in to_revert: um.status = 'PENDING_REVIEW'
                 UnmatchedTransaction.objects.bulk_update(to_revert, ['status'])
                 print(f"   -> Reverted {len(to_revert)} proposed/other items back to PENDING_REVIEW.")

        # 5. CREATE NEW UNMATCHED QUEUE ITEMS
        new_unmatched_count = 0
        if newly_unmatched_transactions:
            unmatched_queue_objects = [UnmatchedTransaction(transaction=tx, assigned_user=assigned_user, status='PENDING_REVIEW') for tx in newly_unmatched_transactions]
            created_unmatched = UnmatchedTransaction.objects.bulk_create(unmatched_queue_objects)
            new_unmatched_count = len(created_unmatched)
            print(f"   -> Created {new_unmatched_count} new items in the unmatched queue.")

        print(f"--- Analysis Complete. Matched: {matched_count}, Newly Unmatched: {new_unmatched_count}, Cleared from Queue: {cleared_unmatched_count} ---")
        return matched_count, new_unmatched_count, cleared_unmatched_count
