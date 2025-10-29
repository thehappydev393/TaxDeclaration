# tax_processor/rules_engine.py

from django.db import transaction
from django.db.models import Q
from .models import TaxRule, Transaction, UnmatchedTransaction, User # Make sure Declaration is imported if needed, though not directly used here
from decimal import Decimal, InvalidOperation
import re
import json
from django.utils import timezone
import traceback

class RulesEngine:
    """
    Core engine that processes unassigned transactions against dynamic rules
    (both global and declaration-specific) and populates the
    UnmatchedTransaction queue for review.
    """

    def __init__(self, declaration_id: int):
        self.declaration_id = declaration_id # Store declaration_id

        # --- THIS IS THE UPDATE ---
        # 1. Query active global rules (declaration is NULL)
        global_rules_qs = TaxRule.objects.filter(
            is_active=True,
            declaration__isnull=True
        )

        # 2. Query active rules specific to this declaration
        specific_rules_qs = TaxRule.objects.filter(
            is_active=True,
            declaration_id=self.declaration_id # Filter by the provided declaration_id
        )

        # 3. Combine the querysets
        # Using the | operator combines them with OR logic.
        # Ensure distinct results if a rule somehow got duplicated (though unique_together should prevent this)
        combined_rules_qs = global_rules_qs | specific_rules_qs

        # 4. Order the combined list by priority, then rule name
        self.rules = list(combined_rules_qs.order_by('priority', 'rule_name').distinct())
        # --- END UPDATE ---

        print(f"   [Rule Engine Init] Loaded {len(self.rules)} active rules (global + specific for Decl ID {self.declaration_id}).")


    def _evaluate_condition(self, transaction: Transaction, condition: dict) -> bool:
        """Evaluates a single condition against a transaction field."""
        # ... (Keep the existing _evaluate_condition method with new conditions) ...
        field = condition.get('field')
        condition_type = condition.get('type')
        value = condition.get('value')

        if not all([field, condition_type]) or value is None:
             print(f"   [Rule Engine Warning] Malformed condition skipped: {condition}")
             return False
        field_value = None
        try:
            # Handle direct fields and related fields (like statement__bank_name)
            if '__' in field:
                # Follow relationship (e.g., transaction.statement.bank_name)
                related_parts = field.split('__')
                obj = transaction
                for part in related_parts:
                    if obj is None: # Break if relationship is null mid-chain
                        field_value = None
                        break
                    obj = getattr(obj, part, None)
                field_value = obj # Final value after traversing
            else:
                # Direct attribute
                field_value = getattr(transaction, field, None)

        except AttributeError:
             print(f"   [Rule Engine Warning] Invalid field or relationship '{field}' in condition: {condition}")
             return False

        if field_value is None: return False

        str_field_value = str(field_value)
        str_value_lower = str(value).lower()

        try:
            if condition_type == 'CONTAINS_KEYWORD':
                keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]; return any(kw in str_field_value.lower() for kw in keywords)
            elif condition_type == 'DOES_NOT_CONTAIN_KEYWORD':
                 keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]; return not any(kw in str_field_value.lower() for kw in keywords)
            elif condition_type == 'EQUALS': return str_field_value.strip().lower() == str_value_lower.strip()
            elif condition_type == 'REGEX_MATCH': return bool(re.search(str(value), str_field_value, re.IGNORECASE))
            elif field == 'amount' and condition_type in ['GREATER_THAN', 'LESS_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL', 'RANGE_AMOUNT']:
                try:
                    tx_amount = Decimal(str(field_value))
                    if condition_type == 'GREATER_THAN': num_value = Decimal(str(value)); return tx_amount > num_value
                    elif condition_type == 'LESS_THAN': num_value = Decimal(str(value)); return tx_amount < num_value
                    elif condition_type == 'GREATER_THAN_OR_EQUAL': num_value = Decimal(str(value)); return tx_amount >= num_value
                    elif condition_type == 'LESS_THAN_OR_EQUAL': num_value = Decimal(str(value)); return tx_amount <= num_value
                    elif condition_type == 'RANGE_AMOUNT':
                        min_val_str, max_val_str = map(str.strip, str(value).split(','))
                        min_val = Decimal(min_val_str); max_val = Decimal(max_val_str); return min_val <= tx_amount <= max_val
                except (InvalidOperation, ValueError, TypeError): print(f"   [Rule Engine Warning] Invalid number for comparison: {condition}, Tx Value: {field_value}"); return False
            elif condition_type in ['GREATER_THAN', 'LESS_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL', 'RANGE_AMOUNT'] and field != 'amount':
                 print(f"   [Rule Engine Warning] Numeric comparison '{condition_type}' on non-amount field '{field}'. Skipped: {condition}"); return False
            else: print(f"   [Rule Engine Warning] Unrecognized condition type '{condition_type}': {condition}"); return False
        except Exception as e: print(f"   [Rule Engine Error] Unexpected error evaluating condition: {condition}. Error: {e}"); traceback.print_exc(); return False


    def _check_rule(self, transaction: Transaction, rule: TaxRule) -> bool:
        """Applies all logic blocks (AND/OR) within a single rule."""
        # ... (Keep the existing _check_rule method) ...
        try:
            rule_conditions = rule.conditions_json
            if not isinstance(rule_conditions, list): print(f"   [Rule Engine Warning] Rule '{rule}' invalid JSON structure. Skipping."); return False
        except (TypeError, json.JSONDecodeError): print(f"   [Rule Engine Warning] Rule '{rule}' malformed JSON. Skipping."); return False
        if not rule_conditions: return False
        logic_block = rule_conditions[0]; logic = logic_block.get('logic', 'AND').upper(); checks = logic_block.get('checks', [])
        if not checks: return False
        results = [self._evaluate_condition(transaction, check) for check in checks]
        if logic == 'AND': return all(results)
        elif logic == 'OR': return any(results)
        else: print(f"   [Rule Engine Warning] Rule '{rule}' unrecognized logic '{logic}'. Defaulting to AND."); return all(results)


    @transaction.atomic
    def run_analysis(self, assigned_user: User):
        """
        Main function to run the rules engine against relevant transactions.
        Uses the combined list of global and specific rules loaded in __init__.
        """
        print(f"--- Running Analysis for Declaration ID: {self.declaration_id} ---")
        # No need to print rule count here again, it's done in __init__

        # 1. SELECT TRANSACTIONS FOR ANALYSIS (Unchanged)
        new_transactions_qs = Transaction.objects.filter( statement__declaration_id=self.declaration_id, declaration_point__isnull=True).select_related('statement')
        re_evaluate_tx_qs = Transaction.objects.filter( statement__declaration_id=self.declaration_id, unmatched_record__status__in=['PENDING_REVIEW', 'NEW_RULE_PROPOSED']).select_related('statement', 'unmatched_record')
        transactions_for_analysis = list(new_transactions_qs) + list(re_evaluate_tx_qs)
        transactions_for_analysis = list({tx.pk: tx for tx in transactions_for_analysis}.values())
        print(f"   -> Found {len(transactions_for_analysis)} transactions to analyze.")

        # --- (Rest of the run_analysis function remains unchanged) ---
        transactions_to_update = []; unmatched_records_to_clear = []; newly_unmatched_transactions = []
        matched_count = 0
        for tx in transactions_for_analysis:
            is_matched = False; original_status = 'NEW' if not hasattr(tx, 'unmatched_record') else tx.unmatched_record.status
            for rule in self.rules: # Iterate through the combined/sorted list
                if self._check_rule(tx, rule):
                    tx.matched_rule = rule; tx.declaration_point = rule.declaration_point
                    transactions_to_update.append(tx); matched_count += 1; is_matched = True
                    if hasattr(tx, 'unmatched_record'): unmatched_records_to_clear.append(tx.unmatched_record)
                    break # First Match Wins
            if not is_matched:
                if not hasattr(tx, 'unmatched_record'): newly_unmatched_transactions.append(tx)
                elif tx.unmatched_record.status != 'PENDING_REVIEW':
                     tx.unmatched_record.status = 'PENDING_REVIEW'
                     unmatched_records_to_clear.append(tx.unmatched_record)
                if tx.matched_rule is not None or tx.declaration_point is not None:
                     tx.matched_rule = None; tx.declaration_point = None
                     transactions_to_update.append(tx)
        if transactions_to_update:
            updated_count = Transaction.objects.bulk_update(transactions_to_update, ['matched_rule', 'declaration_point'])
            print(f"   -> Updated {updated_count} transactions in database.")
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
        new_unmatched_count = 0
        if newly_unmatched_transactions:
            unmatched_queue_objects = [UnmatchedTransaction(transaction=tx, assigned_user=assigned_user, status='PENDING_REVIEW') for tx in newly_unmatched_transactions]
            created_unmatched = UnmatchedTransaction.objects.bulk_create(unmatched_queue_objects)
            new_unmatched_count = len(created_unmatched)
            print(f"   -> Created {new_unmatched_count} new items in the unmatched queue.")
        print(f"--- Analysis Complete. Matched: {matched_count}, Newly Unmatched: {new_unmatched_count}, Cleared from Queue: {cleared_unmatched_count} ---")
        return matched_count, new_unmatched_count, cleared_unmatched_count


    @transaction.atomic
    def run_analysis_pending_only(self, assigned_user: User):
        """
        Analyzes only NEW and PENDING_REVIEW transactions.
        Does NOT re-evaluate NEW_RULE_PROPOSED items.
        Clears PENDING_REVIEW items if a rule now matches.
        """
        print(f"--- Running Analysis (New & Pending Only) for Declaration ID: {self.declaration_id} ---")
        print(f"   -> Using {len(self.rules)} active rules.")

        # Query includes ONLY NULL and PENDING_REVIEW
        new_transactions_qs = Transaction.objects.filter(statement__declaration_id=self.declaration_id, declaration_point__isnull=True).select_related('statement')
        pending_review_tx_qs = Transaction.objects.filter(statement__declaration_id=self.declaration_id, unmatched_record__status='PENDING_REVIEW').select_related('statement', 'unmatched_record')
        transactions_for_analysis = list(new_transactions_qs) + list(pending_review_tx_qs)
        transactions_for_analysis = list({tx.pk: tx for tx in transactions_for_analysis}.values())
        print(f"   -> Found {len(transactions_for_analysis)} transactions for New/Pending analysis.")

        # --- Analysis logic ---
        transactions_to_update = []; unmatched_records_to_clear = []; newly_unmatched_transactions = []
        matched_count = 0
        for tx in transactions_for_analysis:
            is_matched = False
            is_pending = hasattr(tx, 'unmatched_record') # Check if it started as PENDING_REVIEW

            for rule in self.rules:
                if self._check_rule(tx, rule):
                    tx.matched_rule = rule; tx.declaration_point = rule.declaration_point
                    transactions_to_update.append(tx); matched_count += 1; is_matched = True
                    # Only clear if it was previously PENDING_REVIEW
                    if is_pending:
                        unmatched_records_to_clear.append(tx.unmatched_record)
                    break # First Match Wins
            if not is_matched:
                # If it was NEW and failed, add to queue
                if not is_pending:
                    newly_unmatched_transactions.append(tx)
                # If it was PENDING and still fails, do nothing to its status
                # Clear rule match if re-evaluation fails (shouldn't happen if it was pending, but safeguard)
                if tx.matched_rule is not None or tx.declaration_point is not None:
                     tx.matched_rule = None; tx.declaration_point = None
                     if tx not in transactions_to_update: transactions_to_update.append(tx)

        # --- Database Updates (Similar to run_analysis, but NO REVERT logic needed) ---
        if transactions_to_update:
            updated_count = Transaction.objects.bulk_update(transactions_to_update, ['matched_rule', 'declaration_point'])
            print(f"   -> Updated {updated_count} transactions in database.")

        cleared_unmatched_count = 0
        if unmatched_records_to_clear:
             # All items in this list were PENDING and are now matched, mark RESOLVED
             for um in unmatched_records_to_clear:
                 um.status = 'RESOLVED'; um.resolution_date = timezone.now()
             UnmatchedTransaction.objects.bulk_update(unmatched_records_to_clear, ['status', 'resolution_date'])
             cleared_unmatched_count = len(unmatched_records_to_clear)
             print(f"   -> Marked {len(unmatched_records_to_clear)} previously PENDING items as RESOLVED.")

        new_unmatched_count = 0
        if newly_unmatched_transactions:
            unmatched_queue_objects = [UnmatchedTransaction(transaction=tx, assigned_user=assigned_user, status='PENDING_REVIEW') for tx in newly_unmatched_transactions]
            created_unmatched = UnmatchedTransaction.objects.bulk_create(unmatched_queue_objects)
            new_unmatched_count = len(created_unmatched)
            print(f"   -> Created {new_unmatched_count} new items in the unmatched queue.")

        print(f"--- Analysis (New & Pending) Complete. Matched: {matched_count}, Newly Unmatched: {new_unmatched_count}, Cleared from Queue: {cleared_unmatched_count} ---")
        return matched_count, new_unmatched_count, cleared_unmatched_count
