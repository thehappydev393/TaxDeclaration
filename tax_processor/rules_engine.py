# tax_processor/rules_engine.py

from django.db import transaction
from django.db.models import Q
from .models import TaxRule, Transaction, UnmatchedTransaction, User
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
        self.declaration_id = declaration_id

        # 1. Query active global rules (declaration is NULL)
        global_rules_qs = TaxRule.objects.filter(
            is_active=True,
            declaration__isnull=True
        )

        # 2. Query active rules specific to this declaration
        specific_rules_qs = TaxRule.objects.filter(
            is_active=True,
            declaration_id=self.declaration_id
        )

        combined_rules_qs = global_rules_qs | specific_rules_qs
        self.rules = list(combined_rules_qs.order_by('priority', 'rule_name').distinct())

        print(f"   [Rule Engine Init] Loaded {len(self.rules)} active rules (global + specific for Decl ID {self.declaration_id}).")


    # --- NEW: Helper to get field values dynamically ---
    def _get_dynamic_value(self, transaction: Transaction, field_name: str):
        """
        Safely gets a value from a transaction, following relationships.
        e.g., 'description' or 'statement__declaration__first_name'
        """
        try:
            if '__' in field_name:
                related_parts = field_name.split('__')
                obj = transaction
                for part in related_parts:
                    if obj is None: return None
                    obj = getattr(obj, part, None)
                return obj
            else:
                return getattr(transaction, field_name, None)
        except AttributeError:
            return None
    # --- END NEW ---

    # --- UPDATED: _evaluate_condition method ---
    def _evaluate_condition(self, transaction: Transaction, condition: dict) -> bool:
        """Evaluates a single condition against a transaction field."""

        field = condition.get('field')
        condition_type = condition.get('type')
        value = condition.get('value') # This now holds static text OR a field name

        DYNAMIC_CONDITION_TYPES = [
            'CONTAINS_FIELD_VALUE',
            'NOT_CONTAINS_FIELD_VALUE',
            'EQUALS_FIELD_VALUE'
        ]

        if not all([field, condition_type]) or value is None:
             print(f"   [Rule Engine Warning] Malformed condition skipped: {condition}")
             return False

        # Get the "left side" value (e.g., tx.description)
        field_value_raw = self._get_dynamic_value(transaction, field)
        if field_value_raw is None:
            return False # Can't compare None

        str_field_value = str(field_value_raw)

        try:
            # --- NEW: Dynamic Field-to-Field Comparison ---
            if condition_type in DYNAMIC_CONDITION_TYPES:
                # 'value' is the name of the *other* field (e.g., "statement__declaration__first_name")
                value_from_field_raw = self._get_dynamic_value(transaction, value)

                if value_from_field_raw is None:
                    return False # Can't compare against None

                str_value_from_field = str(value_from_field_raw)

                if condition_type == 'CONTAINS_FIELD_VALUE':
                    return str_value_from_field.lower() in str_field_value.lower()
                elif condition_type == 'NOT_CONTAINS_FIELD_VALUE':
                    return str_value_from_field.lower() not in str_field_value.lower()
                elif condition_type == 'EQUALS_FIELD_VALUE':
                    return str_field_value.strip().lower() == str_value_from_field.strip().lower()

            # --- EXISTING: Static Value Comparison ---
            else:
                str_value_lower = str(value).lower()

                if condition_type == 'CONTAINS_KEYWORD':
                    keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]; return any(kw in str_field_value.lower() for kw in keywords)
                elif condition_type == 'DOES_NOT_CONTAIN_KEYWORD':
                     keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]; return not any(kw in str_field_value.lower() for kw in keywords)
                elif condition_type == 'EQUALS':
                    return str_field_value.strip().lower() == str_value_lower.strip()
                elif condition_type == 'REGEX_MATCH':
                    return bool(re.search(str(value), str_field_value, re.IGNORECASE))
                elif field == 'amount' and condition_type in ['GREATER_THAN', 'LESS_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL', 'RANGE_AMOUNT']:
                    try:
                        tx_amount = Decimal(str_field_value)
                        if condition_type == 'GREATER_THAN': num_value = Decimal(str(value)); return tx_amount > num_value
                        elif condition_type == 'LESS_THAN': num_value = Decimal(str(value)); return tx_amount < num_value
                        elif condition_type == 'GREATER_THAN_OR_EQUAL': num_value = Decimal(str(value)); return tx_amount >= num_value
                        elif condition_type == 'LESS_THAN_OR_EQUAL': num_value = Decimal(str(value)); return tx_amount <= num_value
                        elif condition_type == 'RANGE_AMOUNT':
                            min_val_str, max_val_str = map(str.strip, str(value).split(','))
                            min_val = Decimal(min_val_str); max_val = Decimal(max_val_str); return min_val <= tx_amount <= max_val
                    except (InvalidOperation, ValueError, TypeError):
                        print(f"   [Rule Engine Warning] Invalid number for comparison: {condition}, Tx Value: {field_value_raw}"); return False
                elif condition_type in ['GREATER_THAN', 'LESS_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL', 'RANGE_AMOUNT'] and field != 'amount':
                     print(f"   [Rule Engine Warning] Numeric comparison '{condition_type}' on non-amount field '{field}'. Skipped: {condition}"); return False
                else:
                    print(f"   [Rule Engine Warning] Unrecognized condition type '{condition_type}': {condition}"); return False

        except Exception as e:
            print(f"   [Rule Engine Error] Unexpected error evaluating condition: {condition}. Error: {e}"); traceback.print_exc(); return False
    # --- END UPDATED ---

    def _check_rule(self, transaction: Transaction, rule: TaxRule) -> bool:
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
        print(f"--- Running Analysis for Declaration ID: {self.declaration_id} ---")

        # 1. SELECT TRANSACTIONS FOR ANALYSIS
        # --- MODIFIED: Added is_expense=False ---
        new_transactions_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            declaration_point__isnull=True,
            is_expense=False
        ).select_related('statement')

        re_evaluate_tx_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            unmatched_record__status__in=['PENDING_REVIEW', 'NEW_RULE_PROPOSED'],
            is_expense=False
        ).select_related('statement', 'unmatched_record')
        # --- END MODIFIED ---

        transactions_for_analysis = list(new_transactions_qs) + list(re_evaluate_tx_qs)
        transactions_for_analysis = list({tx.pk: tx for tx in transactions_for_analysis}.values())
        print(f"   -> Found {len(transactions_for_analysis)} income transactions to analyze.")

        transactions_to_update = []; unmatched_records_to_clear = []; newly_unmatched_transactions = []
        matched_count = 0
        for tx in transactions_for_analysis:
            is_matched = False; original_status = 'NEW' if not hasattr(tx, 'unmatched_record') else tx.unmatched_record.status
            for rule in self.rules:
                if self._check_rule(tx, rule):
                    tx.matched_rule = rule; tx.declaration_point = rule.declaration_point
                    transactions_to_update.append(tx); matched_count += 1; is_matched = True
                    if hasattr(tx, 'unmatched_record'): unmatched_records_to_clear.append(tx.unmatched_record)
                    break
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
        print(f"--- Running Analysis (New & Pending Only) for Declaration ID: {self.declaration_id} ---")
        print(f"   -> Using {len(self.rules)} active rules.")

        # --- MODIFIED: Added is_expense=False ---
        new_transactions_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            declaration_point__isnull=True,
            is_expense=False
        ).select_related('statement')

        pending_review_tx_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            unmatched_record__status='PENDING_REVIEW',
            is_expense=False
        ).select_related('statement', 'unmatched_record')
        # --- END MODIFIED ---

        transactions_for_analysis = list(new_transactions_qs) + list(pending_review_tx_qs)
        transactions_for_analysis = list({tx.pk: tx for tx in transactions_for_analysis}.values())
        print(f"   -> Found {len(transactions_for_analysis)} transactions for New/Pending analysis.")

        transactions_to_update = []; unmatched_records_to_clear = []; newly_unmatched_transactions = []
        matched_count = 0
        for tx in transactions_for_analysis:
            is_matched = False
            is_pending = hasattr(tx, 'unmatched_record')

            for rule in self.rules:
                if self._check_rule(tx, rule):
                    tx.matched_rule = rule; tx.declaration_point = rule.declaration_point
                    transactions_to_update.append(tx); matched_count += 1; is_matched = True
                    if is_pending:
                        unmatched_records_to_clear.append(tx.unmatched_record)
                    break
            if not is_matched:
                if not is_pending:
                    newly_unmatched_transactions.append(tx)
                if tx.matched_rule is not None or tx.declaration_point is not None:
                     tx.matched_rule = None; tx.declaration_point = None
                     if tx not in transactions_to_update: transactions_to_update.append(tx)

        if transactions_to_update:
            updated_count = Transaction.objects.bulk_update(transactions_to_update, ['matched_rule', 'declaration_point'])
            print(f"   -> Updated {updated_count} transactions in database.")

        cleared_unmatched_count = 0
        if unmatched_records_to_clear:
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
