# tax_processor/transaction_scope_rules_engine.py

from django.db import transaction
from .models import TransactionScopeRule, Transaction
from decimal import Decimal, InvalidOperation
import re
import json
import traceback

class TransactionScopeRulesEngine:
    """
    Engine that processes transactions against TransactionScopeRule models
    to determine if the tx is LOCAL or INTERNATIONAL.
    """

    def __init__(self, declaration_id: int):
        self.declaration_id = declaration_id

        # 1. Query active global rules (declaration is NULL)
        global_rules_qs = TransactionScopeRule.objects.filter(
            is_active=True,
            declaration__isnull=True
        )

        # 2. Query active rules specific to this declaration
        specific_rules_qs = TransactionScopeRule.objects.filter(
            is_active=True,
            declaration_id=self.declaration_id
        )

        # 3. Combine and order
        combined_rules_qs = global_rules_qs | specific_rules_qs
        self.rules = list(combined_rules_qs.order_by('priority', 'rule_name').distinct())

        print(f"   [TxScope Engine Init] Loaded {len(self.rules)} active rules for Decl ID {self.declaration_id}.")


    def _evaluate_condition(self, transaction: Transaction, condition: dict) -> bool:
        """Evaluates a single condition against a transaction field."""
        # This is identical to the main rules_engine
        field = condition.get('field')
        condition_type = condition.get('type')
        value = condition.get('value')

        if not all([field, condition_type]) or value is None:
             print(f"   [TxScope Engine Warn] Malformed condition skipped: {condition}")
             return False

        field_value = None
        try:
            if '__' in field:
                related_parts = field.split('__')
                obj = transaction
                for part in related_parts:
                    if obj is None:
                        field_value = None
                        break
                    obj = getattr(obj, part, None)
                field_value = obj
            else:
                field_value = getattr(transaction, field, None)
        except AttributeError:
             print(f"   [TxScope Engine Warn] Invalid field '{field}': {condition}")
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
                except (InvalidOperation, ValueError, TypeError): return False
            elif condition_type in ['GREATER_THAN', 'LESS_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL', 'RANGE_AMOUNT'] and field != 'amount':
                 return False
            else: return False
        except Exception as e: print(f"   [TxScope Engine Error] {e}"); traceback.print_exc(); return False


    def _check_rule(self, transaction: Transaction, rule: TransactionScopeRule) -> bool:
        """Applies all logic blocks (AND/OR) within a single rule."""
        # This is identical to the main rules_engine
        try:
            rule_conditions = rule.conditions_json
            if not isinstance(rule_conditions, list): return False
        except (TypeError, json.JSONDecodeError): return False
        if not rule_conditions: return False
        logic_block = rule_conditions[0]; logic = logic_block.get('logic', 'AND').upper(); checks = logic_block.get('checks', [])
        if not checks: return False
        results = [self._evaluate_condition(transaction, check) for check in checks]
        if logic == 'AND': return all(results)
        elif logic == 'OR': return any(results)
        else: return all(results)


    @transaction.atomic
    def run_analysis(self, run_all: bool = False):
        """
        Main function.
        If run_all=True, re-evaluates all transactions for this declaration.
        If run_all=False, only evaluates transactions marked 'UNDETERMINED'.
        """
        print(f"--- Running TxScope Analysis for Declaration ID: {self.declaration_id} ---")

        if run_all:
            transactions_qs = Transaction.objects.filter(
                statement__declaration_id=self.declaration_id
            ).select_related('statement')
            print("   -> Mode: Re-evaluating ALL transactions.")
        else:
            transactions_qs = Transaction.objects.filter(
                statement__declaration_id=self.declaration_id,
                transaction_scope='UNDETERMINED'
            ).select_related('statement')
            print("   -> Mode: Evaluating only 'UNDETERMINED' transactions.")

        transactions_for_analysis = list(transactions_qs)
        print(f"   -> Found {len(transactions_for_analysis)} transactions to analyze.")

        transactions_to_update = []
        matched_count = 0

        for tx in transactions_for_analysis:
            # --- THIS IS THE FIX ---
            match_found = False
            # --- END FIX ---

            for rule in self.rules:
                if self._check_rule(tx, rule):
                    # A rule matched, apply its result
                    if tx.transaction_scope != rule.scope_result:
                        tx.transaction_scope = rule.scope_result
                        if tx not in transactions_to_update:
                            transactions_to_update.append(tx)
                    matched_count += 1
                    match_found = True
                    break # First Match Wins

            # This logic is now safe to run
            if not match_found and tx.transaction_scope == 'UNDETERMINED':
                tx.transaction_scope = 'LOCAL'
                if tx not in transactions_to_update:
                    transactions_to_update.append(tx)

        if transactions_to_update:
            updated_count = Transaction.objects.bulk_update(transactions_to_update, ['transaction_scope'])
            print(f"   -> Updated {updated_count} transaction scopes in database.")

        print(f"--- TxScope Analysis Complete. Total rules matched: {matched_count} ---")
        return matched_count
