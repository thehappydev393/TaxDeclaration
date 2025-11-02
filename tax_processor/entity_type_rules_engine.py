# tax_processor/entity_type_rules_engine.py

from django.db import transaction
from .models import EntityTypeRule, Transaction
from decimal import Decimal, InvalidOperation
import re
import json
import traceback

class EntityTypeRulesEngine:
    """
    Engine that processes transactions against EntityTypeRule models
    to determine if the sender is an INDIVIDUAL or LEGAL entity.
    """

    def __init__(self, declaration_id: int):
        self.declaration_id = declaration_id
        global_rules_qs = EntityTypeRule.objects.filter(
            is_active=True,
            declaration__isnull=True
        )
        specific_rules_qs = EntityTypeRule.objects.filter(
            is_active=True,
            declaration_id=self.declaration_id
        )
        combined_rules_qs = global_rules_qs | specific_rules_qs
        self.rules = list(combined_rules_qs.order_by('priority', 'rule_name').distinct())

        print(f"   [EntityType Engine Init] Loaded {len(self.rules)} active rules for Decl ID {self.declaration_id}.")

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
             print(f"   [EntityType Engine Warn] Malformed condition skipped: {condition}")
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
                        print(f"   [EntityType Engine Warn] Invalid number for comparison: {condition}, Tx Value: {field_value_raw}"); return False
                elif condition_type in ['GREATER_THAN', 'LESS_THAN', 'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL', 'RANGE_AMOUNT'] and field != 'amount':
                     print(f"   [EntityType Engine Warn] Numeric comparison '{condition_type}' on non-amount field '{field}'. Skipped: {condition}"); return False
                else:
                    print(f"   [EntityType Engine Warn] Unrecognized condition type '{condition_type}': {condition}"); return False

        except Exception as e:
            print(f"   [EntityType Engine Error] Unexpected error evaluating condition: {condition}. Error: {e}"); traceback.print_exc(); return False
    # --- END UPDATED ---

    def _check_rule(self, transaction: Transaction, rule: EntityTypeRule) -> bool:
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
        print(f"--- Running EntityType Analysis for Declaration ID: {self.declaration_id} ---")

        if run_all:
            transactions_qs = Transaction.objects.filter(
                statement__declaration_id=self.declaration_id
            ).select_related('statement')
            print("   -> Mode: Re-evaluating ALL transactions.")
        else:
            transactions_qs = Transaction.objects.filter(
                statement__declaration_id=self.declaration_id,
                entity_type='UNDETERMINED'
            ).select_related('statement')
            print("   -> Mode: Evaluating only 'UNDETERMINED' transactions.")

        transactions_for_analysis = list(transactions_qs)
        print(f"   -> Found {len(transactions_for_analysis)} transactions to analyze.")

        transactions_to_update = []
        matched_count = 0

        for tx in transactions_for_analysis:
            for rule in self.rules:
                if self._check_rule(tx, rule):
                    if tx.entity_type != rule.entity_type_result:
                        tx.entity_type = rule.entity_type_result
                        transactions_to_update.append(tx)
                    matched_count += 1
                    break

        if transactions_to_update:
            updated_count = Transaction.objects.bulk_update(transactions_to_update, ['entity_type'])
            print(f"   -> Updated {updated_count} transaction entity types in database.")

        print(f"--- EntityType Analysis Complete. Total rules matched: {matched_count} ---")
        return matched_count
