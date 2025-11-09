# tax_processor/entity_type_rules_engine.py

from django.db import transaction
from .models import EntityTypeRule, Transaction, ExchangeRate
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
        self.rates_cache = {}
        print(f"   [EntityType Engine Init] Loaded {len(self.rules)} active rules for Decl ID {self.declaration_id}.")

    def _get_dynamic_value(self, transaction: Transaction, field_name: str):
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

    def _evaluate_condition(self, transaction: Transaction, condition: dict) -> bool:
        field = condition.get('field')
        condition_type = condition.get('type')
        value = condition.get('value')

        DYNAMIC_CONDITION_TYPES = [
            'CONTAINS_FIELD_VALUE',
            'NOT_CONTAINS_FIELD_VALUE',
            'EQUALS_FIELD_VALUE'
        ]

        if not all([field, condition_type]) or value is None:
             print(f"   [EntityType Engine Warn] Malformed condition skipped: {condition}")
             return False

        field_value_raw = self._get_dynamic_value(transaction, field)
        if field_value_raw is None:
            return False

        str_field_value = str(field_value_raw)

        try:
            if condition_type in DYNAMIC_CONDITION_TYPES:
                value_from_field_raw = self._get_dynamic_value(transaction, value)
                if value_from_field_raw is None:
                    return False
                str_value_from_field = str(value_from_field_raw)
                if condition_type == 'CONTAINS_FIELD_VALUE':
                    return str_value_from_field.lower() in str_field_value.lower()
                elif condition_type == 'NOT_CONTAINS_FIELD_VALUE':
                    return str_value_from_field.lower() not in str_field_value.lower()
                elif condition_type == 'EQUALS_FIELD_VALUE':
                    return str_field_value.strip().lower() == str_value_from_field.strip().lower()
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
                        if transaction.currency != 'AMD':
                            rate = self.rates_cache.get((transaction.transaction_date.date(), transaction.currency))
                            if rate is None:
                                closest_rate = ExchangeRate.objects.filter(
                                    currency_code=transaction.currency,
                                    date__lt=transaction.transaction_date.date()
                                ).order_by('-date').first()
                                if closest_rate:
                                    rate = closest_rate.rate
                                    self.rates_cache[(transaction.transaction_date.date(), transaction.currency)] = rate
                                else:
                                    print(f"   [EntityType Engine Warn] No exchange rate found for {transaction.currency} on {transaction.transaction_date.date()}. Rule will fail.")
                                    return False
                            tx_amount = tx_amount * rate
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

    # --- NEW: Recursive function to evaluate a logic group ---
    def _evaluate_logic_group(self, transaction: Transaction, group: dict) -> bool:
        """
        Evaluates a single group of conditions (e.g., "A AND B" or "C OR D").
        """
        logic = group.get('group_logic', 'AND').upper()
        conditions = group.get('conditions', [])

        if not conditions:
            return False

        results = [self._evaluate_condition(transaction, cond) for cond in conditions]

        if logic == 'AND':
            return all(results)
        elif logic == 'OR':
            return any(results)

        print(f"   [EntityType Engine Warning] Unrecognized group logic '{logic}'. Defaulting to AND.")
        return all(results)
    # --- END NEW ---

    # --- MODIFIED: _check_rule now supports both old and new JSON formats ---
    def _check_rule(self, transaction: Transaction, rule: EntityTypeRule) -> bool:
        """
        Evaluates a full rule (groups of conditions) against a transaction.
        Supports both new nested format and old flat format.
        """
        try:
            conditions_json = rule.conditions_json
        except (TypeError, json.JSONDecodeError):
            print(f"   [EntityType Engine Warning] Rule '{rule}' malformed JSON. Skipping."); return False

        if not conditions_json:
            return False

        # --- Backward Compatibility: Detect OLD flat format ---
        if isinstance(conditions_json, list) and conditions_json:
            old_data = conditions_json[0]
            if 'logic' in old_data and 'checks' in old_data:
                conditions_json = {
                    "root_logic": old_data['logic'],
                    "groups": [
                        {
                            "group_logic": old_data['logic'],
                            "conditions": old_data['checks']
                        }
                    ]
                }
        # --- End Backward Compatibility ---

        # --- New Nested Logic ---
        root_logic = conditions_json.get('root_logic', 'AND').upper()
        groups = conditions_json.get('groups', [])

        if not groups:
            return False

        group_results = [self._evaluate_logic_group(transaction, group) for group in groups]

        if root_logic == 'AND':
            return all(group_results)
        elif root_logic == 'OR':
            return any(group_results)

        print(f"   [EntityType Engine Warning] Rule '{rule}' unrecognized root logic '{root_logic}'. Defaulting to AND.")
        return all(group_results)
    # --- END MODIFIED ---


    @transaction.atomic
    def run_analysis(self, run_all: bool = False):
        print(f"--- Running EntityType Analysis for Declaration ID: {self.declaration_id} ---")

        if run_all:
            transactions_qs = Transaction.objects.filter(
                statement__declaration_id=self.declaration_id,
                is_expense=False
            ).select_related('statement')
            print("   -> Mode: Re-evaluating ALL income transactions.")
        else:
            transactions_qs = Transaction.objects.filter(
                statement__declaration_id=self.declaration_id,
                entity_type='UNDETERMINED',
                is_expense=False
            ).select_related('statement')
            print("   -> Mode: Evaluating only 'UNDETERMINED' income transactions.")

        transactions_for_analysis = list(transactions_qs)
        print(f"   -> Found {len(transactions_for_analysis)} transactions to analyze.")

        self.rates_cache = {}
        non_amd_txs = [tx for tx in transactions_for_analysis if tx.currency != 'AMD']
        if non_amd_txs:
            unique_dates = {tx.transaction_date.date() for tx in non_amd_txs}
            unique_currencies = {tx.currency for tx in non_amd_txs}

            rates_qs = ExchangeRate.objects.filter(
                date__in=unique_dates,
                currency_code__in=unique_currencies
            )
            self.rates_cache = {(rate.date, rate.currency_code): rate.rate for rate in rates_qs}
            print(f"   -> Cached {len(self.rates_cache)} exchange rates for amount comparison.")

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
