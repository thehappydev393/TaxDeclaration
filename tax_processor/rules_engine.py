# tax_processor/rules_engine.py

from django.db import transaction
from django.db.models import Q
from .models import TaxRule, Transaction, UnmatchedTransaction, User
from decimal import Decimal, InvalidOperation # Import InvalidOperation
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
        # Load active rules: Global first (null declaration), then specific, sorted by priority
        # Global rules query
        global_rules_qs = TaxRule.objects.filter(is_active=True, declaration__isnull=True)
        # Specific rules query
        specific_rules_qs = TaxRule.objects.filter(is_active=True, declaration_id=self.declaration_id)
        # Combine and sort
        # Note: If priorities overlap, specific rules might run before global ones or vice versa depending on db order.
        # If strict global-first-then-specific priority is needed, adjust sorting.
        self.rules = list((global_rules_qs | specific_rules_qs).order_by('priority', 'rule_name'))

        # Removed self.unmatched_transactions and self.matched_count initialization here
        # as they are handled within run_analysis

    def _evaluate_condition(self, transaction: Transaction, condition: dict) -> bool:
        """Evaluates a single condition against a transaction field."""

        field = condition.get('field')
        condition_type = condition.get('type')
        value = condition.get('value')

        if not all([field, condition_type]) or value is None: # Allow empty string value, but not None
             print(f"   [Rule Engine Warning] Malformed condition skipped: {condition}")
             return False

        # Get the transaction field value (handle potential AttributeError)
        try:
            field_value = getattr(transaction, field, None)
        except AttributeError:
             print(f"   [Rule Engine Warning] Invalid field '{field}' in condition: {condition}")
             return False

        # If the transaction's field is None, it cannot match any condition requiring a value
        if field_value is None:
            return False

        # Convert field value to string for text comparisons
        str_field_value = str(field_value)
        str_value_lower = str(value).lower() # Lowercase the rule's value once for text checks

        try:
            # --- Text Comparisons ---
            if condition_type == 'CONTAINS_KEYWORD':
                keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]
                return any(kw in str_field_value.lower() for kw in keywords)

            # --- NEW: DOES_NOT_CONTAIN_KEYWORD ---
            elif condition_type == 'DOES_NOT_CONTAIN_KEYWORD':
                 keywords = [kw.strip() for kw in str_value_lower.split(',') if kw.strip()]
                 # Return True if NONE of the keywords are found
                 return not any(kw in str_field_value.lower() for kw in keywords)

            elif condition_type == 'EQUALS':
                return str_field_value.strip().lower() == str_value_lower.strip()

            elif condition_type == 'REGEX_MATCH':
                # Compiles regex for efficiency if used repeatedly (though maybe overkill here)
                # re.search returns a match object (truthy) or None (falsy)
                return bool(re.search(str(value), str_field_value, re.IGNORECASE))


            # --- Numeric Comparisons (Only apply to 'amount' field) ---
            elif field == 'amount' and condition_type in [
                'GREATER_THAN', 'LESS_THAN',
                'GREATER_THAN_OR_EQUAL', 'LESS_THAN_OR_EQUAL',
                'RANGE_AMOUNT' # Kept for backward compatibility, though maybe less needed now
            ]:
                try:
                    # Ensure transaction amount is Decimal
                    tx_amount = Decimal(str(field_value))

                    # GREATER_THAN / LESS_THAN (Existing)
                    if condition_type == 'GREATER_THAN':
                        num_value = Decimal(str(value))
                        return tx_amount > num_value
                    elif condition_type == 'LESS_THAN':
                        num_value = Decimal(str(value))
                        return tx_amount < num_value

                    # --- NEW: GREATER_THAN_OR_EQUAL ---
                    elif condition_type == 'GREATER_THAN_OR_EQUAL':
                         num_value = Decimal(str(value))
                         return tx_amount >= num_value

                    # --- NEW: LESS_THAN_OR_EQUAL ---
                    elif condition_type == 'LESS_THAN_OR_EQUAL':
                         num_value = Decimal(str(value))
                         return tx_amount <= num_value

                    elif condition_type == 'RANGE_AMOUNT':
                        # Value format expected: "100.00, 500.00"
                        min_val_str, max_val_str = map(str.strip, str(value).split(','))
                        min_val = Decimal(min_val_str)
                        max_val = Decimal(max_val_str)
                        return min_val <= tx_amount <= max_val

                except (InvalidOperation, ValueError, TypeError):
                    # Handle cases where rule value or transaction amount isn't a valid number
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
            # General catch-all for unexpected errors during evaluation
            print(f"   [Rule Engine Error] Unexpected error evaluating condition: {condition}. Error: {e}")
            traceback.print_exc()
            return False

    def _check_rule(self, transaction: Transaction, rule: TaxRule) -> bool:
        """Applies all logic blocks (AND/OR) within a single rule."""
        try:
            # Check if conditions_json is valid JSON array/list
            rule_conditions = rule.conditions_json
            if not isinstance(rule_conditions, list):
                 print(f"   [Rule Engine Warning] Rule '{rule.rule_name}' (ID: {rule.pk}) has invalid JSON structure (not a list). Skipping rule.")
                 return False
        except (TypeError, json.JSONDecodeError):
             print(f"   [Rule Engine Warning] Rule '{rule.rule_name}' (ID: {rule.pk}) has malformed JSON. Skipping rule.")
             return False

        # If conditions list is empty, the rule should not match anything
        if not rule_conditions:
            return False

        # Assume structure: [{"logic": "AND/OR", "checks": [...]}]
        # We currently only support one logic block per rule via the UI
        logic_block = rule_conditions[0]
        logic = logic_block.get('logic', 'AND').upper()
        checks = logic_block.get('checks', [])

        if not checks:
             # Rule has no actual checks defined
             return False

        # Evaluate all individual checks in this block
        results = [self._evaluate_condition(transaction, check) for check in checks]

        if logic == 'AND':
            # Must all be True
            return all(results)
        elif logic == 'OR':
            # At least one must be True
            return any(results)
        else:
            # Default or unrecognized logic - treat as AND
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
        print(f"   -> Using {len(self.rules)} active rules (global + specific).")

        # 1. SELECT TRANSACTIONS FOR ANALYSIS
        new_transactions_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            declaration_point__isnull=True
        ).select_related('statement')

        re_evaluate_tx_qs = Transaction.objects.filter(
            statement__declaration_id=self.declaration_id,
            unmatched_record__status__in=['PENDING_REVIEW', 'NEW_RULE_PROPOSED']
        ).select_related('statement', 'unmatched_record')

        # Combine QuerySets efficiently if possible (may need list conversion if complex)
        transactions_for_analysis = list(new_transactions_qs) + list(re_evaluate_tx_qs)
        # Remove duplicates if any transaction appears in both querysets
        transactions_for_analysis = list({tx.pk: tx for tx in transactions_for_analysis}.values())
        print(f"   -> Found {len(transactions_for_analysis)} transactions to analyze.")


        transactions_to_update = []
        unmatched_records_to_clear = []
        newly_unmatched_transactions = [] # Store transactions that become unmatched
        matched_count = 0

        # 2. APPLY RULES
        for tx in transactions_for_analysis:
            is_matched = False
            original_status = 'NEW' if not hasattr(tx, 'unmatched_record') else tx.unmatched_record.status

            for rule in self.rules:
                if self._check_rule(tx, rule):
                    # Found a match!
                    tx.matched_rule = rule
                    tx.declaration_point = rule.declaration_point # Assign the DeclarationPoint object
                    transactions_to_update.append(tx)
                    matched_count += 1
                    is_matched = True

                    # If it was previously unmatched, mark for clearance
                    if hasattr(tx, 'unmatched_record'):
                        unmatched_records_to_clear.append(tx.unmatched_record)

                    # print(f"      -> Tx ID {tx.pk} MATCHED rule '{rule.rule_name}' (P{rule.priority}). Status: {original_status} -> RESOLVED.")
                    break # First Match Wins (based on priority)

            if not is_matched:
                # If no rule matched
                if not hasattr(tx, 'unmatched_record'):
                    # It's a new transaction that failed all rules -> add to queue
                    newly_unmatched_transactions.append(tx)
                    # print(f"      -> Tx ID {tx.pk} - NO MATCH. Status: NEW -> PENDING_REVIEW.")
                elif tx.unmatched_record.status != 'PENDING_REVIEW':
                     # It was proposed but now fails rules again, send back to pending
                     tx.unmatched_record.status = 'PENDING_REVIEW'
                     tx.unmatched_record.rule_proposal_json = None # Clear proposal maybe? Or keep? Keeping for now.
                     unmatched_records_to_clear.append(tx.unmatched_record) # Use this list to update status
                     # print(f"      -> Tx ID {tx.pk} - NO MATCH. Status: {original_status} -> PENDING_REVIEW.")

                # Reset matched_rule and declaration_point if it failed re-evaluation
                if tx.matched_rule is not None or tx.declaration_point is not None:
                     tx.matched_rule = None
                     tx.declaration_point = None
                     transactions_to_update.append(tx) # Ensure these fields are cleared in DB


        # 3. BULK UPDATES (Matched/Cleared Transactions)
        if transactions_to_update:
            updated_count = Transaction.objects.bulk_update(
                transactions_to_update,
                ['matched_rule', 'declaration_point']
            )
            print(f"   -> Updated {updated_count} transactions in database.")


        # 4. UPDATE UNMATCHED QUEUE STATUS (Clear resolved items, revert proposals)
        cleared_unmatched_count = 0
        if unmatched_records_to_clear:
             # Separate items to be marked RESOLVED vs PENDING_REVIEW
             to_resolve = [um for um in unmatched_records_to_clear if um.transaction.declaration_point is not None]
             to_revert = [um for um in unmatched_records_to_clear if um.transaction.declaration_point is None and um.status != 'PENDING_REVIEW']

             if to_resolve:
                 for um in to_resolve:
                     um.status = 'RESOLVED'
                     um.resolution_date = timezone.now()
                 UnmatchedTransaction.objects.bulk_update(to_resolve, ['status', 'resolution_date'])
                 cleared_unmatched_count = len(to_resolve)
                 print(f"   -> Marked {len(to_resolve)} previously unmatched items as RESOLVED.")

             if to_revert:
                 for um in to_revert: um.status = 'PENDING_REVIEW' # Revert status
                 UnmatchedTransaction.objects.bulk_update(to_revert, ['status'])
                 print(f"   -> Reverted {len(to_revert)} proposed/other items back to PENDING_REVIEW.")


        # 5. CREATE NEW UNMATCHED QUEUE ITEMS
        new_unmatched_count = 0
        if newly_unmatched_transactions:
            unmatched_queue_objects = [
                UnmatchedTransaction(
                    transaction=tx,
                    assigned_user=assigned_user, # Assign to the user who ran the analysis
                    status='PENDING_REVIEW'
                )
                for tx in newly_unmatched_transactions
            ]
            created_unmatched = UnmatchedTransaction.objects.bulk_create(unmatched_queue_objects)
            new_unmatched_count = len(created_unmatched)
            print(f"   -> Created {new_unmatched_count} new items in the unmatched queue.")

        print(f"--- Analysis Complete. Matched: {matched_count}, Newly Unmatched: {new_unmatched_count}, Cleared from Queue: {cleared_unmatched_count} ---")
        return matched_count, new_unmatched_count, cleared_unmatched_count
