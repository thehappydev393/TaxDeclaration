# tax_processor/analysis_hints.py

import pandas as pd
from rapidfuzz import process, utils
from django.db.models import Sum
from .models import Transaction, Declaration, AnalysisHint
from collections import defaultdict

# --- Configuration ---
MIN_SENDER_FREQUENCY = 5  # Min txs from one sender to trigger a hint
LARGE_AMOUNT_THRESHOLD = 1000000  # Min amount (in any currency) to trigger a hint
SIMILAR_DESC_THRESHOLD = 90       # Similarity score (0-100)
MIN_DESC_CLUSTER_SIZE = 3         # Min txs in a "similar description" cluster


def _find_frequent_senders(df: pd.DataFrame, declaration: Declaration, new_hints: list):
    """
    Finds senders who appear frequently in the unmatched transaction list.
    """
    if 'sender' not in df.columns:
        return

    # Group by sender and get count, total amount, and all transaction IDs
    sender_groups = df.groupby('sender').agg(
        transaction_ids=('id', list),
        count=('id', 'size')
    ).reset_index()

    # Filter for senders who appear more than the minimum threshold
    frequent_senders = sender_groups[sender_groups['count'] >= MIN_SENDER_FREQUENCY]

    for _, row in frequent_senders.iterrows():
        # Re-query to get currency-specific totals (cleaner than complex pandas agg)
        currency_totals = Transaction.objects.filter(id__in=row['transaction_ids']).values('currency').annotate(total=Sum('amount'))
        totals_str = ", ".join([f"{t['total']:,.2f} {t['currency']}" for t in currency_totals])

        new_hints.append(
            AnalysisHint(
                declaration=declaration,
                hint_type='SENDER',
                title=f"Հաճախակի Ուղարկող (Frequent Sender): {row['sender']}",
                description=f"Հայտնաբերվել է {row['count']} չհամընկած գործարք այս ուղարկողից, ընդհանուր՝ {totals_str}։",
                related_transaction_ids=row['transaction_ids'],
                is_resolved=False
            )
        )

def _find_large_amount_outliers(df: pd.DataFrame, declaration: Declaration, new_hints: list):
    """
    Finds single transactions that are over a large absolute amount.
    """
    if 'amount' not in df.columns:
        return

    # Filter for any transaction over the absolute threshold
    large_txs = df[df['amount'] > LARGE_AMOUNT_THRESHOLD]

    for _, tx in large_txs.iterrows():
        new_hints.append(
            AnalysisHint(
                declaration=declaration,
                hint_type='AMOUNT',
                title=f"Խոշոր Գործարք (Large Amount): {tx['amount']:,.2f} {tx['currency']}",
                description=f"Ուղարկող՝ {tx['sender']}, Նկարագրություն՝ {tx['description'][:70]}...",
                related_transaction_ids=[tx['id']],
                is_resolved=False
            )
        )

def _find_similar_descriptions(df: pd.DataFrame, declaration: Declaration, new_hints: list):
    """
    Finds clusters of transactions with highly similar descriptions using rapidfuzz.
    """
    if 'description' not in df.columns:
        return

    # Map descriptions to their transaction IDs
    desc_to_ids_map = df.groupby('description')['id'].apply(list).to_dict()
    all_descs = list(desc_to_ids_map.keys())

    # Keep track of descriptions we've already added to a cluster
    processed_descs = set()

    for desc in all_descs:
        if desc in processed_descs:
            continue

        # Find other descriptions that are highly similar
        # e.g., "Salary for Jan 2024", "Salary for Feb 2024"
        similar = process.extract(
            desc,
            all_descs,
            scorer=utils.default_scorer,
            processor=utils.default_processor,
            score_cutoff=SIMILAR_DESC_THRESHOLD,
            limit=50 # Limit search space for performance
        )

        # Get the *text* of all descriptions in this new cluster
        cluster_descs = [s[0] for s in similar]

        if len(cluster_descs) >= MIN_DESC_CLUSTER_SIZE:
            cluster_tx_ids = []

            # Mark all descriptions in this cluster as processed
            for s_desc in cluster_descs:
                if s_desc not in processed_descs:
                    cluster_tx_ids.extend(desc_to_ids_map[s_desc])
                    processed_descs.add(s_desc)

            if not cluster_tx_ids:
                continue

            # Get currency-specific totals for this cluster
            currency_totals = Transaction.objects.filter(id__in=cluster_tx_ids).values('currency').annotate(total=Sum('amount'))
            totals_str = ", ".join([f"{t['total']:,.2f} {t['currency']}" for t in currency_totals])

            new_hints.append(
                AnalysisHint(
                    declaration=declaration,
                    hint_type='DESCRIPTION',
                    title=f"Նմանատիպ Նկարագրություններ (Similar Descriptions): (օրինակ՝ \"{desc[:50]}...\")",
                    description=f"Հայտնաբերվել է {len(cluster_tx_ids)} գործարք նմանատիպ նկարագրություններով, ընդհանուր՝ {totals_str}։",
                    related_transaction_ids=cluster_tx_ids,
                    is_resolved=False
                )
            )

def generate_analysis_hints(declaration_id: int):
    """
    Main function to generate all hints for a declaration's unmatched transactions.
    """
    print(f"--- Running Analysis Hint Generation for Declaration ID: {declaration_id} ---")

    try:
        declaration = Declaration.objects.get(pk=declaration_id)
    except Declaration.DoesNotExist:
        print("   [Hint Engine Error] Declaration not found.")
        return 0

    # 1. Clear all old hints for this declaration
    AnalysisHint.objects.filter(declaration=declaration).delete()

    # 2. Get all unmatched income transactions
    # --- MODIFIED: Added is_expense=False ---
    unmatched_txs = Transaction.objects.filter(
        statement__declaration=declaration,
        declaration_point__isnull=True,
        is_expense=False
    ).values('id', 'description', 'sender', 'amount', 'currency')
    # --- END MODIFIED ---

    if not unmatched_txs:
        print("   -> No unmatched transactions found. No hints to generate.")
        return 0

    # 3. Load into pandas DataFrame
    df = pd.DataFrame.from_records(unmatched_txs)

    # Clean data to avoid grouping 'N/A' or empty strings
    df['sender'] = df['sender'].replace(['N/A', 'nan', None], pd.NA)
    df = df.dropna(subset=['sender'])
    df = df[df['sender'].str.strip() != '']

    df['description'] = df['description'].replace(['N/A', 'nan', None], pd.NA)
    df = df.dropna(subset=['description'])
    df = df[df['description'].str.len() > 10] # Ignore short/generic descriptions

    if df.empty:
        print("   -> No valid data for hint analysis after cleaning.")
        return 0

    new_hints = []

    # 4. Run analyses
    try:
        _find_frequent_senders(df.copy(), declaration, new_hints)
    except Exception as e:
        print(f"   [Hint Engine Error] Failed _find_frequent_senders: {e}")

    try:
        _find_large_amount_outliers(df.copy(), declaration, new_hints)
    except Exception as e:
        print(f"   [Hint Engine Error] Failed _find_large_amount_outliers: {e}")

    try:
        _find_similar_descriptions(df.copy(), declaration, new_hints)
    except Exception as e:
        print(f"   [Hint Engine Error] Failed _find_similar_descriptions: {e}")

    # 5. Save new hints to database
    if new_hints:
        AnalysisHint.objects.bulk_create(new_hints)
        print(f"   -> Successfully created {len(new_hints)} new analysis hints.")
    else:
        print("   -> No significant patterns found.")

    return len(new_hints)
