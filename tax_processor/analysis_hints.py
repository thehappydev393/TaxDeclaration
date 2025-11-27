# tax_processor/analysis_hints.py

import pandas as pd
import json
import os
import google.generativeai as genai
from rapidfuzz import process, fuzz
from django.db.models import Sum
from django.conf import settings
from .models import Transaction, Declaration, AnalysisHint
from collections import defaultdict
import time

# --- Configuration ---
MIN_SENDER_FREQUENCY = 5
LARGE_AMOUNT_THRESHOLD = 1000000
SIMILAR_DESC_THRESHOLD = 90
MIN_DESC_CLUSTER_SIZE = 3

# --- AI Configuration ---
API_KEY = getattr(settings, 'GEMINI_API_KEY', os.getenv('GEMINI_API_KEY'))

if API_KEY:
    genai.configure(api_key=API_KEY)
else:
    print("Warning: GEMINI_API_KEY not found. AI hints will be disabled.")


def _find_frequent_senders(df: pd.DataFrame, declaration: Declaration, new_hints: list):
    """
    Finds senders who appear frequently in the unmatched transaction list.
    """
    if 'sender' not in df.columns:
        return

    # Filter out empty/unknown senders just for this specific hint type
    valid_senders = df[
        (df['sender'].notna()) &
        (df['sender'] != '') &
        (df['sender'] != 'N/A')
    ]

    if valid_senders.empty:
        return

    sender_groups = valid_senders.groupby('sender').agg(
        transaction_ids=('id', list),
        count=('id', 'size')
    ).reset_index()

    frequent_senders = sender_groups[sender_groups['count'] >= MIN_SENDER_FREQUENCY]

    for _, row in frequent_senders.iterrows():
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

    large_txs = df[df['amount'] > LARGE_AMOUNT_THRESHOLD]

    for _, tx in large_txs.iterrows():
        sender_str = tx['sender'] if tx['sender'] and tx['sender'] != 'N/A' else "(Անհայտ)"
        new_hints.append(
            AnalysisHint(
                declaration=declaration,
                hint_type='AMOUNT',
                title=f"Խոշոր Գործարք (Large Amount): {tx['amount']:,.2f} {tx['currency']}",
                description=f"Ուղարկող՝ {sender_str}, Նկարագրություն՝ {tx['description'][:70]}...",
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

    # Filter out empty/short descriptions
    valid_desc_df = df[
        (df['description'].notna()) &
        (df['description'].str.len() > 3)
    ]

    if valid_desc_df.empty:
        return

    desc_to_ids_map = valid_desc_df.groupby('description')['id'].apply(list).to_dict()
    all_descs = list(desc_to_ids_map.keys())
    processed_descs = set()

    for desc in all_descs:
        if desc in processed_descs:
            continue

        # Use fuzz.WRatio for best text matching
        similar = process.extract(
            desc,
            all_descs,
            scorer=fuzz.WRatio,
            score_cutoff=SIMILAR_DESC_THRESHOLD,
            limit=50
        )

        cluster_descs = [s[0] for s in similar]

        if len(cluster_descs) >= MIN_DESC_CLUSTER_SIZE:
            cluster_tx_ids = []
            for s_desc in cluster_descs:
                if s_desc not in processed_descs:
                    cluster_tx_ids.extend(desc_to_ids_map[s_desc])
                    processed_descs.add(s_desc)

            if not cluster_tx_ids:
                continue

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

def _generate_ai_hints(df: pd.DataFrame, declaration: Declaration, new_hints: list):
    """
    Uses Gemini API to categorize distinct unmatched transactions.
    """
    if not API_KEY:
        return

    print("   -> Preparing AI analysis...")

    # 1. Filter for interesting items
    # Allow items with N/A sender, but ensure description exists
    candidates = df[
        (df['description'].notna()) &
        (df['description'].str.len() > 3)
    ].copy()

    # deduplicate by description to save tokens
    candidates = candidates.drop_duplicates(subset=['description'])

    if candidates.empty:
        return

    # 2. Batching
    BATCH_SIZE = 20
    batches = [candidates.iloc[i:i + BATCH_SIZE] for i in range(0, len(candidates), BATCH_SIZE)][:5]

    try:
        model = genai.GenerativeModel('gemini-2.5-flash')
    except:
        model = genai.GenerativeModel('gemini-flash') # Fallback

    safety_settings = [
        {"category": "HARM_CATEGORY_HARASSMENT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_HATE_SPEECH", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_SEXUALLY_EXPLICIT", "threshold": "BLOCK_NONE"},
        {"category": "HARM_CATEGORY_DANGEROUS_CONTENT", "threshold": "BLOCK_NONE"},
    ]

    for i, batch in enumerate(batches):
        print(f"      -> Processing AI batch {i+1}/{len(batches)}...")

        prompt_data = []
        for _, row in batch.iterrows():
            # Handle potential N/A in sender safely for JSON
            sender_val = row['sender'] if pd.notna(row['sender']) else "Unknown"

            prompt_data.append({
                "id": int(row['id']),
                "desc": row['description'],
                "sender": sender_val,
                "amt": f"{row['amount']} {row['currency']}"
            })

        prompt = f"""
        You are an expert tax accountant for Armenia.
        Analyze these bank transactions and suggest a specific tax category for each.
        Keep in mind that these are all incoming transactions.
        Please provide suggestions in Armenian language.

        Categories to consider (but suggest better ones if needed):
        - Professional Services Income
        - Salary
        - Office Rent
        - Software/Hosting Expense
        - Bank Fees
        - Dividends
        - Personal/Non-Business

        Also you can group and categorize them based on similarity of the description text.

        Input Data:
        {json.dumps(prompt_data, indent=2, ensure_ascii=False)}

        Return ONLY a JSON array. Format:
        [
            {{"id": 123, "category": "Category Name", "reason": "Short explanation"}}
        ]
        """

        try:
            response = model.generate_content(prompt, safety_settings=safety_settings)

            try:
                if not response.text:
                    print("      [AI Warning] Empty response received (possibly blocked).")
                    continue

                clean_text = response.text.replace('```json', '').replace('```', '').strip()
                suggestions = json.loads(clean_text)

                for item in suggestions:
                    tx_id = item.get('id')
                    category = item.get('category')
                    reason = item.get('reason')

                    if tx_id and category:
                        new_hints.append(
                            AnalysisHint(
                                declaration=declaration,
                                hint_type='AI_SUGGESTION',
                                title=f"🤖 AI Առաջարկ: {category}",
                                description=f"Պատճառաբանություն: {reason}",
                                related_transaction_ids=[tx_id],
                                is_resolved=False
                            )
                        )
            except ValueError:
                print(f"      [AI Warning] Response content error. Feedback: {response.prompt_feedback}")
            except json.JSONDecodeError:
                print(f"      [AI Warning] Failed to parse JSON response.")

            time.sleep(4)

        except Exception as e:
            print(f"      [AI Error] Batch failed: {e}")


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

    AnalysisHint.objects.filter(declaration=declaration).delete()

    # 2. Get all unmatched income transactions
    unmatched_txs = Transaction.objects.filter(
        statement__declaration=declaration,
        declaration_point__isnull=True,
        is_expense=False
    ).values('id', 'description', 'sender', 'amount', 'currency')

    if not unmatched_txs:
        print("   -> No unmatched transactions found. No hints to generate.")
        return 0

    # 3. Load into pandas DataFrame
    df = pd.DataFrame.from_records(unmatched_txs)

    # --- CRITICAL FIX: Relaxed Cleaning ---
    # Do NOT drop rows if sender is missing. Just normalize values.
    df['sender'] = df['sender'].replace(['N/A', 'nan', None], pd.NA)
    # df = df.dropna(subset=['sender'])  <-- REMOVED THIS LINE

    df['description'] = df['description'].replace(['N/A', 'nan', None], pd.NA)
    # df = df.dropna(subset=['description']) <-- REMOVED THIS LINE

    # Only fail if dataframe is truly empty (no IDs)
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

    # 5. Run AI Analysis
    try:
        _generate_ai_hints(df.copy(), declaration, new_hints)
    except Exception as e:
        print(f"   [Hint Engine Error] Failed _generate_ai_hints: {e}")

    # 6. Save new hints
    if new_hints:
        AnalysisHint.objects.bulk_create(new_hints)
        print(f"   -> Successfully created {len(new_hints)} new analysis hints.")
    else:
        print("   -> No significant patterns found.")

    return len(new_hints)
