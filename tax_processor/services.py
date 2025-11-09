# tax_processor/services.py

import os
import io
from django.db import transaction
from django.core.files.storage import default_storage
from django.core.files.base import ContentFile
from django.contrib.auth.models import User
from .models import Declaration, Statement, Transaction
from .parser_logic import (
    parse_transactions,
    normalize_transactions,
    extract_full_content_for_search,
    identify_bank_from_text,
    find_header_start_index,
    validate_statement_owner
)
import pandas as pd

def import_statement_service(uploaded_file, declaration_obj: Declaration, user: User):
    """
    Handles file saving, parsing, normalization, and saving transactions to the DB.
    """

    # --- 1. Prepare File Content ---
    filename = uploaded_file.name
    file_content = uploaded_file.read()

    temp_path = default_storage.save(f'temp_statements/{declaration_obj.pk}_{filename}', ContentFile(file_content))
    filepath = default_storage.path(temp_path)

    _, ext = os.path.splitext(filename)
    ext = ext.lower()

    transactions_count = 0

    try:
        with transaction.atomic():
            # --- 2. Run Pre-Parsing Logic ---
            file_content_for_search = extract_full_content_for_search(filepath, ext)
            if isinstance(file_content_for_search, list):
                search_content = ' '.join(file_content_for_search)
            else:
                search_content = file_content_for_search

            bank_name = identify_bank_from_text(search_content)
            header_index = find_header_start_index(file_content_for_search, ext)

            # --- Validation ---
            is_owner_valid = validate_statement_owner(
                file_content_for_search,
                declaration_obj.first_name,
                declaration_obj.last_name
            )
            if not is_owner_valid:
                return 0, f"Validation Error: Client name '{declaration_obj.first_name} {declaration_obj.last_name}' was not found in the file {filename}."

            if ext in ('.xls', '.xlsx'):
                content_source = io.BytesIO(file_content)
            else:
                content_source = filepath

            # 3. Call parse_transactions
            df_transactions = parse_transactions(
                content_source,
                ext,
                bank_name,
                header_index,
                filename
            )
            df_universal = normalize_transactions(df_transactions, bank_name, filename)

            if df_universal.empty:
                return 0, f"File {filename}: No transactions (in or out) found after parsing."

            # --- 4. Database Saving Logic ---
            statement = Statement.objects.create(
                declaration=declaration_obj,
                file_name=filename,
                bank_name=bank_name,
                status='PROCESSED'
            )

            transaction_objects = []
            for index, row in df_universal.iterrows():
                transaction_objects.append(
                    Transaction(
                        statement=statement,
                        transaction_date=row['Transaction_Date'],
                        provision_date=row['Provision_Date'],
                        date_from_description=row['date_from_description'] if pd.notna(row['date_from_description']) else None,
                        amount=row['Amount'],
                        currency=row['Currency'],
                        description=row['Description'],
                        sender=row['Sender'],
                        sender_account=row['Sender account number'],
                        is_expense=row['is_expense']
                    )
                )

            transactions_count = len(transaction_objects)
            Transaction.objects.bulk_create(transaction_objects)

            return transactions_count, f"Successfully imported {transactions_count} transactions (in and out)."

    # --- THIS BLOCK IS NOW CORRECTLY INDENTED ---
    except Exception as e:
        print(f"IMPORT CRITICAL ERROR for {filename}: {e}")
        traceback.print_exc()
        return 0, f"Import failed due to internal error: {e}"
    # --- END FIX ---

    finally:
        # --- 5. Clean up ---
        if default_storage.exists(temp_path):
             try:
                 default_storage.delete(temp_path)
             except Exception as e:
                 print(f"File Deletion Warning (WinError 32 likely): Could not delete {temp_path}. {e}")
