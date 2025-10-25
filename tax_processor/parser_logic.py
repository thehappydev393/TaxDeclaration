import os
import pandas as pd
from pypdf import PdfReader
from typing import Dict, Any, List, Union
import re
import gc
import shutil
import io

# --- PDF Dependency Check (unchanged) ---
try:
    import camelot
except ImportError:
    print("Warning: Camelot not installed. PDF parsing will fail. Please install it and Ghostscript.")

# --- Configuration Constants ---
BANK_KEYWORDS: Dict[str, List[str]] = {
    'ACBA Bank': ['ACBA', 'Akba', 'ACBA BANK OJSC'],
    'Ameriabank': ['Ameria', 'Ameriabank', 'AMERIABANK CJSC'],
    'Ardshinbank': ['Ardshinbank', 'Ardshin'],
    'Armswissbank': ['Armswiss', 'Armswissbank'],
    'Artsakhbank': ['Artsakhbank', 'Artsakh'],
    'Byblos Bank Armenia': ['Byblos Bank', 'Byblos', 'BBA'],
    'Converse Bank': ['Converse Bank', 'Converse', 'CBA'],
    'Evocabank': ['Evocabank', 'Evoca'],
    'HSBC Bank Armenia': ['HSBC Armenia', 'HSBC Bank'],
    'IDBank': ['IDBank', 'ID Bank', 'Idram', 'Իդրամ'],
    'InecoBank': ['InecoBank', 'Ineco', 'InectoBank', 'Ինեկո'],
    'Unibank': ['Unibank', 'Uni Bank'],
    'FastBank': ['fastbank', 'fast bank', 'ՖԱՍԹ ԲԱՆԿ'],
    'AEB': ['AEB', 'ArmEconomBank', 'ՀԱՅԷԿՈՆՈՄԲԱՆԿ'],
}

HEADER_KEYWORDS_DATE = ['ամսաթիվ', 'date', 'օր']
HEADER_KEYWORDS_AMOUNT = ['գումար', 'amount', 'դեբետ', 'կրեդիտ', 'մուտք', 'ելք', 'daily balance']
MAX_HEADER_SEARCH_ROWS = 50

UNIVERSAL_HEADERS = [
    'Bank_Name',
    'Bank_File_Name',
    'Transaction_Date',
    'Provision_Date',
    'Amount',
    'Currency',
    'Description',
    'Transaction_Place',
    'Sender',
    'Sender account number',
]


# ------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------

def identify_bank_from_text(text_content: str) -> str:
    """Identifies the bank name from the extracted header text."""
    text_content_lower = text_content.lower()
    for bank_name, keywords in BANK_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in text_content_lower:
                return bank_name
    return 'Unknown Bank'

def extract_full_content_for_search(filepath: str, file_extension: str) -> Union[List[str], str]:
    """Extracts content required for bank identification and header search."""
    try:
        if file_extension in ('.xls', '.xlsx'):
            df = pd.read_excel(filepath, sheet_name=0, header=None, nrows=MAX_HEADER_SEARCH_ROWS, dtype=str)
            content = [" ".join(row.dropna().astype(str).values) for _, row in df.iterrows()]
            return content
        elif file_extension == '.pdf':
            reader = PdfReader(filepath)
            if reader.is_encrypted:
                return ""
            all_text = reader.pages[0].extract_text() or "" if reader.pages else ""
            return all_text.strip()
    except Exception as e:
        return ""
    return ""

def find_header_start_index(content: Union[List[str], str], extension: str) -> int:
    """Heuristically finds the row/line index where the transaction header starts."""
    lines = content if isinstance(content, list) else content.split('\n')

    MULTI_HEADER_PARENTS = [
        'գործարքներ, այլ գործառնություններ',
        'գործարքի գումար հաշվի արժույթով',
        'գործարքի գումար քարտի արժույթով',
        'գործարքի գումար'
    ]

    for i, line in enumerate(lines):
        normalized_line = ' '.join(line.lower().split())
        has_date = any(kw in normalized_line for kw in HEADER_KEYWORDS_DATE)
        has_amount = any(kw in normalized_line for kw in HEADER_KEYWORDS_AMOUNT)

        if has_date and has_amount:
            return i

        if any(parent in normalized_line for parent in MULTI_HEADER_PARENTS):
            if i + 1 < len(lines):
                 next_line = ' '.join(lines[i+1].lower().split())
                 if 'մուտք' in next_line or 'ելք' in next_line:
                     return i

    return -1

def flatten_headers(multiindex_cols):
    """
    Combines MultiIndex headers into a single, clean, and unique string.
    """
    new_cols = []
    seen_cols = {}

    for col in multiindex_cols:
        cleaned_parts = [re.sub(r'[\s\W_]+', '', str(c).lower().replace('\n', '')) for c in col if pd.notna(c) and str(c).strip()]

        final_col = ''

        if len(cleaned_parts) >= 2:
            child = cleaned_parts[-1]
            parent = cleaned_parts[0]

            final_col = f'{parent}_{child}'

            if 'գործարքներայլգործառնություններ' in parent or 'գործարքիգումարքարտիարժույթով' in parent:
                 final_col = child
                 if not final_col: final_col = parent

            if 'նկարագրություն' in parent and not child:
                 final_col = parent

        elif len(cleaned_parts) == 1:
            final_col = cleaned_parts[0]
        else:
            final_col = f'unnamed_{len(new_cols)}'

        original_col = final_col
        count = seen_cols.get(original_col, 0)

        if count > 0:
            final_col = f'{original_col}_{count}'

        seen_cols[original_col] = count + 1
        new_cols.append(final_col)

    return new_cols


# ------------------------------------------------------------------------
# Main Parsing Function (HYBRID: Reads from BytesIO or FilePath)
# ------------------------------------------------------------------------

def parse_transactions(content_source: Union[str, io.BytesIO], extension: str, bank_name: str, header_index: int, filename: str) -> pd.DataFrame:
    """
    Parses the file using content_source (BytesIO for Excel, filepath for PDF).
    """
    print(f"   -> Loading transaction data. Identified bank: {bank_name}...")

    if extension in ('.xls', '.xlsx'):
        df = pd.DataFrame()

        if isinstance(content_source, str):
            with open(content_source, 'rb') as f:
                excel_content = io.BytesIO(f.read())
        else:
            excel_content = content_source

        try:
            sheet_name = 0

            excel_file = pd.ExcelFile(excel_content)

            try:
                sheet_names = excel_file.sheet_names
                if 'քաղվածք' in [name.lower() for name in sheet_names]:
                    sheet_name = [name for name in sheet_names if name.lower() == 'քաղվածք'][0]
            except Exception:
                pass

            excel_content.seek(0)

            h_index = header_index if header_index is not None and header_index >= 0 else 0

            if bank_name in ['ACBA Bank', 'Evocabank', 'FastBank', 'AEB']:
                 df = pd.read_excel(excel_content, sheet_name=sheet_name, header=[h_index, h_index + 1], dtype=str)
                 df.columns = flatten_headers(df.columns)
                 print(f"   -> Mode: Multi-Row Headers (Index {h_index} and {h_index + 1})")

            else:
                 df = pd.read_excel(excel_content, sheet_name=sheet_name, header=h_index, dtype=str)
                 print(f"   -> Mode: Single Header Row (Index {h_index})")

            return df
        except Exception as e:
            print(f"   [Error] Failed to read Excel data: {e}")
            return pd.DataFrame()

    elif extension == '.pdf':
        # PDF logic uses content_source as the filepath
        try:
            tables = []

            reader = PdfReader(content_source)
            total_pages = len(reader.pages)

            all_extracted_tables = []

            for page_num in range(1, total_pages + 1):
                page_str = str(page_num)

                # Page 1: Use lattice for complex header structure
                if page_num == 1:
                    tables = camelot.read_pdf(content_source, pages=page_str, flavor='lattice')
                # Page 2 onwards: Use stream for continuous, less structured data rows
                else:
                    tables = camelot.read_pdf(content_source, pages=page_str, flavor='stream')

                all_extracted_tables.extend(tables)


            if not all_extracted_tables:
                return pd.DataFrame()


            processed_dfs = []
            initial_headers = None
            initial_table_index = -1

            for i, table in enumerate(all_extracted_tables):
                if not table.df.empty:
                    df = table.df

                    if initial_headers is None:
                        initial_headers = df.iloc[0].astype(str)
                        df.columns = initial_headers
                        initial_table_index = i

                        if bank_name == 'IDBank':
                            # IDBank/Idram: Skip header (0) and summary row (1). Data starts from index 2.
                            df = df.iloc[2:].reset_index(drop=True)
                        else:
                            # Generic PDF: Skip header (0) only. Data starts from index 1.
                            df = df.iloc[1:].reset_index(drop=True)

                        processed_dfs.append(df)
                        break

            if initial_headers is None:
                return pd.DataFrame()


            for i in range(len(all_extracted_tables)):
                if i != initial_table_index:
                    df_rest = all_extracted_tables[i].df

                    if not df_rest.empty:
                        expected_cols = initial_headers.shape[0]

                        if df_rest.iloc[0].equals(initial_headers):
                            df_rest = df_rest.iloc[1:].reset_index(drop=True)

                        if df_rest.shape[1] == expected_cols:
                             df_rest.columns = initial_headers
                             processed_dfs.append(df_rest)
                        elif df_rest.shape[1] > expected_cols:
                             df_rest = df_rest.iloc[:, :expected_cols]
                             df_rest.columns = initial_headers
                             processed_dfs.append(df_rest)

            final_df = pd.concat(processed_dfs, ignore_index=True) if processed_dfs else pd.DataFrame()

            return final_df

        except Exception as e:
            print(f"   [Error] Failed to read PDF data with Camelot: {e}")
            return pd.DataFrame()

    return pd.DataFrame()


# ------------------------------------------------------------------------
# Core Normalization Logic
# ------------------------------------------------------------------------

def normalize_transactions(df: pd.DataFrame, bank_name: str, filename: str) -> pd.DataFrame:
    """
    Transforms and filters the data into the FINAL universal structure,
    keeping only incoming transactions (Amount > 0 in the correct column).
    """
    if df.empty: return pd.DataFrame(columns=UNIVERSAL_HEADERS)

    # Pre-fetch helper functions for this block
    def find_column(keys):
        for key in keys:
            if key in df.columns: return key
        return None

    def create_placeholder(value='N/A'):
        return pd.Series([value] * len(df), index=df.index).astype(str)

    universal_df = pd.DataFrame(index=df.index, columns=UNIVERSAL_HEADERS)
    universal_df['Bank_Name'] = bank_name
    universal_df['Bank_File_Name'] = filename

    # 1. Clean column names (CRITICAL for PDF/Excel)
    cleaned_df_columns = {}
    for col in df.columns:
        if pd.isna(col) or str(col).strip() == '':
            # PDF FIX: Rename the empty header column (Credit/Inflow in IDBank PDF)
            cleaned_col = 'idbank_raw_credit_column'
        elif pd.notna(col):
            # Normalization: remove all non-alphanumeric, convert to lowercase, remove newlines
            cleaned_col = re.sub(r'[\s\W_]+', '', str(col).lower().replace('\n', ''))

        cleaned_df_columns[col] = cleaned_col

    df.rename(columns=cleaned_df_columns, inplace=True)

    # 2. Re-enforce Column Uniqueness (Prevent AttributeError)
    new_cols = []
    seen = {}
    for col in df.columns:
        original_col = col
        if col in seen:
            new_name = f"{col}_{seen[col]}"
            df.rename(columns={original_col: new_name}, inplace=True)
            seen[col] += 1
            new_cols.append(new_name)
        else:
            seen[col] = 1
            new_cols.append(original_col)
    df.columns = new_cols


    # 3. Define internal column lookup maps
    column_maps = {
        'transaction_date': ['ամսաթիվ', 'գործարքիամսաթիվ', 'transactiondate', 'օր', 'հաշվառմանամսաթիվ'],
        'provision_date': ['ձևակերպմանհաշվարկիապահովմանամսաթիվ', 'provisiondate'],

        'description': [
            'նկարագրություն', 'մեկնաբանություն', 'նպատակ', 'բացատրություն', 'details',
            'գործարքնկարագրություն', 'գործարքինկարագրություն',
            'գործարքինկարագրությունunnamed12level1',
            'գործարքնկարագիր', # IDBank PDF addition ('Գործարք նկարագիր')
        ],
        'transaction_place': [
            'գործարքիվայրը', 'գործարքիվայրը1',
        ],

        # PRIORITY 1: Explicit Multi-Row Inflow Column (ACBA Account/Card)
        'explicit_inflow': ['գործարքիգումարhաշվիարժույթովմուտք', 'գործարքիգումարըքարտիարժույթովմուտք'],

        # PRIORITY 2: Generic Dual-Column Inflow (Ameria/Ineco/IDBank PDF)
        'credit': ['մուտքamd', 'մուտք', 'credit', 'inflow', 'կրեդիտ', 'idbank_raw_credit_column'],

        # PRIORITY 3: Single Amount/Sign Column (Fallback/Other)
        'single_amount_sign': ['գործարքիգումարքարտիարժույթով', 'գործարքիգումարհաշվիարժույթով', 'amount', 'գործարքիգումարը'],

        'sender': ['շահառուվճարող', 'շահառու', 'վճարող', 'sendername'],
        'sender_account': ['շահառույիվճարողիհաշիվ', 'հաշիվ', 'accountnumber'],
        'currency_col': ['արժույթ', 'currency', 'քարտիարժույթով', 'հաշվիարժույթով']
    }

    # --- FINDING COLUMNS USING SUBSTRING SEARCH (Original Logic) ---
    desc_cols_found = []
    for keyword in column_maps['description']:
        desc_cols_found.extend([
            col for col in df.columns
            if keyword in col
        ])
    desc_cols_found = sorted(list(set(desc_cols_found)), key=desc_cols_found.index)

    # 4a. Date Mapping
    transaction_date_col = find_column(column_maps['transaction_date'])
    provision_date_col = find_column(column_maps['provision_date'])

    # Date formats (Includes PDF fix)
    DATE_FORMATS = ['%Y-%m-%d %H:%M:%S', '%d.%m.%Y', '%d.%m.%Y %H:%M:%S', '%m/%d/%Y', '%m/%d/%Y %H:%M:%S', '%Y.%m.%d', '%d/%m/%Y', '%d/%m/%Y %H:%M']

    def robust_date_parser(col):
        for fmt in DATE_FORMATS:
            parsed_dates = pd.to_datetime(col.astype(str).str.strip(), format=fmt, errors='coerce')
            if not parsed_dates.isna().all():
                return parsed_dates

        try:
             numeric_col = pd.to_numeric(col, errors='coerce')
             if numeric_col.max() > 40000:
                valid_indices = ~numeric_col.isna()
                dates_as_floats = numeric_col[valid_indices]
                converted_dates = pd.to_datetime(dates_as_floats, unit='D', origin='1899-12-30', errors='coerce')
                result = pd.Series(pd.NaT, index=col.index)
                result.loc[valid_indices] = converted_dates.values
                return result
        except:
             pass

        return pd.to_datetime(col, errors='coerce', dayfirst=True)

    if transaction_date_col: universal_df['Transaction_Date'] = robust_date_parser(df[transaction_date_col])
    if provision_date_col: universal_df['Provision_Date'] = robust_date_parser(df[provision_date_col])

    if transaction_date_col and not provision_date_col: universal_df['Provision_Date'] = universal_df['Transaction_Date']
    elif provision_date_col and not transaction_date_col: universal_df['Transaction_Date'] = universal_df['Provision_Date']


    # 4b. Amount Determination (STRICT INFLOW LOGIC)

    credit_col = find_column(column_maps['credit'])
    amount_col_to_use = None

    # PRIORITY 1: Explicit Inflow (ACBA Account/Card Multi-Row)
    if find_column(column_maps['explicit_inflow']) and find_column(column_maps['explicit_inflow']) in df.columns:
        amount_col_to_use = find_column(column_maps['explicit_inflow'])
        print(f"   -> Amount logic: Using EXPLICIT INFLOW column '{amount_col_to_use}' for INFLOW.")

    # PRIORITY 2: Dedicated Credit Column (Ameria/Ineco/IDBank PDF)
    elif credit_col and credit_col in df.columns:
        amount_col_to_use = credit_col
        print(f"   -> Amount logic: Using DEDICATED CREDIT column '{amount_col_to_use}' for INFLOW.")

    # PRIORITY 3: Single Amount/Sign Column (ACBA Card/Evocabank/Fallback)
    elif find_column(column_maps['single_amount_sign']) and find_column(column_maps['single_amount_sign']) in df.columns:
        amount_col_to_use = find_column(column_maps['single_amount_sign'])
        print(f"   -> Amount logic: Using SINGLE AMOUNT/SIGN column '{amount_col_to_use}'.")


    if amount_col_to_use:
        selection = df[amount_col_to_use]

        # --- Handle AttributeError by ensuring Series selection ---
        if isinstance(selection, pd.DataFrame):
            print(f"   ⚠️ Ambiguity detected: Amount column '{amount_col_to_use}' returned a DataFrame. Using first column.")
            amount_series = pd.Series(selection.iloc[:, 0].values, index=df.index).astype(str)
        else:
            amount_series = selection.astype(str)

        # Clean amount column and convert to numeric
        amounts = amount_series

        # --- CRITICAL FIX: Robust, Targeted Amount Cleaning ---
        if bank_name == 'Evocabank':
            # 1. Remove all thousands grouping periods (if present, though uncommon with comma decimal)
            amounts = amounts.str.replace('.', '', regex=False)
            # 2. Replace comma (decimal separator) with period
            amounts = amounts.str.replace(',', '.', regex=False)
            # 3. Keep only digits, period, and minus sign
            amounts = amounts.str.replace(r'[^\d\.\-]', '', regex=True)
        else:
            # Standard logic (Covers IDBank, ACBA, Ameria, Ineco, etc.)
            # 1. Remove all commas (assumed thousands/grouping separators)
            amounts = amounts.str.replace(',', '', regex=False)
            # 2. Keep only digits, periods (assumed decimal), and minus sign
            amounts = amounts.str.replace(r'[^\d\.\-]', '', regex=True)
        # ----------------------------------------------------------------

        credit_amounts = pd.to_numeric(amounts, errors='coerce').fillna(0)

        # FINAL LOGIC: Only keep positive values (since we are targeting incoming amounts)
        universal_df['Amount'] = credit_amounts.apply(lambda x: x if x > 0 else 0)

    else:
        universal_df['Amount'] = pd.Series([0] * len(df), index=df.index)
        print("   ❌ Amount logic: Could not find the recognized 'credit' column.")

    # 5. Filtering (Keep only rows where calculated Amount > 0)
    initial_rows = len(universal_df)

    universal_df = universal_df[universal_df['Amount'] > 0].copy()
    print(f"   -> Filtered: Kept {len(universal_df)} of {initial_rows} rows (Incoming only).")

    # 6. Currency Detection (unchanged)
    currency_col = find_column(column_maps['currency_col'])
    inferred_currency = 'AMD'

    if currency_col:
        currency_val = df[currency_col].dropna().iloc[0] if not df[currency_col].dropna().empty else inferred_currency
        inferred_currency = currency_val.upper()
    elif credit_col and 'amd' in credit_col: inferred_currency = 'AMD'
    elif credit_col and 'usd' in credit_col: inferred_currency = 'USD'
    universal_df['Currency'] = inferred_currency

    # 7. Description, Sender, and Account Mapping
    if desc_cols_found:
        print(f"   -> Description logic: Combining {len(desc_cols_found)} description column(s).")
        description_data = df[desc_cols_found].astype(str).copy()
        description_data.replace('nan', '', inplace=True)
        universal_df['Description'] = description_data.apply(
            lambda row: ' '.join(row.values).strip(), axis=1
        ).str.replace(r'\s{2,}', ' ', regex=True)

    else:
        universal_df['Description'] = create_placeholder()
        print("   ❌ Description logic: Could not find any recognized description column.")


    sender_col = find_column(column_maps['sender'])
    universal_df['Sender'] = df[sender_col].astype(str) if sender_col in df.columns else create_placeholder()

    acc_col = find_column(column_maps['sender_account'])
    universal_df['Sender account number'] = df[acc_col].astype(str) if acc_col in df.columns else create_placeholder()

    place_cols_found = [col for keyword in column_maps['transaction_place'] for col in df.columns if keyword in col]
    if place_cols_found:
        place_data = df[place_cols_found].astype(str).copy()
        place_data.replace('nan', '', inplace=True)
        universal_df['Transaction_Place'] = place_data.apply(
            lambda row: ' '.join(row.values).strip(), axis=1
        ).str.replace(r'\s{2,}', ' ', regex=True)
    else:
        universal_df['Transaction_Place'] = create_placeholder()

    # 8. Final cleanup and logging
    final_df = universal_df.dropna(subset=['Transaction_Date', 'Amount']).copy()

    if len(final_df) < len(universal_df):
        print(f"   ❌ Final Date/Amount check dropped {len(universal_df) - len(final_df)} rows due to invalid data format.")

    print(f"   ✅ Final normalized size: {len(final_df)} incoming transactions.")

    return final_df
