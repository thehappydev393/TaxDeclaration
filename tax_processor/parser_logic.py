# tax_processor/parser_logic.py

import io
import os
import gc  # Standard library
import re
import shutil
from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd  # Third-party
from pypdf import PdfReader

# --- PDF Dependency Check ---
try:
    import camelot
except ImportError:
    print(
        "Warning: Camelot not installed. PDF parsing will fail. "
        "Please install it and Ghostscript."
    )

# --- Configuration Constants ---
BANK_KEYWORDS: Dict[str, List[str]] = {
    "ACBA Bank": ["ACBA", "Akba", "ACBA BANK OJSC"],
    "Ameriabank": ["Ameria", "Ameriabank", "AMERIABANK CJSC"],
    "Ardshinbank": ["Ardshinbank", "Ardshin"],
    "Armswissbank": ["Armswiss", "Armswissbank"],
    "Artsakhbank": ["Artsakhbank", "Artsakh"],
    "Byblos Bank Armenia": ["Byblos Bank", "Byblos", "BBA"],
    "Converse Bank": ["Converse Bank", "Converse", "CBA"],
    "Evocabank": ["Evocabank", "Evoca"],
    "HSBC Bank Armenia": ["HSBC Armenia", "HSBC Bank"],
    "IDBank": ["IDBank", "ID Bank", "Idram", "Իդրամ"],
    "InecoBank": ["InecoBank", "Ineco", "InectoBank", "Ինեկո"],
    "Unibank": ["Unibank", "Uni Bank"],
    "FastBank": ["fastbank", "fast bank", "ՖԱՍԹ ԲԱՆԿ"],
    "AEB": ["AEB", "ArmEconomBank", "ՀԱՅԷԿՈՆՈՄԲԱՆԿ"],
}

HEADER_KEYWORDS_DATE = ["ամսաթիվ", "date", "օր"]
HEADER_KEYWORDS_AMOUNT = [
    "գումար",
    "amount",
    "դեբետ",
    "կրեդիտ",
    "մուտք",
    "ելք",
    "daily balance",
]
MAX_HEADER_SEARCH_ROWS = 50

UNIVERSAL_HEADERS = [
    "Bank_Name",
    "Bank_File_Name",
    "Transaction_Date",
    "Provision_Date",
    "date_from_description",
    "Amount",
    "Currency",
    "is_expense",
    "Description",
    "Transaction_Place",
    "Sender",
    "Sender account number",
    "excel_row_number",
]

# --- Month and Date Regex Definitions ---
MONTH_MAP = {
    # Armenian (Nominative)
    "հունվար": 1,
    "հնվ": 1,
    "փետրվար": 2,
    "փտր": 2,
    "մարտ": 3,
    "մրտ": 3,
    "ապրիլ": 4,
    "ապր": 4,
    "մայիս": 5,
    "մյս": 5,
    "հունիս": 6,
    "հնս": 6,
    "հուլիս": 7,
    "հլս": 7,
    "օգոստոս": 8,
    "օգս": 8,
    "սեպտեմբեր": 9,
    "սպտ": 9,
    "հոկտեմբեր": 10,
    "հկտ": 10,
    "նոյեմբեր": 11,
    "նմբ": 11,
    "դեկտեմբեր": 12,
    "դկտ": 12,
    # Armenian (Genitive, e.g., "հունվարի 25")
    "հունվարի": 1,
    "փետրվարի": 2,
    "մարտի": 3,
    "ապրիլի": 4,
    "մայիսի": 5,
    "հունիսի": 6,
    "հուլիսի": 7,
    "օգոստոսի": 8,
    "սեպտեմբերի": 9,
    "հոկտեմբերի": 10,
    "նոյեմբերի": 11,
    "դեկտեմբերի": 12,
    # Russian (Nominative)
    "январь": 1,
    "янв": 1,
    "февраль": 2,
    "фев": 2,
    "март": 3,
    "мар": 3,
    "апрель": 4,
    "апр": 4,
    "май": 5,
    "мая": 5,
    "июнь": 6,
    "июн": 6,
    "июль": 7,
    "июл": 7,
    "август": 8,
    "авг": 8,
    "сентябрь": 9,
    "сен": 9,
    "октябрь": 10,
    "окт": 10,
    "ноябрь": 11,
    "ноя": 11,
    "декабрь": 12,
    "дек": 12,
    # Russian (Genitive, e.g., "25 января")
    "января": 1,
    "февраля": 2,
    "марта": 3,
    "апреля": 4,
    # 'мая' is already present
    "июня": 6,
    "июля": 7,
    "августа": 8,
    "сентября": 9,
    "октября": 10,
    "ноября": 11,
    "декабря": 12,
    # English
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}

DATE_REGEX_DMY = re.compile(r"(\d{1,2})[\./-](\d{1,2})[\./-](\d{2,4})")
CHAR_SET = r"[a-zа-яա-ֆ]+"
MONTH_YEAR_REGEX = re.compile(rf"({CHAR_SET})[\s,]+(\d{{4}})", re.IGNORECASE)
DAY_MONTH_REGEX = re.compile(rf"(\d{{1,2}})[\s,]+({CHAR_SET})", re.IGNORECASE)
MONTH_DAY_REGEX = re.compile(rf"({CHAR_SET})[\s,]+(\d{{1,2}})", re.IGNORECASE)


# ------------------------------------------------------------------------
# Helper Functions
# ------------------------------------------------------------------------
def _parse_date_from_description(description, transaction_date):
    if not description or pd.isna(description):
        return None
    desc_lower = description.lower()
    try:
        match = DATE_REGEX_DMY.search(desc_lower)
        if match:
            day, month, year = (
                int(match.group(1)),
                int(match.group(2)),
                int(match.group(3)),
            )
            if year < 100:
                year += 2000
            if 1 <= month <= 12 and 1 <= day <= 31:
                return datetime(year, month, day).date()
        match = MONTH_YEAR_REGEX.search(desc_lower)
        if match:
            month_str, year_str = match.group(1), match.group(2)
            month = MONTH_MAP.get(month_str)
            year = int(year_str)
            if month:
                return datetime(year, month, 1).date()
        match = DAY_MONTH_REGEX.search(desc_lower)
        if match:
            day_str, month_str = match.group(1), match.group(2)
            month = MONTH_MAP.get(month_str)
            day = int(day_str)
            if month and 1 <= day <= 31:
                year = transaction_date.year
                if transaction_date.month == 1 and month == 12:
                    year -= 1
                return datetime(year, month, day).date()
        match = MONTH_DAY_REGEX.search(desc_lower)
        if match:
            month_str, day_str = match.group(1), match.group(2)
            month = MONTH_MAP.get(month_str)
            day = int(day_str)
            if month and 1 <= day <= 31:
                year = transaction_date.year
                if transaction_date.month == 1 and month == 12:
                    year -= 1
                return datetime(year, month, day).date()
    except Exception:
        return None
    return None


def identify_bank_from_text(text_content: str) -> str:
    text_content_lower = text_content.lower()
    for bank_name, keywords in BANK_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in text_content_lower:
                return bank_name
    return "Unknown Bank"


def extract_full_content_for_search(
    filepath: str, file_extension: str
) -> Union[List[str], str]:
    try:
        if file_extension in (".xls", ".xlsx"):
            df = pd.read_excel(
                filepath,
                sheet_name=0,
                header=None,
                nrows=MAX_HEADER_SEARCH_ROWS,
                dtype=str,
            )
            content = [
                " ".join(row.dropna().astype(str).values)
                for _, row in df.iterrows()
            ]
            return content
        elif file_extension == ".pdf":
            reader = PdfReader(filepath)
            if reader.is_encrypted:
                return ""
            all_text = (
                reader.pages[0].extract_text() or "" if reader.pages else ""
            )
            return all_text.strip()
    except Exception as e:
        return ""
    return ""


def find_header_start_index(
    content: Union[List[str], str], extension: str
) -> (int, bool):
    lines = content if isinstance(content, list) else content.split("\n")
    MULTI_HEADER_PARENTS = [
        "գործարքներ, այլ գործառնություններ",
        "գործարքի գումար հաշվի արժույթով",
        "գործարքի գումար քարտի արժույթով",
        "գործարքի գումար",
    ]

    for i, line in enumerate(lines):
        normalized_line = " ".join(line.lower().split())
        if any(parent in normalized_line for parent in MULTI_HEADER_PARENTS):
            if i + 1 < len(lines):
                next_line = " ".join(lines[i + 1].lower().split())
                if "մուտք" in next_line or "ելք" in next_line:
                    return i, True
        has_date = any(kw in normalized_line for kw in HEADER_KEYWORDS_DATE)
        has_amount = any(kw in normalized_line for kw in HEADER_KEYWORDS_AMOUNT)
        if has_date and has_amount:
            return i, False
    return -1, False


def flatten_headers(multiindex_cols):
    new_cols = []
    seen_cols = {}
    for col in multiindex_cols:
        cleaned_parts = [
            re.sub(r"[\s\W_]+", "", str(c).lower().replace("\n", ""))
            for c in col
            if pd.notna(c) and str(c).strip()
        ]
        final_col = ""
        if len(cleaned_parts) >= 2:
            child = cleaned_parts[-1]
            parent = cleaned_parts[0]
            final_col = f"{parent}_{child}"
            if not child:
                final_col = parent
        elif len(cleaned_parts) == 1:
            final_col = cleaned_parts[0]
        else:
            final_col = f"unnamed_{len(new_cols)}"
        original_col = final_col
        count = seen_cols.get(original_col, 0)
        if count > 0:
            final_col = f"{original_col}_{count}"
        seen_cols[original_col] = count + 1
        new_cols.append(final_col)
    return new_cols


def validate_statement_owner(
    content_for_search: Union[List[str], str],
    client_first_name: str,
    client_last_name: str,
) -> bool:
    if not client_first_name or not client_last_name:
        return True
    if isinstance(content_for_search, list):
        content_str = " ".join(content_for_search).lower()
    else:
        content_str = content_for_search.lower()
    fn = client_first_name.lower()
    ln = client_last_name.lower()
    if fn in content_str and ln in content_str:
        return True
    print(f"   [Validation Error] Client '{fn} {ln}' not found in statement.")
    return False


# ------------------------------------------------------------------------
# Main Parsing Function
# ------------------------------------------------------------------------
def parse_transactions(
    content_source: Union[str, io.BytesIO],
    extension: str,
    bank_name: str,
    header_index: int,
    is_multi_row: bool,
    filename: str,
) -> pd.DataFrame:
    print(f"   -> Loading transaction data. Identified bank: {bank_name}...")
    if extension in (".xls", ".xlsx"):
        df = pd.DataFrame()
        if isinstance(content_source, str):
            with open(content_source, "rb") as f:
                excel_content = io.BytesIO(f.read())
        else:
            excel_content = content_source
        try:
            sheet_name = 0
            excel_file = pd.ExcelFile(excel_content)

            try:
                sheet_names = excel_file.sheet_names
                print(f"   [DEBUG] Found sheet names: {sheet_names}")
                if "քաղվածք" in [name.lower() for name in sheet_names]:
                    sheet_name = [
                        name
                        for name in sheet_names
                        if name.lower() == "քաղվածք"
                    ][0]
                    print(f"   [DEBUG] Found 'քաղվածք' sheet. Using it.")
                else:
                    print(f"   [DEBUG] No 'քաղվածք' sheet. Using default sheet 0.")
            except Exception as e:
                print(f"   [DEBUG] Error checking sheet names: {e}. Using default.")

            excel_content.seek(0)
            h_index = (
                header_index if header_index is not None and header_index >= 0 else 0
            )

            if is_multi_row:
                df = pd.read_excel(
                    excel_content,
                    sheet_name=sheet_name,
                    header=[h_index, h_index + 1],
                    dtype=str,
                )
                df.columns = flatten_headers(df.columns)
                data_row_offset = h_index + 2
                df["original_excel_row"] = df.index + data_row_offset
                print(
                    f"   -> Mode: Multi-Row Headers (Index {h_index} and "
                    f"{h_index + 1}). Data offset: {data_row_offset}"
                )
            else:
                df = pd.read_excel(
                    excel_content,
                    sheet_name=sheet_name,
                    header=h_index,
                    dtype=str,
                )
                data_row_offset = h_index + 1
                df["original_excel_row"] = df.index + data_row_offset
                print(
                    f"   -> Mode: Single Header Row (Index {h_index}). "
                    f"Data offset: {data_row_offset}"
                )

            return df
        except Exception as e:
            print(f"   [Error] Failed to read Excel data: {e}")
            return pd.DataFrame()
    elif extension == ".pdf":
        try:
            tables = []
            reader = PdfReader(content_source)
            total_pages = len(reader.pages)
            all_extracted_tables = []
            for page_num in range(1, total_pages + 1):
                page_str = str(page_num)
                if page_num == 1:
                    tables = camelot.read_pdf(
                        content_source, pages=page_str, flavor="lattice"
                    )
                else:
                    tables = camelot.read_pdf(
                        content_source, pages=page_str, flavor="stream"
                    )
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
                        if bank_name == "IDBank":
                            df = df.iloc[2:].reset_index(drop=True)
                        else:
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
            final_df = (
                pd.concat(processed_dfs, ignore_index=True)
                if processed_dfs
                else pd.DataFrame()
            )
            return final_df
        except Exception as e:
            print(f"   [Error] Failed to read PDF data with Camelot: {e}")
            return pd.DataFrame()

    return pd.DataFrame()


# ------------------------------------------------------------------------
# Core Normalization Logic
# ------------------------------------------------------------------------
def normalize_transactions(
    df: pd.DataFrame, bank_name: str, filename: str
) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=UNIVERSAL_HEADERS)

    # --- HELPER FUNCTIONS ---
    def find_column(keys):
        for key in keys:
            if key in df.columns:
                return key
        return None

    def find_column_by_substring(keys):
        for key in keys:
            for col in df.columns:
                if key in col:
                    return col
        return None

    def create_placeholder(value="N/A"):
        return pd.Series([value] * len(df), index=df.index).astype(str)

    def clean_amount_series(amount_series, bank_name, column_name=""):
        if amount_series is None:
            return pd.Series(0, index=df.index)
        if isinstance(amount_series, pd.DataFrame):
            print(
                f"   [DEBUG] Ambiguity detected in amount column. "
                f"Using first column."
            )
            s = pd.Series(
                amount_series.iloc[:, 0].values, index=df.index
            ).astype(str)
        else:
            s = amount_series.astype(str)
        print(
            f"   [DEBUG] clean_amount_series for '{column_name}' (BEFORE): "
            f"\n{s.head()}"
        )
        if bank_name == "Evocabank":
            s = s.str.replace(".", "", regex=False)
            s = s.str.replace(",", ".", regex=False)
            s = s.str.replace(
                r"[^\d\.\-]", "", regex=True
            )
        else:
            s = s.str.replace(
                r"[()\,\s\xa0]", "", regex=True
            )
            s = s.str.replace(r"[^\d\.\-]", "", regex=True)
        print(
            f"   [DEBUG] clean_amount_series for '{column_name}' (AFTER): "
            f"\n{s.head()}"
        )
        numeric_s = pd.to_numeric(s, errors="coerce").fillna(0)
        print(
            f"   [DEBUG] clean_amount_series for '{column_name}' (NUMERIC): "
            f"\n{numeric_s.head()}"
        )
        return numeric_s

    # --- START NEW HELPER FOR LOGGING ---
    def get_log_row_num(row):
        # This function must accept a row from universal_df
        row_num = row.get('excel_row_number', 'N/A')
        try:
            # Try to convert to int and add 1
            return str(int(row_num) + 1)
        except (ValueError, TypeError):
            # If it fails (e.g., 'N/A' or pd.NA), return 'N/A'
            return 'N/A'
    # --- END NEW HELPER FOR LOGGING ---

    # --- END HELPER ---

    universal_df = pd.DataFrame(index=df.index, columns=UNIVERSAL_HEADERS)
    universal_df["Bank_Name"] = bank_name
    universal_df["Bank_File_Name"] = filename
    if "original_excel_row" in df.columns:
        universal_df["excel_row_number"] = df["original_excel_row"]
    else:
        universal_df["excel_row_number"] = pd.NA

    # 1. Clean column names
    cleaned_df_columns = {}
    for col in df.columns:
        if pd.isna(col) or str(col).strip() == "":
            cleaned_col = "idbank_raw_credit_column"
        elif pd.notna(col):
            cleaned_col = re.sub(
                r"[\s\W_]+", "", str(col).lower().replace("\n", "")
            )
        cleaned_df_columns[col] = cleaned_col
    df.rename(columns=cleaned_df_columns, inplace=True)

    # 2. Re-enforce Column Uniqueness
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

    print(f"   [DEBUG] Cleaned and flattened columns: {df.columns.to_list()}")

    # 3. Define internal column lookup maps
    column_maps = {
        "transaction_date": [
            "ամսաթիվ",
            "գործարքիամսաթիվ",
            "transactiondate",
            "օր",
            "հաշվառմանամսաթիվ",
            "գործարքներայլգործառնություններամսաթիվ",  # From log
        ],
        "provision_date": [
            "ձևակերպմանհաշվարկիապահովմանամսաթիվ",
            "provisiondate",
        ],
        "description": [
            "նկարագրություն",
            "մեկնաբանություն",
            "նպատակ",
            "բացատրություն",
            "details",
            "գործարքնկարագրություն",
            "գործարքինկարագրություն",
            "գործարքինկարագրությունunnamed17level1",  # From log
            "գործարքնկարագիր",
        ],
        "transaction_place": ["գործարքիվայրը", "գործարքիվայրը1"],
        "currency_col": [
            "արժույթ",
            "currency",
            "քարտիարժույթով",
            "հաշվիարժույթով",
        ],
        "explicit_inflow": [
            "գործարքիգումարhաշվիարժույթով_մուտք",
            "գործարքիգումարըքարտիարժույթով_մուտք",
            "գործարքիգումարքարտիարժույթով_մուտք",
        ],
        "explicit_outflow": [
            "գործարքիգումարhաշվիարժույթով_ելք",
            "գործարքիգումարըքարտիարժույթով_ելք",
            "գործարքիգումարքարտիարժույթով_ելք",
        ],
        "credit": [
            "մուտքamd",
            "մուտք",
            "credit",
            "inflow",
            "կրեդիտ",
            "idbank_raw_credit_column",
        ],
        "debit": ["ելքamd", "ելք", "debit", "outflow", "դեբետ"],
        "single_amount_sign": [
            "գործարքիգումարքարտիարժույթով",
            "գործարքիգումարհաշվիարժույթով",
            "amount",
            "գործարքիգումարը",
        ],
        "sender": ["շահառուվճարող", "շահառու", "վճարող", "sendername", "թղթակից"],
        "sender_account": [
            "շահառույիվճարողիհաշիվ",
            "հաշիվ",
            "accountnumber",
        ],
    }

    # 4a. Date Mapping
    transaction_date_col = find_column(column_maps["transaction_date"])
    provision_date_col = find_column(column_maps["provision_date"])

    # --- START MODIFICATION (Fix Date Bug) ---
    DATE_FORMATS = [
        "%Y-%m-%d %H:%M:%S",
        "%d.%m.%Y",
        "%d.%m.%Y %H:%M:%S",
        "%m/%d/%Y",
        "%m/%d/%Y %H:%M:%S",
        "%Y.%m.%d",
        "%d/%m/%Y",
        "%d/%m/%Y %H:%M",
    ]

    def robust_date_parser(col):
        # Create a working copy
        s = col.astype(str).str.strip()

        # --- FIX for '0023' year (Using correct \g<1> syntax) ---
        s = s.str.replace(r"([/\.])00(\d{2})\b", r"\g<1>20\g<2>", regex=True)
        # --- END FIX ---

        parsed_dates = pd.NaT
        for fmt in DATE_FORMATS:
            # We parse as naive first
            parsed_dates = pd.to_datetime(s, format=fmt, errors="coerce")
            if not parsed_dates.isna().all():
                # Found format, return naive series
                return parsed_dates
        try:
            # Try numeric conversion for Excel float dates
            numeric_col = pd.to_numeric(s, errors="coerce")
            if not numeric_col.isna().any() and numeric_col.max() > 40000:
                valid_indices = ~numeric_col.isna()
                dates_as_floats = numeric_col[valid_indices]
                converted_dates = pd.to_datetime(
                    dates_as_floats, unit="D", origin="1899-12-30", errors="coerce"
                )
                # Create a result series
                result = pd.Series(pd.NaT, index=col.index, dtype="datetime64[ns]")
                result.loc[valid_indices] = converted_dates.values
                return result
        except:
            pass

        # Fallback for any other format
        return pd.to_datetime(
            s, errors="coerce", dayfirst=True
        )
    # --- END MODIFICATION ---

    if transaction_date_col:
        universal_df["Transaction_Date"] = robust_date_parser(
            df[transaction_date_col]
        )
        print(f"   [DEBUG] Found Date column: '{transaction_date_col}'")
    else:
        print("   ❌ [DEBUG] Date column NOT FOUND.")

    if provision_date_col:
        universal_df["Provision_Date"] = robust_date_parser(
            df[provision_date_col]
        )
    if transaction_date_col and not provision_date_col:
        universal_df["Provision_Date"] = universal_df["Transaction_Date"]
    elif provision_date_col and not transaction_date_col:
        universal_df["Transaction_Date"] = universal_df["Provision_Date"]

    # --- START MODIFICATION (Add Balance Filter) ---
    # 0. Find description columns for filtering BEFORE amount logic
    desc_cols_found = []
    for keyword in column_maps["description"]:
        desc_cols_found.extend(
            [col for col in df.columns if keyword in col]
        )
    desc_cols_found = sorted(
        list(set(desc_cols_found)), key=desc_cols_found.index
    )

    # Create a temporary combined description series for filtering
    if desc_cols_found:
        temp_desc = df[desc_cols_found].astype(str).fillna('').apply(
            lambda row: ' '.join(row.values).strip(), axis=1
        ).str.lower()

        # Filter out "daily balance" rows ("մնացորդ")
        balance_mask = temp_desc.str.contains("մնացորդ", na=False)
        rows_to_drop_balance = df[balance_mask]

        if not rows_to_drop_balance.empty:
            print(
                f"   [DEBUG] Dropping {len(rows_to_drop_balance)} rows "
                f"containing 'մնացորդ' (balance)."
            )
            # --- START MODIFIED DEBUG (FIX TypeError) ---
            for index, row in rows_to_drop_balance.head(5).iterrows():
                row_num_val = row.get('original_excel_row', 'N/A')
                row_num = str(row_num_val + 1) if isinstance(row_num_val, int) else 'N/A'
                print(
                    f"     - (Row {row_num}) "
                    f"Desc: {str(temp_desc.get(index, 'N/A'))[:70]}"
                )
            # --- END MODIFIED DEBUG ---

            # Drop these rows from the main dataframe 'df'
            df = df[~balance_mask].copy()
            # Also drop them from universal_df to keep indexes aligned
            universal_df = universal_df[~balance_mask].copy()

    # --- END MODIFICATION ---

    # 4b. Amount Determination
    inflow_series = pd.Series(0.0, index=df.index)
    outflow_series = pd.Series(0.0, index=df.index)

    explicit_inflow_col = find_column(column_maps["explicit_inflow"])
    explicit_outflow_col = find_column(column_maps["explicit_outflow"])

    if not explicit_inflow_col:
        explicit_inflow_col = find_column_by_substring(
            column_maps["explicit_inflow"]
        )
    if not explicit_outflow_col:
        explicit_outflow_col = find_column_by_substring(
            column_maps["explicit_outflow"]
        )

    credit_col = find_column_by_substring(column_maps["credit"])
    debit_col = find_column_by_substring(column_maps["debit"])
    single_amount_col = find_column_by_substring(
        column_maps["single_amount_sign"]
    )

    if explicit_inflow_col or explicit_outflow_col:
        print(
            f"   -> [DEBUG] Amount logic: Using EXPLICIT INFLOW/OUTFLOW "
            f"columns ('{explicit_inflow_col}', '{explicit_outflow_col}')."
        )
        inflow_series = clean_amount_series(
            df.get(explicit_inflow_col), bank_name, explicit_inflow_col
        )
        raw_outflow_series = clean_amount_series(
            df.get(explicit_outflow_col), bank_name, explicit_outflow_col
        )
        outflow_series = raw_outflow_series.abs()
    elif credit_col or debit_col:
        print(
            f"   -> [DEBUG] Amount logic: Using DEDICATED CREDIT/DEBIT "
            f"columns ('{credit_col}', '{debit_col}')."
        )
        inflow_series = clean_amount_series(
            df.get(credit_col), bank_name, credit_col
        )
        raw_outflow_series = clean_amount_series(
            df.get(debit_col), bank_name, debit_col
        )
        outflow_series = raw_outflow_series.abs()

    elif single_amount_col:
        print(
            f"   -> [DEBUG] Amount logic: Using SINGLE AMOUNT/SIGN column "
            f"('{single_amount_col}')."
        )
        amounts = clean_amount_series(
            df.get(single_amount_col), bank_name, single_amount_col
        )
        inflow_series = amounts.apply(lambda x: x if x > 0 else 0.0)
        outflow_series = amounts.apply(lambda x: abs(x) if x < 0 else 0.0)
    else:
        print("   ❌ Amount logic: Could not find any recognized amount column.")

    universal_df["is_expense"] = outflow_series > 0
    universal_df["Amount"] = inflow_series.mask(
        universal_df["is_expense"], outflow_series
    )

    print(
        f"   [DEBUG] 'is_expense' series (head 10): "
        f"\n{universal_df['is_expense'].head(10)}"
    )
    print(
        f"   [DEBUG] 'Amount' series (head 10): "
        f"\n{universal_df['Amount'].head(10)}"
    )

    # 5. Filtering
    initial_rows = len(universal_df)

    rows_to_drop_no_amount = universal_df[universal_df["Amount"] <= 0]
    if not rows_to_drop_no_amount.empty:
        print(
            f"   [DEBUG] Dropping {len(rows_to_drop_no_amount)} rows "
            f"because Amount is <= 0."
        )
        # --- START MODIFIED DEBUG (FIX TypeError) ---
        for index, row in rows_to_drop_no_amount.head(5).iterrows():
            print(
                f"     - (Row {get_log_row_num(row)}) "
                f"Inflow: {inflow_series.get(index, 'N/A')}, "
                f"Outflow: {outflow_series.get(index, 'N/A')}, "
                f"Desc: {str(row.get('Description', 'N/A'))[:50]}"
            )
        # --- END MODIFIED DEBUG ---

    universal_df = universal_df[universal_df["Amount"] > 0].copy()
    print(
        f"   -> Filtered: Kept {len(universal_df)} of {initial_rows} rows "
        f"(In/Out > 0)."
    )

    # 6. Currency Detection
    currency_col = find_column(column_maps["currency_col"])
    inferred_currency = "AMD"
    if currency_col:
        currency_val = (
            df[currency_col].dropna().iloc[0]
            if not df[currency_col].dropna().empty
            else inferred_currency
        )
        inferred_currency = currency_val.upper()
    elif credit_col and "amd" in credit_col:
        inferred_currency = "AMD"
    elif credit_col and "usd" in credit_col:
        inferred_currency = "USD"
    universal_df["Currency"] = inferred_currency

    # 7. Description, Sender, and Account Mapping
    # (Re-using desc_cols_found from step 0)
    if desc_cols_found:
        print(
            f"   -> Description logic: Combining {len(desc_cols_found)} "
            f"description column(s)."
        )
        description_data = df.loc[universal_df.index, desc_cols_found].astype(str).copy()
        description_data.replace("nan", "", inplace=True)
        universal_df["Description"] = (
            description_data.apply(
                lambda row: " ".join(row.values).strip(), axis=1
            )
            .str.replace("_x000D_", " ", regex=False)
            .str.replace(r"\s{2,}", " ", regex=True)
        )
    else:
        universal_df["Description"] = create_placeholder()
        print(
            "   ❌ Description logic: Could not find any recognized "
            "description column."
        )

    sender_col = find_column(column_maps["sender"])
    universal_df["Sender"] = (
        df[sender_col].astype(str)
        if sender_col in df.columns
        else create_placeholder()
    )
    if sender_col is None:
        print("   ❌ Sender logic: Could not find sender column.")
    else:
        print(f"   -> Sender logic: Using column '{sender_col}'.")

    acc_col = find_column(column_maps["sender_account"])
    universal_df["Sender account number"] = (
        df[acc_col].astype(str)
        if acc_col in df.columns
        else create_placeholder()
    )
    if acc_col is None:
        print("   ❌ Account logic: Could not find account column.")
    else:
        print(f"   -> Account logic: Using column '{acc_col}'.")

    place_cols_found = [
        col
        for keyword in column_maps["transaction_place"]
        for col in df.columns
        if keyword in col
    ]
    if place_cols_found:
        place_data = df.loc[universal_df.index, place_cols_found].astype(str).copy()
        place_data.replace("nan", "", inplace=True)
        universal_df["Transaction_Place"] = (
            place_data.apply(lambda row: " ".join(row.values).strip(), axis=1)
            .str.replace(r"\s{2,}", " ", regex=True)
        )
    else:
        universal_df["Transaction_Place"] = create_placeholder()

    # 8. Final cleanup and logging
    rows_before_final_drop = len(universal_df)
    final_df = universal_df.dropna(subset=["Transaction_Date", "Amount"]).copy()
    rows_dropped_final = rows_before_final_drop - len(final_df)

    if rows_dropped_final > 0:
        print(
            f"   ❌ Final Date/Amount check dropped {rows_dropped_final} "
            f"rows due to invalid data (e.g., failed date parse)."
        )
        invalid_rows = universal_df[
            universal_df["Transaction_Date"].isna()
            | universal_df["Amount"].isna()
        ]
        # --- START MODIFIED DEBUG (FIX TypeError) ---
        for index, row in invalid_rows.head(5).iterrows():
            raw_date_val = "N/A"
            if transaction_date_col and index in df.index:
                raw_date_val = df.loc[index].get(transaction_date_col, "N/A")

            print(
                f"     - (Row {get_log_row_num(row)}) "
                f"Raw Date: '{str(raw_date_val)}', "
                f"Amount: {row.get('Amount')}, "
                f"Desc: {str(row.get('Description', 'N/A'))[:50]}"
            )
        # --- END MODIFIED DEBUG ---

    # 9. Parse Date from Description
    print(f"   -> Parsing dates from description for {len(final_df)} rows...")
    final_df["date_from_description"] = final_df.apply(
        lambda row: _parse_date_from_description(
            row["Description"], row["Transaction_Date"]
        ),
        axis=1,
    )
    final_df["date_from_description"] = pd.to_datetime(
        final_df["date_from_description"], errors="coerce"
    )

    print(
        f"   ✅ Final normalized size: {len(final_df)} transactions "
        f"(in and out)."
    )

    return final_df
