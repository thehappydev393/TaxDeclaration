# tax_processor/parser_logic.py

import io
import os
import gc
import re
import shutil
import traceback
from datetime import datetime
from typing import Any, Dict, List, Union

import pandas as pd
from pypdf import PdfReader

# --- PDF Dependency Check ---
try:
    import camelot
    CAMELOT_AVAILABLE = True
except ImportError:
    CAMELOT_AVAILABLE = False
    print("Warning: Camelot not installed. PDF parsing will fail.")

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
    "InecoBank": ["InecoBank", "Ineco", "InectoBank", "Ինեկո", "ԻՆԵԿՈԲԱՆԿ", "INECOBANK"],
    "Unibank": ["Unibank", "Uni Bank"],
    "FastBank": ["fastbank", "fast bank", "ՖԱՍԹ ԲԱՆԿ"],
    "AEB": ["AEB", "ArmEconomBank", "ՀԱՅԷԿՈՆՈՄԲԱՆԿ"],
}

# --- INECO SPECIFIC CONSTANTS ---
INECO_TABLE_TITLE_MARKERS = [
    "ԿԱՏԱՐՎԱԾ ԳՈՐԾԱՐՔՆԵՐԻ/ԳՈՐԾԱՌՆՈՒԹՅՈՒՆՆԵՐԻ ՎԵՐԱԲԵՐՅԱԼ ՄԱՆՐԱՄԱՍՆ ՏԵՂԵԿԱՏՎՈՒԹՅՈՒՆ",
    "DETAILED INFORMATION ABOUT CONCLUDED TRANSACTIONS/OPERATIONS"
]
INECO_CORE_SUBHEADERS = [
    'Ամսաթիվ', 'Ելք', 'Մուտք', 'Արժույթ',
    'Date', 'Out', 'In', 'Currency'
]
INECO_EXPECTED_COL_COUNT = 9
# -------------------------------------------

HEADER_KEYWORDS_DATE = ["ամսաթիվ", "date", "օր"]
HEADER_KEYWORDS_AMOUNT = ["գումար", "amount", "դեբետ", "կրեդիտ", "մուտք", "ելք", "daily balance"]
HEADER_KEYWORDS_FLOW = ["income", "expense", "մուտք", "ելք", "credit", "debit", "in", "out", "inflow", "outflow"]
MAX_HEADER_SEARCH_ROWS = 50

UNIVERSAL_HEADERS = [
    "Bank_Name", "Bank_File_Name", "Transaction_Date", "Provision_Date",
    "date_from_description", "Amount", "Currency", "is_expense",
    "Description", "Transaction_Place", "Sender", "Sender account number",
    "excel_row_number",
]

# --- Utilities ---
DATE_REGEX_DMY = re.compile(r"(\d{1,2})[\./-](\d{1,2})[\./-](\d{2,4})")
CHAR_SET = r"[a-zа-яա-ֆ]+"
MONTH_YEAR_REGEX = re.compile(rf"({CHAR_SET})[\s,]+(\d{{4}})", re.IGNORECASE)
DAY_MONTH_REGEX = re.compile(rf"(\d{{1,2}})[\s,]+({CHAR_SET})", re.IGNORECASE)
MONTH_DAY_REGEX = re.compile(rf"({CHAR_SET})[\s,]+(\d{{1,2}})", re.IGNORECASE)

MONTH_MAP = {
    "հունվար": 1, "հնվ": 1, "փետրվար": 2, "փտր": 2, "մարտ": 3, "մրտ": 3,
    "ապրիլ": 4, "ապր": 4, "մայիս": 5, "մյս": 5, "հունիս": 6, "հնս": 6,
    "հուլիս": 7, "հլս": 7, "օգոստոս": 8, "օգս": 8, "սեպտեմբեր": 9, "սպտ": 9,
    "հոկտեմբեր": 10, "հկտ": 10, "նոյեմբեր": 11, "նմբ": 11, "դեկտեմբեր": 12, "դկտ": 12,
    "հունվարի": 1, "փետրվարի": 2, "մարտի": 3, "ապրիլի": 4, "մայիսի": 5, "հունիսի": 6,
    "հուլիսի": 7, "օգոստոսի": 8, "սեպտեմբերի": 9, "հոկտեմբերի": 10, "նոյեմբերի": 11, "դեկտեմբերի": 12,
    "январь": 1, "янв": 1, "февраль": 2, "фев": 2, "март": 3, "мар": 3,
    "апрель": 4, "апр": 4, "май": 5, "мая": 5, "июнь": 6, "июн": 6,
    "июль": 7, "июл": 7, "август": 8, "авг": 8, "сентябрь": 9, "сен": 9,
    "октябрь": 10, "окт": 10, "ноябрь": 11, "ноя": 11, "декабрь": 12, "дек": 12,
    "января": 1, "февраля": 2, "марта": 3, "апреля": 4, "июня": 6, "июля": 7,
    "августа": 8, "сентября": 9, "октября": 10, "ноября": 11, "декабря": 12,
    "january": 1, "jan": 1, "february": 2, "feb": 2, "march": 3, "mar": 3,
    "april": 4, "apr": 4, "may": 5, "june": 6, "jun": 6, "july": 7, "jul": 7,
    "august": 8, "aug": 8, "september": 9, "sep": 9, "october": 10, "oct": 10,
    "november": 11, "nov": 11, "december": 12, "dec": 12,
}

def _parse_date_from_description(description, transaction_date):
    if not description or pd.isna(description): return None
    desc_lower = description.lower()
    try:
        match = DATE_REGEX_DMY.search(desc_lower)
        if match:
            day, month, year = int(match.group(1)), int(match.group(2)), int(match.group(3))
            if year < 100: year += 2000
            if 1 <= month <= 12 and 1 <= day <= 31:
                return datetime(year, month, day).date()
        match = MONTH_YEAR_REGEX.search(desc_lower)
        if match:
            month_str, year_str = match.group(1), match.group(2)
            month = MONTH_MAP.get(month_str)
            year = int(year_str)
            if month: return datetime(year, month, 1).date()
        match = DAY_MONTH_REGEX.search(desc_lower)
        if match:
            day_str, month_str = match.group(1), match.group(2)
            month = MONTH_MAP.get(month_str)
            day = int(day_str)
            if month and 1 <= day <= 31:
                year = transaction_date.year
                if transaction_date.month == 1 and month == 12: year -= 1
                return datetime(year, month, day).date()
        match = MONTH_DAY_REGEX.search(desc_lower)
        if match:
            month_str, day_str = match.group(1), match.group(2)
            month = MONTH_MAP.get(month_str)
            day = int(day_str)
            if month and 1 <= day <= 31:
                year = transaction_date.year
                if transaction_date.month == 1 and month == 12: year -= 1
                return datetime(year, month, day).date()
    except Exception: return None
    return None

def identify_bank_from_text(text_content: str) -> str:
    text_content_lower = text_content.lower()
    for bank_name, keywords in BANK_KEYWORDS.items():
        for keyword in keywords:
            if keyword.lower() in text_content_lower:
                return bank_name
    return "Unknown Bank"

def extract_full_content_for_search(filepath: str, file_extension: str) -> Union[List[str], str]:
    try:
        if file_extension in (".xls", ".xlsx"):
            df = pd.read_excel(filepath, sheet_name=0, header=None, nrows=MAX_HEADER_SEARCH_ROWS, dtype=str)
            return [" ".join(row.dropna().astype(str).values) for _, row in df.iterrows()]
        elif file_extension == ".pdf":
            reader = PdfReader(filepath)
            if reader.is_encrypted: return ""
            return (reader.pages[0].extract_text() or "").strip()
    except Exception: return ""
    return ""

def find_header_start_index(content: Union[List[str], str], extension: str) -> (int, bool):
    if extension == '.pdf': return 0, False # PDF logic handled separately
    lines = content if isinstance(content, list) else content.split("\n")
    MULTI_HEADER_PARENTS = [
        "գործարքներ, այլ գործառնություններ", "գործարքի գումար հաշվի արժույթով",
        "գործարքի գումար քարտի արժույթով", "գործարքի գումար",
        "transactions, other operations", "transaction amount in the account currency",
    ]
    for i, line in enumerate(lines):
        normalized_line = " ".join(line.lower().split())
        if any(parent in normalized_line for parent in MULTI_HEADER_PARENTS):
            if i + 1 < len(lines):
                next_line = " ".join(lines[i + 1].lower().split())
                if "մուտք" in next_line or "ելք" in next_line or "in" in next_line or "out" in next_line:
                    return i, True
        has_date = any(kw in normalized_line for kw in HEADER_KEYWORDS_DATE)
        has_flow = any(kw in normalized_line for kw in HEADER_KEYWORDS_FLOW)
        if has_date and has_flow: return i, False
    return -1, False

def flatten_headers(multiindex_cols):
    new_cols = []
    seen_cols = {}
    for col in multiindex_cols:
        cleaned_parts = [re.sub(r"[\s\W_]+", "", str(c).lower().replace("\n", "")) for c in col if pd.notna(c) and str(c).strip()]
        final_col = ""
        if len(cleaned_parts) >= 2:
            child = cleaned_parts[-1]
            parent = cleaned_parts[0]
            final_col = f"{parent}_{child}"
            if not child: final_col = parent
        elif len(cleaned_parts) == 1:
            final_col = cleaned_parts[0]
        else:
            final_col = f"unnamed_{len(new_cols)}"
        original_col = final_col
        count = seen_cols.get(original_col, 0)
        if count > 0: final_col = f"{original_col}_{count}"
        seen_cols[original_col] = count + 1
        new_cols.append(final_col)
    return new_cols

def validate_statement_owner(content, fn, ln): return True

# ==============================================================================
#  FALLBACK HELPERS (Ameriabank)
# ==============================================================================

def _repair_ameriabank_rows(df: pd.DataFrame) -> pd.DataFrame:
    """Fallback: Merges split rows. Used only in Ameriabank fallback path."""
    if df.empty: return df
    date_col_idx = 0
    # Identify date column by content
    for c_idx in range(min(3, df.shape[1])):
        matches = 0
        for r_idx in range(min(10, len(df))):
            val = str(df.iloc[r_idx, c_idx]).strip()
            if re.match(r'^\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4}', val): matches += 1
        if matches >= 1: date_col_idx = c_idx; break

    new_rows = []; current_row = None
    for index, row in df.iterrows():
        val = str(row.iloc[date_col_idx]).strip()
        if re.match(r'^\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4}', val):
            if current_row is not None: new_rows.append(current_row)
            current_row = row.copy()
        else:
            if current_row is not None:
                for col_idx in range(len(row)):
                    cell_val = str(row.iloc[col_idx]).strip()
                    if cell_val and cell_val.lower() not in ['nan', 'none', '']:
                        prev_val = str(current_row.iloc[col_idx]).strip()
                        if prev_val and prev_val.lower() not in ['nan', 'none', '']:
                            current_row.iloc[col_idx] = prev_val + " " + cell_val
                        else:
                            current_row.iloc[col_idx] = cell_val
    if current_row is not None: new_rows.append(current_row)
    return pd.DataFrame(new_rows).reset_index(drop=True)

def _parse_pdf_ameriabank_fallback(content_source) -> pd.DataFrame:
    """Fallback PDF parser for Ameriabank using forced Stream mode."""
    try:
        reader = PdfReader(content_source)
        total_pages = len(reader.pages)
        all_tables = []
        for page_num in range(1, total_pages + 1):
            # Force stream for all pages in fallback
            if CAMELOT_AVAILABLE:
                tables = camelot.read_pdf(content_source, pages=str(page_num), flavor="stream")
                all_tables.extend(tables)

        dfs = [t.df for t in all_tables if not t.df.empty]
        if not dfs: return pd.DataFrame()
        full_df = pd.concat(dfs, ignore_index=True)

        # Cleanup empty columns
        full_df = full_df.dropna(axis=1, how='all')
        full_df.columns = range(full_df.shape[1])

        # Clean newlines and repair rows
        full_df = full_df.replace(r'\n', ' ', regex=True)
        full_df = _repair_ameriabank_rows(full_df)
        full_df["original_excel_row"] = full_df.index + 1
        return full_df
    except: return pd.DataFrame()

def _normalize_ameriabank_fallback(df: pd.DataFrame, filename: str) -> pd.DataFrame:
    """Fallback normalizer for Ameriabank when headers are missing."""
    universal_df = pd.DataFrame(index=df.index, columns=UNIVERSAL_HEADERS)
    universal_df["Bank_Name"] = "Ameriabank"
    universal_df["Bank_File_Name"] = filename
    if "original_excel_row" in df.columns:
        universal_df["excel_row_number"] = df["original_excel_row"]

    # 1. Find Date (Content scan)
    date_col = None
    for c in df.columns:
        samp = df[c].head(10).astype(str).to_string()
        if re.search(r'\d{1,2}[/\.-]\d{1,2}[/\.-]\d{2,4}', samp):
            date_col = c; break

    if date_col is not None:
        def parse_d(s):
            s = str(s).strip().replace("/", ".").replace("-", ".")
            s = re.sub(r"([/\.])00(\d{2})\b", r"\g<1>20\g<2>", s)
            try: return pd.to_datetime(s, dayfirst=True, errors='coerce')
            except: return pd.NaT
        universal_df["Transaction_Date"] = df[date_col].apply(parse_d)
        universal_df["Provision_Date"] = universal_df["Transaction_Date"]

    # 2. Find Amount (Scan for AMD/USD)
    amount_col = None
    for c in df.columns:
        samp = df[c].head(5).astype(str).to_string()
        if "AMD" in samp or "USD" in samp:
            amount_col = c; break

    if amount_col is not None:
        def clean(s):
            s = str(s).replace("AMD","").replace("USD","").replace(",","")
            try: return float(s)
            except: return 0
        raw = df[amount_col].apply(clean)
        universal_df["is_expense"] = raw < 0
        universal_df["Amount"] = raw.abs()

        samp = df[amount_col].astype(str).head(10).to_string()
        if "USD" in samp: universal_df["Currency"] = "USD"
        elif "EUR" in samp: universal_df["Currency"] = "EUR"
        else: universal_df["Currency"] = "AMD"

    # 3. Description (Longest text col)
    desc_col = None
    best_len = 0
    for c in df.columns:
        if c in [date_col, amount_col, "original_excel_row"]: continue
        avg_len = df[c].astype(str).str.len().mean()
        if avg_len > best_len:
            best_len = avg_len
            desc_col = c

    if desc_col is not None:
        universal_df["Description"] = df[desc_col].astype(str).str.strip().str.replace("_x000D_", " ")
    else:
        universal_df["Description"] = "N/A"

    universal_df["Sender"] = "N/A"

    universal_df = universal_df.dropna(subset=["Transaction_Date", "Amount"])
    universal_df = universal_df[universal_df["Amount"] > 0].copy()

    # Parse Date from Desc
    universal_df["date_from_description"] = universal_df.apply(
        lambda row: _parse_date_from_description(row["Description"], row["Transaction_Date"]),
        axis=1
    )
    universal_df["date_from_description"] = pd.to_datetime(universal_df["date_from_description"], errors="coerce")

    print(f"   [Fallback] Normalized {len(universal_df)} rows using Fallback Logic.")
    return universal_df

# ------------------------------------------------------------------------
# Main Parsing Function
# ------------------------------------------------------------------------
def parse_transactions(content_source, extension, bank_name, header_index, is_multi_row, filename) -> pd.DataFrame:
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
                if "քաղվածք" in [name.lower() for name in sheet_names]:
                    sheet_name = [name for name in sheet_names if name.lower() == "քաղվածք"][0]
            except Exception: pass

            excel_content.seek(0)
            h_index = header_index if header_index is not None and header_index >= 0 else 0

            if is_multi_row:
                df = pd.read_excel(excel_content, sheet_name=sheet_name, header=[h_index, h_index + 1], dtype=str)
                df.columns = flatten_headers(df.columns)
                data_row_offset = h_index + 2
                df["original_excel_row"] = df.index + data_row_offset
                print(f"   -> Mode: Multi-Row Headers (Index {h_index} and {h_index + 1})")
            else:
                df = pd.read_excel(excel_content, sheet_name=sheet_name, header=h_index, dtype=str)
                data_row_offset = h_index + 1
                df["original_excel_row"] = df.index + data_row_offset
                print(f"   -> Mode: Single Header Row (Index {h_index})")

            return df
        except Exception as e:
            print(f"   [Error] Excel parse failed: {e}")
            return pd.DataFrame()

    elif extension == ".pdf":
        try:
            # --- INECO BANK RESTORED & ENHANCED LOGIC ---
            if bank_name == 'InecoBank':
                print(f"   -> Mode: PDF Parsing for InecoBank using Camelot 'lattice' flavor (2-row header).")
                try:
                    # --- PERFORMANCE OPTIMIZATION: REMOVED process_background=True ---
                    tables_camelot = camelot.read_pdf(content_source, pages='all', flavor='lattice', strip_text='\n', line_scale=40)
                except Exception as e_lattice:
                    print(f"   [Error] PDF (Ineco): Camelot lattice failed: {e_lattice}. Cannot parse.")
                    traceback.print_exc()
                    return pd.DataFrame()

                if not tables_camelot: print("   [Warning] PDF (Ineco): Camelot found no tables."); return pd.DataFrame()
                all_dfs = [table.df for table in tables_camelot if not table.df.empty]
                if not all_dfs: print("   [Warning] PDF (Ineco): All tables extracted were empty."); return pd.DataFrame()

                # --- Ineco Header Detection (Multilingual) ---
                detected_header_df_index = -1; header_row1_index = -1; header_row2_index = -1
                header_row1_list = []; header_row2_list = []; final_combined_header = []

                # Pre-clean all possible markers (remove spaces/slashes for loose matching)
                title_markers_clean = [re.sub(r'[\s/]+', '', m) for m in INECO_TABLE_TITLE_MARKERS]
                header_found_flag = False

                for df_idx, df_table in enumerate(all_dfs):
                    if header_found_flag: break
                    if df_table.empty: continue

                    for row_idx, row in df_table.head(15).iterrows():
                        row_text_combined = ''.join(row.astype(str).str.strip().tolist()); row_text_combined_clean = re.sub(r'[\s/]+', '', row_text_combined)

                        # Check against ALL title markers
                        if any(marker in row_text_combined_clean for marker in title_markers_clean):
                            print(f"   -> PDF (Ineco): Found title marker in Table {df_idx}, Row {row_idx}.")

                            # Search next 5 rows for subheaders (flexible spacing)
                            found_subheader_row = False
                            for offset in range(1, 6): # Check next 5 rows
                                check_idx = row_idx + offset
                                if check_idx >= len(df_table): break

                                row_to_check = df_table.iloc[check_idx]
                                row_str_lower = ' '.join(row_to_check.astype(str).str.strip().str.lower().tolist())

                                # --- NEW LOGGING ---
                                found_keywords = [kw for kw in INECO_CORE_SUBHEADERS if kw.lower() in row_str_lower]
                                print(f"      [Debug] Checking Offset {offset} (Row {check_idx}). Content: {row_str_lower[:50]}...")
                                print(f"      [Debug] Found Keywords: {found_keywords}")
                                # -------------------

                                # Relaxed check: Look for at least 3 of the expected keywords (Armenian OR English)
                                matches = len(found_keywords)
                                if matches >= 3:
                                    header_row2_index = check_idx
                                    header_row1_index = check_idx - 1 # Assume header 1 is directly above
                                    found_subheader_row = True
                                    print(f"   -> PDF (Ineco): Found subheaders at Row {check_idx}. Content: {row_str_lower[:50]}...")
                                    break

                            if found_subheader_row:
                                header_row1 = df_table.iloc[header_row1_index]
                                header_row2 = df_table.iloc[header_row2_index]

                                header_row1_list = header_row1.astype(str).str.strip().tolist()
                                header_row2_list = header_row2.astype(str).str.strip().tolist()
                                final_combined_header = []
                                max_len = max(len(header_row1_list), len(header_row2_list))
                                h1_padded = header_row1_list + [''] * (max_len - len(header_row1_list))
                                h2_padded = header_row2_list + [''] * (max_len - len(header_row2_list))

                                for h1, h2 in zip(h1_padded, h2_padded):
                                    h1 = h1.replace('\n', ' ').strip(); h2 = h2.replace('\n', ' ').strip()
                                    if h1 and h2: final_combined_header.append(f"{h1} {h2}")
                                    elif h2: final_combined_header.append(h2)
                                    elif h1: final_combined_header.append(h1)
                                    else: final_combined_header.append('')

                                final_combined_header = [f'Unknown_{i}' if not h else h for i, h in enumerate(final_combined_header)]

                                if len(final_combined_header) < INECO_EXPECTED_COL_COUNT: final_combined_header.extend([f'Unknown_{i}' for i in range(len(final_combined_header), INECO_EXPECTED_COL_COUNT)])
                                elif len(final_combined_header) > INECO_EXPECTED_COL_COUNT: final_combined_header = final_combined_header[:INECO_EXPECTED_COL_COUNT]

                                detected_header_df_index = df_idx
                                print(f"   -> PDF Header (Ineco 2-Row) identified at Table {df_idx}, Rows {header_row1_index}&{header_row2_index}: {final_combined_header}")
                                header_found_flag = True
                                break
                            else: print(f"   [Warning] PDF (Ineco): Could not find subheaders in 5 rows after title marker.")
                    if header_found_flag: break

                if not final_combined_header:
                    print("   [Error] PDF (Ineco): Could not find valid 2-row header after title marker. Cannot process.")
                    return pd.DataFrame()

                # Combine Ineco tables
                processed_dfs = []; num_header_cols = len(final_combined_header)
                data_start_row_index = header_row2_index + 1

                for df_idx, df_table in enumerate(all_dfs):
                    if df_table.empty: continue
                    if df_table.shape[1] == num_header_cols: df_current = df_table.copy()
                    elif df_table.shape[1] > num_header_cols: df_current = df_table.iloc[:, :num_header_cols].copy()
                    else:
                        df_current = df_table.copy()
                        [df_current.insert(loc=df_table.shape[1]+i, column=f'Padded_{i}', value=pd.NA) for i in range(num_header_cols - df_table.shape[1])]

                    df_current.columns = final_combined_header; start_row = 0

                    if df_idx == detected_header_df_index: start_row = data_start_row_index
                    else:
                        if not df_current.empty:
                            first_row_text = ''.join(df_current.iloc[0].astype(str).str.strip().tolist()); first_row_cleaned = re.sub(r'[\s/]+', '', first_row_text)
                            if any(marker in first_row_cleaned for marker in title_markers_clean): start_row = 1
                            if len(df_current) > start_row and df_current.iloc[start_row].astype(str).str.strip().tolist() == header_row1_list: start_row += 1
                            if len(df_current) > start_row and df_current.iloc[start_row].astype(str).str.strip().tolist() == header_row2_list: start_row += 1
                            elif len(df_current) > 0 and df_current.iloc[0].astype(str).str.strip().tolist() == final_combined_header: start_row = 1
                    df_current = df_current.iloc[start_row:].reset_index(drop=True); processed_dfs.append(df_current)

                if not processed_dfs: print("   [Warning] PDF (Ineco): No data after header processing."); return pd.DataFrame()
                final_df = pd.concat(processed_dfs, ignore_index=True)
                return final_df
            # --- END INECO LOGIC ---

            # --- ORIGINAL PDF LOGIC (Generic + Hybrid + Ameriabank Fallback) ---
            reader = PdfReader(content_source)
            total_pages = len(reader.pages)
            all_extracted_tables = []
            for page_num in range(1, total_pages + 1):
                page_str = str(page_num)
                if page_num == 1:
                    tables = camelot.read_pdf(content_source, pages=page_str, flavor="lattice")
                else:
                    tables = camelot.read_pdf(content_source, pages=page_str, flavor="stream")
                all_extracted_tables.extend(tables)

            if not all_extracted_tables:
                # Fallback Trigger 1: No tables
                if bank_name == "Ameriabank":
                    print("   [Info] Standard PDF returned nothing. Trying Fallback.")
                    return _parse_pdf_ameriabank_fallback(content_source)
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
                # Fallback Trigger 2: No headers
                if bank_name == "Ameriabank":
                    print("   [Info] Standard PDF missed headers. Trying Fallback.")
                    return _parse_pdf_ameriabank_fallback(content_source)
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
            final_df = (pd.concat(processed_dfs, ignore_index=True) if processed_dfs else pd.DataFrame())

            # Fallback Trigger 3: Garbage Headers (First column is a date)
            if not final_df.empty:
                col0 = str(final_df.columns[0])
                if bank_name == "Ameriabank" and re.search(r'\d{2}[/\.]\d{2}', col0):
                    print("   [Info] Standard PDF result looks bad (Date in header). Trying Fallback.")
                    return _parse_pdf_ameriabank_fallback(content_source)

            return final_df
        except Exception as e:
            print(f"   [Error] PDF parse failed: {e}")
            if bank_name == "Ameriabank": return _parse_pdf_ameriabank_fallback(content_source)
            return pd.DataFrame()

    return pd.DataFrame()


# ------------------------------------------------------------------------
# Normalization Logic (Standard + Fallback + Ineco Fixes + Positional)
# ------------------------------------------------------------------------
def normalize_transactions(df: pd.DataFrame, bank_name: str, filename: str) -> pd.DataFrame:
    # --- CHECK FOR FALLBACK DATA ---
    if df.empty: return pd.DataFrame(columns=UNIVERSAL_HEADERS)

    # If columns are generic integers (from fallback parser), go straight to fallback normalizer
    if str(df.columns[0]) == "0" and bank_name == "Ameriabank":
        return _normalize_ameriabank_fallback(df, filename)

    # --- USER'S ORIGINAL NORMALIZATION LOGIC ---
    cleaned_df_columns = {}
    for col in df.columns:
        if pd.isna(col) or str(col).strip() == "":
            cleaned_col = "idbank_raw_credit_column"
        elif pd.notna(col):
            # Ineco 2-row header cleaning
            cleaned_col = str(col).replace('\n', ' ').strip()
            if ' ' in cleaned_col:
                 last_part = cleaned_col.split(' ')[-1]
                 # Look for both Armenian and English keywords at the end of the header
                 if last_part.lower() in ['ամսաթիվ', 'մուտք', 'ելք', 'արժույթ', 'date', 'credit', 'debit', 'currency', 'in', 'out']:
                      cleaned_col = last_part
            cleaned_col = re.sub(r"[\s\W_]+", "", cleaned_col.lower())
        cleaned_df_columns[col] = cleaned_col
    df.rename(columns=cleaned_df_columns, inplace=True)

    new_cols = []; seen = {}
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

    column_maps = {
        "transaction_date": ["ամսաթիվ", "գործարքիամսաթիվ", "transactiondate", "օր", "հաշվառմանամսաթիվ", "գործարքներայլգործառնություններգործարքիամսաթիվ", "գործարքներայլգործառնություններամսաթիվ", "transactionsotheroperationsdate", "date"],
        "provision_date": ["ձևակերպմանհաշվարկիապահովմանամսաթիվ", "provisiondate"],
        "description": ["նկարագրություն", "մեկնաբանություն", "նպատակ", "բացատրություն", "details", "գործարքնկարագրություն", "գործարքինկարագրություն", "գործարքինկարագրությունunnamed17level1", "գործարքնկարագիր", "transactiondescription"],
        "transaction_place": ["գործարքիվայրը", "գործարքիվայրը1"],
        "currency_col": ["արժույթ", "currency", "քարտիարժույթով", "հաշվիարժույթով", "գործարքներայլգործառնություններարժույթ", "transactionsotheroperationscurrency"],
        "explicit_inflow": ["գործարքիգումարhաշվիարժույթով_մուտք", "գործարքիգումարըքարտիարժույթով_մուտք", "գործարքիգումարքարտիարժույթով_մուտք", "transactionamountintheaccountcurrency_in", "transactionamountintheaccountcurrencyin", "գործարքիգումարըքարտիարժույթովմուտք"],
        "explicit_outflow": ["գործարքիգումարhաշվիարժույթով_ելք", "գործարքիգումարըքարտիարժույթով_ելք", "գործարքիգումարքարտիարժույթով_ելք", "transactionamountintheaccountcurrency_out", "transactionamountintheaccountcurrencyout", "գործարքիգումարըքարտիարժույթովելք"],
        "credit": ["մուտքamd", "մուտք", "credit", "inflow", "կրեդիտ", "idbank_raw_credit_column", "income", "in"],
        "debit": ["ելքamd", "ելք", "debit", "outflow", "դեբետ", "expense", "out"],
        "single_amount_sign": ["գործարքիգումարքարտիարժույթով", "գործարքիգումարհաշվիարժույթով", "amount", "գործարքիգումարը"],
        "sender": ["շահառուվճարող", "շահառու", "վճարող", "sendername", "թղթակից", "receiverpayer"],
        "sender_account": ["շահառույիվճարողիհաշիվ", "հաշիվ", "accountnumber", "receiverpayeraccount"],
    }

    def find_column(keys):
        for k in keys:
            if k in df.columns: return k
        return None
    def find_column_by_substring(keys):
        for k in keys:
            for c in df.columns:
                if k in c: return c
        return None
    def create_placeholder(value="N/A", index=df.index): return pd.Series([value] * len(index), index=index).astype(str)
    def clean_amount_series(s, b, c=""):
        if s is None: return pd.Series(0, index=df.index)
        if isinstance(s, pd.DataFrame): s = pd.Series(s.iloc[:, 0].values, index=df.index).astype(str)
        else: s = s.astype(str)
        if b == "Evocabank": s = s.str.replace(".", "", regex=False).str.replace(",", ".", regex=False).str.replace(r"[^\d\.\-]", "", regex=True)
        else: s = s.str.replace(r"[()\,\s\xa0]", "", regex=True).str.replace(r"[^\d\.\-]", "", regex=True)
        return pd.to_numeric(s, errors="coerce").fillna(0)

    # --- START NEW HELPER FOR LOGGING ---
    def get_log_row_num(row):
        row_num = row.get('excel_row_number', 'N/A')
        try: return str(int(row_num) + 1)
        except: return 'N/A'
    # --- END NEW HELPER ---

    universal_df = pd.DataFrame(index=df.index, columns=UNIVERSAL_HEADERS)
    universal_df["Bank_Name"] = bank_name
    universal_df["Bank_File_Name"] = filename
    if "original_excel_row" in df.columns: universal_df["excel_row_number"] = df["original_excel_row"]
    else: universal_df["excel_row_number"] = pd.NA

    # --- POSITION-BASED FALLBACK FOR INECO (New Logic) ---
    t_date = find_column(column_maps["transaction_date"])
    cred = find_column_by_substring(column_maps["credit"])

    # If we are in InecoBank and couldn't find Date or Credit columns by name,
    # assume the standard 8-column layout (Date, ..., Debit, Credit, Currency, ..., Description)
    use_positional = False
    if bank_name == 'InecoBank' and (not t_date or not cred):
        print("   [Info] InecoBank: Named columns not found. Attempting positional fallback.")
        if len(df.columns) >= 8:
            use_positional = True
            # Mapping based on standard Ineco PDF layout:
            # Col 0: Date
            # Col 3: Currency
            # Col 4: Credit (In)
            # Col 5: Debit (Out)
            # Col 7: Description (often split, but let's take 7)
            col_date_idx = df.columns[0]
            col_curr_idx = df.columns[3]
            col_credit_idx = df.columns[4]
            col_debit_idx = df.columns[5]
            col_desc_idx = df.columns[7]
        else:
            print(f"   [Warning] InecoBank: Positional fallback failed. Not enough columns ({len(df.columns)} < 8).")

    DATE_FORMATS = ["%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d.%m.%Y %H:%M:%S", "%m/%d/%Y", "%m/%d/%Y %H:%M:%S", "%Y.%m.%d", "%d/%m/%Y", "%d/%m/%Y %H:%M"]
    def robust_date_parser(col):
        if col is None or col.empty: return pd.Series([pd.NaT] * len(df.index), index=df.index)
        s = col.astype(str).str.strip()
        s = s.str.replace(r"([/\.])00(\d{2})\b", r"\g<1>20\g<2>", regex=True)
        parsed = pd.Series(pd.NaT, index=s.index, dtype="datetime64[ns]")
        for fmt in DATE_FORMATS:
            unmatched = parsed.isna()
            if not unmatched.any(): break
            curr = pd.to_datetime(s[unmatched], format=fmt, errors="coerce")
            parsed.loc[unmatched] = parsed.loc[unmatched].fillna(curr)
        if parsed.isna().any():
            try:
                num = pd.to_numeric(s[parsed.isna()], errors="coerce")
                if not num.isna().all() and num.min() > 30000 and num.max() < 60000:
                    idx = num.dropna().index
                    conv = pd.to_datetime(num[idx], unit="D", origin="1899-12-30", errors="coerce")
                    parsed.loc[idx] = conv.values
            except: pass
        if parsed.isna().any():
             final = pd.to_datetime(s[parsed.isna()], errors="coerce", dayfirst=True)
             parsed.loc[parsed.isna()] = parsed.loc[parsed.isna()].fillna(final)
        return parsed

    # --- APPLY DATE ---
    if use_positional:
        universal_df["Transaction_Date"] = robust_date_parser(df[col_date_idx])
    elif t_date:
        universal_df["Transaction_Date"] = robust_date_parser(df[t_date])

    # Try to find provision date
    p_date = find_column(column_maps["provision_date"])
    if p_date:
        universal_df["Provision_Date"] = robust_date_parser(df[p_date])

    # Fill Provision_Date if it is completely empty
    if universal_df["Provision_Date"].isna().all() and not universal_df["Transaction_Date"].isna().all():
        universal_df["Provision_Date"] = universal_df["Transaction_Date"]

    # Balance filter
    desc_cols = []
    for k in column_maps["description"]: desc_cols.extend([c for c in df.columns if k in c])
    desc_cols = sorted(list(set(desc_cols)), key=lambda x: df.columns.tolist().index(x))
    if desc_cols:
        temp_desc = df[desc_cols].astype(str).fillna('').apply(lambda r: ' '.join(r.values).strip(), axis=1).str.lower()
        mask = temp_desc.str.contains("մնացորդ", na=False)
        df = df[~mask].copy()
        universal_df = universal_df[~mask].copy()

    # --- APPLY AMOUNT ---
    in_s = pd.Series(0.0, index=df.index)
    out_s = pd.Series(0.0, index=df.index)

    if use_positional:
        in_s = clean_amount_series(df[col_credit_idx], bank_name)
        out_s = clean_amount_series(df[col_debit_idx], bank_name).abs()
    else:
        exp_in = find_column(column_maps["explicit_inflow"]) or find_column_by_substring(column_maps["explicit_inflow"])
        exp_out = find_column(column_maps["explicit_outflow"]) or find_column_by_substring(column_maps["explicit_outflow"])
        cred = find_column_by_substring(column_maps["credit"])
        debt = find_column_by_substring(column_maps["debit"])
        sing = find_column_by_substring(column_maps["single_amount_sign"])

        if exp_in or exp_out:
            in_s = clean_amount_series(df.get(exp_in), bank_name, exp_in)
            raw_out = clean_amount_series(df.get(exp_out), bank_name, exp_out)
            out_s = raw_out.abs()
        elif cred or debt:
            in_s = clean_amount_series(df.get(cred), bank_name, cred)
            raw_out = clean_amount_series(df.get(debt), bank_name, debt)
            out_s = raw_out.abs()
        elif sing:
            amts = clean_amount_series(df.get(sing), bank_name, sing)
            in_s = amts.apply(lambda x: x if x > 0 else 0.0)
            out_s = amts.apply(lambda x: abs(x) if x < 0 else 0.0)

    universal_df["is_expense"] = out_s > 0
    universal_df["Amount"] = in_s.mask(universal_df["is_expense"], out_s)

    # Filtering
    universal_df = universal_df[universal_df["Amount"] > 0].copy()
    filtered_index = universal_df.index

    # --- APPLY CURRENCY ---
    curr = "AMD"
    if use_positional:
        v = df.loc[filtered_index, col_curr_idx].astype(str).str.strip().str.upper()
        valid = v.str.match(r'^[A-Z]{3}$')
        universal_df["Currency"] = v.where(valid, "AMD")
    else:
        cur_col = find_column(column_maps["currency_col"])
        if cur_col:
            v = df.loc[filtered_index, cur_col].astype(str).str.strip().str.upper()
            valid = v.str.match(r'^[A-Z]{3}$')
            universal_df["Currency"] = v.where(valid, "AMD")
        elif cred and "amd" in cred: universal_df["Currency"] = "AMD"
        elif cred and "usd" in cred: universal_df["Currency"] = "USD"
        else: universal_df["Currency"] = "AMD"

    # --- APPLY DESC/SENDER ---
    if use_positional:
        universal_df['Description'] = df.loc[filtered_index, col_desc_idx].astype(str).str.strip().str.replace('_x000D_', ' ').str.replace(r'\s{2,}', ' ', regex=True)
    elif desc_cols:
        description_data = df.loc[filtered_index, desc_cols].astype(str).copy()
        description_data.replace('nan', '', inplace=True)
        universal_df['Description'] = description_data.apply(lambda row: ' '.join(row.values).strip(), axis=1).str.replace('_x000D_', ' ', regex=False).str.replace(r'\s{2,}', ' ', regex=True)
    else: universal_df["Description"] = create_placeholder(index=filtered_index)

    snd = find_column(column_maps["sender"])
    universal_df["Sender"] = df.loc[filtered_index, snd].astype(str).fillna('N/A') if snd else create_placeholder(index=filtered_index)
    acc = find_column(column_maps["sender_account"])
    universal_df["Sender account number"] = df.loc[filtered_index, acc].astype(str).fillna('N/A') if acc else create_placeholder(index=filtered_index)

    # --- INECO SPECIFIC SENDER PARSING ---
    if bank_name == 'InecoBank' and 'Description' in universal_df.columns and not universal_df['Description'].empty:
         sender_pattern = re.compile(r'\{([^{}\(\)]+?)\s*\((\d[\d\s\-]*\d|\d+)\)\}')
         parsed_data = universal_df['Description'].str.extract(sender_pattern).reindex(filtered_index)
         if not parsed_data.empty:
             if 0 in parsed_data.columns:
                 mask_sender = (universal_df['Sender'] == 'N/A') & parsed_data[0].notna()
                 universal_df.loc[mask_sender, 'Sender'] = parsed_data.loc[mask_sender, 0]
             if 1 in parsed_data.columns:
                 mask_account = (universal_df['Sender account number'] == 'N/A') & parsed_data[1].notna()
                 universal_df.loc[mask_account, 'Sender account number'] = parsed_data.loc[mask_account, 1]
    # ------------------------------------------------

    plc = [c for k in column_maps["transaction_place"] for c in df.columns if k in c]
    if plc:
        universal_df["Transaction_Place"] = df.loc[filtered_index, plc].astype(str).fillna("").apply(lambda r: " ".join(r.values).strip(), axis=1).str.replace(r"\s{2,}", " ", regex=True)
    else: universal_df["Transaction_Place"] = create_placeholder(index=filtered_index)

    final_df = universal_df.dropna(subset=["Transaction_Date", "Amount"]).copy()

    # Fallback Trigger (Normalization)
    if final_df.empty and not df.empty and bank_name == "Ameriabank":
        print("   [Info] Standard Normalization returned 0 rows. Trying Fallback.")
        return _normalize_ameriabank_fallback(df, filename)

    final_df["date_from_description"] = final_df.apply(
        lambda row: _parse_date_from_description(row["Description"], row["Transaction_Date"]),
        axis=1,
    )
    final_df["date_from_description"] = pd.to_datetime(final_df["date_from_description"], errors="coerce")

    return final_df
