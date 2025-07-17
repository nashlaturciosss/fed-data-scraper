import os
import re
import csv
from urllib.parse import unquote  # used to decode URL-encoded filenames

# === config ===
INPUT_FOLDER = "gemini_outputs_from_s3"
INSIDERS_OUTPUT = "simple_all_insiders_s3.csv"
SECURITIES_OUTPUT = "simple_all_securities_holders_s3.csv"

# === headers ===
INSIDERS_HEADER = [
    "Bank", "Internal Title", "Person", 
    "Fiscal Year", "Occupation", "Percentage of Voting Shares", "RSSD_ID",
    "Percentage of Voting Shares in Subsidiaries", "URL_Bank_Name", "Table Presence"
]

SECURITIES_HEADER = [
    "Bank", "Town", "Fiscal Year", "Owner Name", "Stock Class",
    "Number of Shares", "Percentage of Ownership", "RSSD_ID",
    "URL_Bank_Name", "Table Presence"
]

# === Storage ===
all_insiders = []
all_securities = []

# === Extract table blocks even if wrapped in ```csv or ``` ===
def extract_table_blocks(text, table_name):
    lines = text.splitlines()
    block_lines = []
    collecting = False

    for line in lines:
        if table_name.lower() in line.lower():
            collecting = True
            continue
        if collecting and ("INSIDERS CSV" in line or "SECURITIES HOLDERS CSV" in line):
            break
        if collecting and line.strip().startswith("```"):
            continue
        if collecting:
            block_lines.append(line)

    return "\n".join(block_lines).strip() if block_lines else ""

# === Check if block contains actual data rows (not just header) ===
def is_valid_data_block(block, expected_header_line):
    if not block:
        return False
    lines = [line for line in block.splitlines() if line.strip()]
    if len(lines) <= 1:
        return False
    header_cleaned = lines[0].strip().lower().replace('"', '').replace(' ', '')
    expected_cleaned = expected_header_line.strip().lower().replace(' ', '')
    return expected_cleaned in header_cleaned

def parse_csv_block(csv_block, expected_columns, url_bank_name, table_presence):
    rows = []

    if not csv_block.strip():
        return rows

    cleaned_lines = []
    for line in csv_block.splitlines():
        if not line.strip() or line.strip().startswith("```"):
            continue
        cleaned_lines.append(line)

    if len(cleaned_lines) <= 1:
        return rows

    reader = csv.reader(cleaned_lines)

    for row in reader:
        if not row:
            continue
        cell = row[0].strip().lower()
        if cell == "bank":
            continue

        if len(row) != expected_columns:
            print(f"Row length mismatch (expected {expected_columns}, got {len(row)}): {row}")

        while len(row) < expected_columns:
            row.append("n/a")
        if len(row) > expected_columns:
            row = row[:expected_columns]

        row.append(url_bank_name)
        row.append(table_presence)
        rows.append(row)
    return rows

# === main loop ===
file_counter = 0
for filename in sorted(os.listdir(INPUT_FOLDER)):
    if not filename.endswith(".txt"):
        continue

    file_counter += 1
    filepath = os.path.join(INPUT_FOLDER, filename)
    url_bank_name = unquote(filename).replace(".txt", "")

    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    print(f"\n Processing file: {filename}")

    securities_block = extract_table_blocks(content, "SECURITIES HOLDERS CSV")
    insiders_block = extract_table_blocks(content, "INSIDERS CSV")

    has_securities = is_valid_data_block(
        securities_block, "Bank,Town,Fiscal Year,Owner Name,Stock Class"
    )
    has_insiders = is_valid_data_block(
        insiders_block, "Bank,Internal Title,Person"
    )

    if has_securities:
        table_presence = "securities" if not has_insiders else "both"
        parsed_securities = parse_csv_block(
            securities_block, len(SECURITIES_HEADER) - 2, url_bank_name, table_presence
        )
        all_securities.extend(parsed_securities)
        print(f"    Added {len(parsed_securities)} securities holder rows.")
    else:
        print("No Securities table found.")

    if has_insiders:
        table_presence = "insiders" if not has_securities else "both"
        parsed_insiders = parse_csv_block(
            insiders_block, len(INSIDERS_HEADER) - 2, url_bank_name, table_presence
        )
        all_insiders.extend(parsed_insiders)
        print(f"   Added {len(parsed_insiders)} insider rows.")
    else:
        print(" No Insiders table found.")

    if not has_securities and not has_insiders:
        print(" No valid tables found, insert empty placeholders.")
        placeholder_sec = ["n/a"] * (len(SECURITIES_HEADER) - 2)
        placeholder_sec.append(url_bank_name)
        placeholder_sec.append("none")
        all_securities.append(placeholder_sec)

        placeholder_ins = ["n/a"] * (len(INSIDERS_HEADER) - 2)
        placeholder_ins.append(url_bank_name)
        placeholder_ins.append("none")
        all_insiders.append(placeholder_ins)

# === write combined output files ===
with open(SECURITIES_OUTPUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(SECURITIES_HEADER)
    writer.writerows(all_securities)

with open(INSIDERS_OUTPUT, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(INSIDERS_HEADER)
    writer.writerows(all_insiders)

print(f"\nMerged {len(all_securities)} securities holders and {len(all_insiders)} insiders from {file_counter} files.")
print(f" Output written to: {SECURITIES_OUTPUT}, {INSIDERS_OUTPUT}")
