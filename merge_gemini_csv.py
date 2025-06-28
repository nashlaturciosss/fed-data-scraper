import os
import re
import csv

# === config ===
INPUT_FOLDER = "gemini_outputs"
INSIDERS_OUTPUT = "all_insiders_Atlanta.csv"
SECURITIES_OUTPUT = "all_securities_holders_Atlanta.csv"

# === headers ===
INSIDERS_HEADER = ["Bank", "Internal Title", "Person", "External Title", "Affiliation", "Fiscal Year","Occupation", "RSSD_ID" ]
SECURITIES_HEADER = ["Bank", "Town", "Fiscal Year", "Owner Name", "Stock Class", "Number of Shares", "Percentage of Ownership", "RSSD_ID"]

# === Storage ===
all_insiders = []
all_securities = []

def extract_table_blocks(text, table_name):
    pattern = rf"### {re.escape(table_name)}\s*\n(.*?)(?=\n###|\Z)"
    match = re.search(pattern, text, re.DOTALL | re.IGNORECASE)
    return match.group(1).strip() if match else None

def parse_csv_block(csv_block, expected_columns):
    rows = []
    reader = csv.reader(csv_block.splitlines())
    for row in reader:
        # skip empty or malformed rows
        if not row or len(row) < expected_columns:
            continue
        # skip repeated header rows
        if row[0].strip().lower() == "bank":
            continue
        rows.append(row[:expected_columns])
    return rows

# === main loop ===
file_counter = 0
for filename in sorted(os.listdir(INPUT_FOLDER)):
    if not filename.endswith(".txt"):
        continue

    file_counter += 1
    filepath = os.path.join(INPUT_FOLDER, filename)
    with open(filepath, "r", encoding="utf-8") as f:
        content = f.read()

    print(f"\n Processing file: {filename}")

    # Extract and parse Securities table
    securities_block = extract_table_blocks(content, "SECURITIES HOLDERS CSV")
    if securities_block:
        parsed_securities = parse_csv_block(securities_block, len(SECURITIES_HEADER))
        all_securities.extend(parsed_securities)
        print(f"    Added {len(parsed_securities)} securities holder rows.")
    else:
        print("No Securities table found.")

    # Extract and parse Insiders table
    insiders_block = extract_table_blocks(content, "INSIDERS CSV")
    if insiders_block:
        parsed_insiders = parse_csv_block(insiders_block, len(INSIDERS_HEADER))
        all_insiders.extend(parsed_insiders)
        print(f"   Added {len(parsed_insiders)} insider rows.")
    else:
        print(" No Insiders table found.")

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
 