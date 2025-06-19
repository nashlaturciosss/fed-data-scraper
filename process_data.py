import json
import io
import os
import httpx
import time
from google import genai
from google.genai import types

# === config ===
PDF_URL_FILE = "pdf_urls.json"
OUTPUT_DIR = "gemini_outputs"
MODEL_NAME = "gemini-2.0-flash"


# === gemini client set up ===
client = genai.Client(api_key="AIzaSyABQAkMpuDJQZekMDbi33Qh8oxwcvEsmRI")

# === load pdf urls ===
with open(PDF_URL_FILE, "r") as f:
    pdf_urls = json.load(f)

# === make sure output dirs exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === define prompt ===
PROMPT = (
     "You are analyzing an FR Y-6 form submitted by a U.S. bank holding company. Extract only the following two tables "
    "in valid CSV format. Use the exact column names listed below. Label each table clearly and include only rows under each header.\n\n"

    "### SECURITIES HOLDERS CSV\n"
    "Bank,Town,Fiscal Year,Owner Name,Stock Class,Number of Shares,Percentage of Ownership, RSSD_ID\n"

    "### INSIDERS CSV\n"
    "Bank,Internal Title,Person,External Title,Affiliation,Fiscal Year, Occupation, RSSD_ID \n"

    "Strict Extraction Instructions:\n"
    "- Extract the Bank name and Fiscal Year from the first page of the document.\n"

    "- For the SECURITIES HOLDERS table (Report Item 3):\n"
    "  • Use data from both Report Item 3(1) and 3(2).\n"
    "  • 'Owner Name' comes from Column A.\n"
    "  • 'Town' comes from Column B (City) and Column C (State) (for instance, if you have city LA and state CA, value should be LA, CA).\n"
    "  • 'Stock Class' must come from one o fthe reproted items in 3(1). Some examples might include common stock, Class A Voting Shares, Class A non-voting shares, preferred, etc. If a person holds more than one type of stock, return all the names of the differente stock types but make duplicate rows to separate the values. If unspecified, return 'n/a'.\n"
    "  • 'Number of Shares' must be extracted only from the numeric part before the word 'shares'.\n"
    "    • For example, from '16,531 shares – 14.73%' extract '16531' as Number of Shares.\n"
    "  • 'Percentage of Ownership' must come from the value after the dash or '–', like '14.73%'.\n"
    "  • Never combine shares and percentage in a single column. Do not include the word 'shares'.\n"
    "  • If a value is missing, use 'n/a'.\n"

    "- For the INSIDERS table (Report Item 4):\n"
    "  • Each row must contain exactly six fields. Use 'n/a' where data is missing to maintain alignment.\n"
    "  • 'Person' comes from Item 4(1), Column A.\n"
    "  • 'Internal Title' must come ONLY from Item 4(3): Title or Position with the Holding Company. and Title or Position with direct and indirect subsidiaries. If a value appears twice, only return one\n"
    "  • 'External Title' must come ONLY from Item 4(5): Title or Position with any other company in which the person is a director, trustee, partner, or executive officer. If a value appears twice, only return one\n"
    "  • 'Affiliation' must come ONLY from Item 4(5), Column B (or 4c where applicable). The value of affiliation must be only the full name of the external companies or firms. Not the percentage they own, or the title they hold inside the company. Only the name of the external company or firm. If a person is affiliated with more than one company, return all the names of the differente companies but make duplicate rows to separate the values.\n"
    "  • 'Fiscal Year' must match the report year from the first page (e.g., '2017').\n"
    "  • 'Occupation' must come ONLY from Item 4(2): Principal Occupation, if other than with holding company.\n"
    " • 'RSSD_ID' must come ONLY from a value that says RSSD_ID or ID_RSSD. Try to find this, it could be in small print. If none are found, return n/a. \n"
    " • Do NOT shift fields — each value must appear in the correct column. Do NOT let 'Affiliation' appear in 'Fiscal Year'.\n"

    "- GENERAL RULES:\n"
    "  • Do not hallucinate or infer missing data — use 'n/a' instead.\n"
    "  • Do not include summaries, commentary, explanations, or notes.\n"
    "  • Ensure both CSV tables are returned in plain text and clearly labeled.\n"
    "  • Maintain the header format and column count exactly as shown above.\n"
    "  • Wrap every CSV field in double quotes — even if the value doesn't contain com\n"
    "  • If a given column has multiple values (e.g one person holds a common stock and Class A Voting Shares, make separate rows for each value)\n"
)

# === Function to quote CSV fields safely ===
def quote_csv_fields(text):
    fixed_lines = []
    for line in text.splitlines():
        if line.strip() == "":
            continue
        if ',' in line:
            parts = [f'"{p.strip()}"' if not p.strip().startswith('"') else p.strip() for p in line.split(',')]
            fixed_lines.append(','.join(parts))
        else:
            fixed_lines.append(line.strip())
    return '\n'.join(fixed_lines)

# === main preocessing loop ===
for i, url in enumerate(pdf_urls, start=1):
    try:
        file_name = url.split("/")[-1]
        output_path = os.path.join(OUTPUT_DIR, file_name.replace(".pdf", ".txt"))

        if os.path.exists(output_path):
            print(f"Skipping {file_name} (already processed)")
            continue

        print(f"\n [{i}] Fetching PDF from: {url}")
        pdf_bytes = httpx.get(url).content
        pdf_stream = io.BytesIO(pdf_bytes)

        print(f"⬆Uploading to Gemini: {file_name}")
        uploaded_file = client.files.upload(
            file=pdf_stream,
            config={"mime_type": "application/pdf"}
        )

        print("Sending prompt to Gemini...")
        response = client.models.generate_content(
            model=MODEL_NAME,
            contents=[uploaded_file, PROMPT]
        )

        output_text = response.text.strip()

        # Quote all fields to prevent comma-related formatting issues
        cleaned_output = quote_csv_fields(output_text)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned_output)

        print(f" Output saved to: {output_path}")
        time.sleep(1)  # polite delay to avoid RPM spike

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            print("Rate limit hit (HTTP 429). Stopping further requests.")
            break
        else:
            print(f" HTTP error {e.response.status_code}: {e}")
            break

    except Exception as e:
        print(f"Failed to process {url}: {e}")
        continue
