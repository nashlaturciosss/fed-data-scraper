import json
import io
import os
import httpx
import time
import subprocess
from google import genai
from google.genai import types

# === CONFIG ===
BUCKET_NAME = 'bank-y6-pdfs'
EXPIRATION_TIME = 20000  # 15 minutes
OUTPUT_DIR = "gemini_outputs_from_s3"
MODEL_NAME = "gemini-2.5-flash"
GENAI_API_KEY = "AIzaSyABQAkMpuDJQZekMDbi33Qh8oxwcvEsmRI"

# === Generate Pre-signed URL using AWS CLI ===
def generate_presigned_url_cli(bucket, key, expires):
    command = [
        "aws", "s3", "presign", f"s3://{bucket}/{key}",
        "--expires-in", str(expires)
    ]
    url = subprocess.check_output(command).decode().strip()
    return url

# === Get list of files from S3 ===
import boto3
s3 = boto3.client('s3')
response = s3.list_objects_v2(Bucket=BUCKET_NAME)
pdf_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.pdf')]

# === Generate URLs and Save to JSON ===
presigned_urls = []
for key in pdf_files:
    try:
        url = generate_presigned_url_cli(BUCKET_NAME, key, EXPIRATION_TIME)
        presigned_urls.append(url)
        print(f" Generated: {url}")
    except Exception as e:
        print(f" Failed to generate URL for {key}: {e}")

with open('temp_presigned_urls.json', 'w') as f:
    json.dump(presigned_urls, f, indent=2)

print("\n All URLs saved to temp_presigned_urls.json")

# === Setup Gemini ===
client = genai.Client(api_key=GENAI_API_KEY)

# === Ensure output directory exists ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Load PDF URLs ===
with open("temp_presigned_urls.json", "r") as f:
    pdf_urls = json.load(f)

# === Prompt ===

PROMPT = """
    "You are analyzing an FR Y-6 form submitted by a U.S. bank holding company. Extract only the following two tables in valid CSV format. Use the exact column names listed below. Label each table clearly and include only rows under each header.\n\n"
    "### SECURITIES HOLDERS CSV\n"
    "Bank,Town,Fiscal Year,Owner Name,Stock Class,Number of Shares,Percentage of Ownership,RSSD_ID\n"
    "**IMPORTANT: Every row in the SECURITIES HOLDERS CSV MUST have EXACTLY 8 columns. Ensure correct alignment. Fields containing commas or multiple words MUST be enclosed in double quotes (\").**\n\n"
    "### INSIDERS CSV\n"
    "Bank,Internal Title, Person, Fiscal Year, Occupation, Percentage of Voting Shares, RSSD_ID, Percentage of Voting Shares in Subsidiaries\n"
    "**IMPORTANT: Every row in the INSIDERS CSV MUST have EXACTLY 8 columns. Ensure correct alignment. Fields containing commas or multiple words MUST be enclosed in double quotes (\").**\n\n"
    "Strict Extraction Instructions:\n"
    "Extract the Bank name and Fiscal Year from the first page of the document.\n\n"
    "IMPORTANT RULE:\n"
    "• Carefully check the checklist section of the FR Y-6 form (usually near page 2).\n"
    "• If the report says \"No\" for Report Item 3, return the SECURITIES HOLDERS CSV with only the header row.\n"
    "• If the report says \"No\" for Report Item 4, return the INSIDERS CSV with only the header row.\n"
    "• Do not use information from the cover page (like names, titles, or officers) to populate either table if the checklist says \"No\".\n"
    "• If both items are marked \"No\", return both tables with only their respective headers.\n"
    "• Do not include explanations, commentary, or notes. Return only the labeled tables in plain CSV format.\n\n"
    "For the SECURITIES HOLDERS table (Report Item 3):\n"
    "• Use data from both Report Item 3(1) and 3(2).\n"
    "• \"Bank\" must be the **full, official name of the bank holding company** (e.g., \"ABDO Investments, Inc.\"). This entire name MUST be treated as a single field. If it contains commas, enclose it in quotes.\n"
    "• \"Owner Name\" comes from Column A. The **entire name of the person or entity** (e.g., \"Jay M. Abdo\", \"Kao, Cheng Yuan\") must be captured as a **single field**. If it contains commas, enclose it in quotes.\n"
    "• \"Town\" should strictly be a **single field** in 'City, State' format (e.g., 'Naples, FL', 'Houston, TX'). If city and state are in separate columns, combine them into one field. If they are already combined, extract that combined value. This field MUST be enclosed in quotes if it contains a comma.\n"
    "• \"Stock Class\" must come from one of the reported items in 3(1). Examples: common stock, Class A Voting Shares, preferred, etc.\n"
    "  - If a person holds more than one type of stock, make duplicate rows for each stock type.\n"
    "  - If unspecified, return \"n/a\".\n"
    "• \"Number of Shares\" must be extracted only from the numeric part before the word \"shares\".\n"
    "  - For example, from \"16,531 shares – 14.73%\" extract \"16531\" as Number of Shares.\n"
    "• \"Percentage of Ownership\" must come from the value after the dash or \"–\", like \"14.73%\".\n"
    "• Never combine shares and percentage in a single column. Do not include the word \"shares\".\n"
    "• If a value is missing, use \"n/a\".\n\n"
    "For the INSIDERS table (Report Item 4):\n"
    "• Every row MUST contain exactly 8 fields. Use \"n/a\" where data is missing to maintain alignment.\n"
    "• \"Bank\" must be the **full, official name of the bank holding company** as a single field. If it contains commas, enclose it in quotes.\n"
    "• \"Person\" comes from Item 4(1), Column A. It should be the **full Name of the person** (e.g., \"Wu, Wen Lung\", \"Chang, Ray S\") captured as a **single field**. This field MUST be enclosed in quotes if it contains a comma.\n"
    "• \"Internal Title\" must be a **single field** containing a comma-separated list of all **unique** titles and positions held by the person. Extract these titles by combining relevant information ONLY from ‘Title or Position with the Holding Company’ and ‘Title or Position with Subsidiaries’ columns. Prioritize actual roles and titles (e.g., Director, CEO, President). **DO NOT include company names, abbreviations like \"INC\", \"LLC\", \"LP\", or other non-title/non-role text in this field, nor should you include addresses or locations. For example, 'INC.' is NOT a title.** This field MUST be enclosed in quotes if it contains commas. If a title appears twice, return only one. If no titles are found, use \"n/a\".\n"
    "• \"Fiscal Year\" must match the report year from the first page (e.g., 2017).\n"
    "• \"Occupation\" must come ONLY from Item 4(2): Principal Occupation, if other than with holding company.\n"
    "• \"RSSD_ID\" must come ONLY from a value that says RSSD_ID or ID_RSSD. If none are found, return \"n/a\".\n"
    "• \"Percentage of Voting Shares\" must come ONLY from a value that says (4)(a) Percentage of Voting Shares in Bank Holding Company. If none are found, return \"n/a\".\n"
    "• \"Percentage of Voting Shares in Subsidiaries\" must come ONLY from a value that says (4)(b) Percentage of Voting Shares in Subsidiaries.\n"
    "  - Do not pull data from “List names of other companies...” unless it’s clearly from (4)(b).\n"
    "  - If none are found, return \"n/a\".\n"
    "• Do NOT shift fields. Every value must appear in its **exact corresponding column**. Every row MUST have exactly the same number of columns as the header.\n\n"
    "GENERAL RULES:\n"
    "• Do not hallucinate or infer missing data — use \"n/a\" instead.\n"
    "• Do not include summaries, commentary, explanations, or notes.\n"
    "• Ensure both CSV tables are returned in plain text and clearly labeled.\n"
    "• Maintain the header format and column count exactly as shown above. Each row must strictly adhere to the defined 8 columns for both tables.\n"
    "• If a given column has multiple values (e.g., one person holds both common stock and Class A Voting Shares), make separate rows for each value.\n"
"""


# === Quote CSV Utility ===
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

# === Process Each URL ===
start_time = time.time()
for i, url in enumerate(pdf_urls, start=1):
    try:
        file_name = url.split("/")[-1].split("?")[0]  # strip query params
        output_path = os.path.join(OUTPUT_DIR, file_name.replace(".pdf", ".txt"))

        if os.path.exists(output_path):
            print(f"Skipping {file_name} (already processed)")
            continue

        print(f"\n [{i}] Fetching PDF from: {url}")
        pdf_bytes = httpx.get(url).content
        pdf_stream = io.BytesIO(pdf_bytes)

        print(f"Uploading to Gemini: {file_name}")
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
        cleaned_output = quote_csv_fields(output_text)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned_output)

        print(f" Output saved to: {output_path}")
        time.sleep(1)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            print("Rate limit hit (HTTP 429). Stopping further requests.")
            break
        else:
            print(f" HTTP error {e.response.status_code}: {e}")
            break

    except Exception as e:
        print(f" Failed to process {url}: {e}")
        continue

end_time = time.time()  # Add this here
print(f"\n Total processing time: {round(end_time - start_time, 2)} seconds")