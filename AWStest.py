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
EXPIRATION_TIME = 900  # 15 minutes
OUTPUT_DIR = "gemini_outputs_from_s3"
MODEL_NAME = "gemini-2.0-flash"
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
PROMPT = (
    "You are analyzing an FR Y-6 form submitted by a U.S. bank holding company. Extract only the following two tables "
    "in valid CSV format. Use the exact column names listed below. Label each table clearly and include only rows under each header.\n\n"
    "### SECURITIES HOLDERS CSV\n"
    "Bank,Town,Fiscal Year,Owner Name,Stock Class,Number of Shares,Percentage of Ownership, RSSD_ID\n"
    "### INSIDERS CSV\n"
    "Bank,Internal Title,Person,External Title,Affiliation,Fiscal Year, Occupation, RSSD_ID \n"
    "... [your detailed instructions continue here, trimmed for space] ..."
)

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
        cleaned_output = quote_csv_fields(output_text)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(cleaned_output)

        print(f" Output saved to: {output_path}")
        time.sleep(1)

    except httpx.HTTPStatusError as e:
        if e.response.status_code == 429:
            print("⚠️ Rate limit hit (HTTP 429). Stopping further requests.")
            break
        else:
            print(f" HTTP error {e.response.status_code}: {e}")
            break

    except Exception as e:
        print(f" Failed to process {url}: {e}")
        continue

end_time = time.time()  # 👈 Add this here
print(f"\n Total processing time: {round(end_time - start_time, 2)} seconds")