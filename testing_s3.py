import os
import boto3

BUCKET_NAME = 'bank-y6-pdfs'
LOCAL_PDF_FOLDER = '/Users/nashlaturcios/Desktop/S3BucketTestData'  

s3 = boto3.client('s3')

for filename in os.listdir(LOCAL_PDF_FOLDER):
    if filename.endswith('.pdf'):
        full_path = os.path.join(LOCAL_PDF_FOLDER, filename)
        print(f"Uploading {filename}...")
        s3.upload_file(full_path, BUCKET_NAME, filename)
        print(f" Uploaded {filename}")

