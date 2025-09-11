import os, json, datetime
from dotenv import load_dotenv
from google.cloud import storage

def main():
    # Load .env from project root
    load_dotenv()

    bucket_name = os.environ.get("GCS_BUCKET")
    creds_path  = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    if not bucket_name:
        raise RuntimeError("Missing GCS_BUCKET in .env")
    if not creds_path or not os.path.exists(creds_path):
        raise RuntimeError("Missing or bad GOOGLE_APPLICATION_CREDENTIALS path in .env")

    # Make a small NDJSON blob
    rows = [
        {"msg": "hello, gcs", "ts": datetime.datetime.utcnow().isoformat()+"Z"},
        {"ok": True}
    ]
    ndjson = "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)

    # Write to: raw/date=YYYY-MM-DD/test.json
    date_str = datetime.date.today().isoformat()
    object_name = f"raw/date={date_str}/test.json"

    client = storage.Client.from_service_account_json(creds_path)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_name)
    blob.upload_from_string(ndjson, content_type="application/x-ndjson")

    print(f"Uploaded gs://{bucket_name}/{object_name}")

if __name__ == "__main__":
    main()