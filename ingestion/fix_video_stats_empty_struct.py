import os, json, datetime
from io import StringIO
from dotenv import load_dotenv
from google.cloud import storage

def prune_empty(obj):
    # recursively drop keys where value is {} or []
    if isinstance(obj, dict):
        return {k: prune_empty(v) for k, v in obj.items() if v not in ({}, [])}
    if isinstance(obj, list):
        return [prune_empty(v) for v in obj if v not in ({}, [])]
    return obj

def main():
    load_dotenv()
    bucket_name = os.environ["GCS_BUCKET"]
    date_str = os.environ.get("DATE_OVERRIDE") or datetime.date.today().isoformat()
    src = f"raw/date={date_str}/video_stats.json"
    dst = f"raw/date={date_str}/video_stats_clean.json"

    client = storage.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])
    bucket = client.bucket(bucket_name)

    # download original
    blob = bucket.blob(src)
    raw = blob.download_as_text(encoding="utf-8")

    # clean line by line (NDJSON)
    cleaned_lines = []
    for line in raw.splitlines():
        if not line.strip():
            continue
        row = json.loads(line)
        row = prune_empty(row)
        cleaned_lines.append(json.dumps(row, ensure_ascii=False))

    # upload cleaned
    bucket.blob(dst).upload_from_string("\n".join(cleaned_lines), content_type="application/x-ndjson")
    print(f"Cleaned file written: gs://{bucket_name}/{dst}")

if __name__ == "__main__":
    main()