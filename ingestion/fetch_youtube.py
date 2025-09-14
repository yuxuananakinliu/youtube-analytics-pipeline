import os, json, datetime, time
from pathlib import Path
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.cloud import storage

def load_channels():
    cfg_path = Path(__file__).parent / "channel_list.json"
    with open(cfg_path, "r", encoding="utf-8") as f:
        cfg = json.load(f)
    return cfg["channels"], int(cfg.get("max_results_per_channel", 10))

def youtube_client():
    api_key = os.environ["YOUTUBE_API_KEY"]
    return build("youtube", "v3", developerKey=api_key, cache_discovery=False)

def gcs_client():
    return storage.Client.from_service_account_json(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

def fetch_channel(yt, cid):
    resp = yt.channels().list(part="snippet,statistics", id=cid).execute()
    return resp.get("items", [None])[0]

def fetch_recent_videos(yt, cid, max_results):
    resp = yt.search().list(
        part="id",
        channelId=cid,
        maxResults=max_results,
        order="date",
        type="video"
    ).execute()
    return [it["id"]["videoId"] for it in resp.get("items", []) if "videoId" in it["id"]]

def fetch_video_stats(yt, ids):
    out = []
    for i in range(0, len(ids), 50):
        chunk = ids[i:i+50]
        resp = yt.videos().list(part="snippet,statistics,contentDetails", id=",".join(chunk)).execute()
        out.extend(resp.get("items", []))
        time.sleep(0.1)
    return out

def ndjson(rows):
    return "\n".join(json.dumps(r, ensure_ascii=False) for r in rows)

def upload(gcs, bucket, prefix, filename, rows):
    if not rows: return
    blob = gcs.bucket(bucket).blob(f"{prefix}/{filename}")
    blob.upload_from_string(ndjson(rows), content_type="application/x-ndjson")
    print(f"Uploaded {filename} → gs://{bucket}/{prefix}/")

def prune_empty(obj):
    if isinstance(obj, dict):
        return {k: prune_empty(v) for k, v in obj.items() if v not in ({}, [])}
    if isinstance(obj, list):
        return [prune_empty(v) for v in obj if v not in ({}, [])]
    return obj


def run_ingestion(exec_date=None):
    load_dotenv()
    bucket = os.environ["GCS_BUCKET"]

    # pick up DATE_OVERRIDE from env if caller didn’t pass it
    exec_date = exec_date or os.environ.get("DATE_OVERRIDE")
    date_str = exec_date or os.environ.get("DATE_OVERRIDE") or datetime.date.today().isoformat()
    prefix = f"raw/date={date_str}"

    yt = youtube_client()
    gcs = gcs_client()

    channels, max_results = load_channels()
    ch_docs, vid_docs, stat_docs = [], [], []

    for cid in channels:
        ch = fetch_channel(yt, cid)
        if ch: ch_docs.append(ch)

        vids = fetch_recent_videos(yt, cid, max_results)
        for vid in vids: vid_docs.append({"videoId": vid, "channelId": cid})

        if vids:
            stat_docs.extend(fetch_video_stats(yt, vids))

    # Pruning
    stat_docs = [prune_empty(x) for x in stat_docs]

    upload(gcs, bucket, prefix, "channels.json", ch_docs)
    upload(gcs, bucket, prefix, "video_ids.json", vid_docs)
    upload(gcs, bucket, prefix, "video_stats.json", stat_docs)

if __name__ == "__main__":
    run_ingestion()