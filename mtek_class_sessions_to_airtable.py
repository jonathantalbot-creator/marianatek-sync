# file: mtek_class_sessions_to_airtable.py
import os, csv, time, requests, sys
from io import StringIO
from datetime import datetime, timedelta, timezone

BASE_URL = "https://spinco.marianatek.com/api"
ENDPOINT = "class_sessions"

MT_TOKEN = os.environ.get("MT_TOKEN")
AIRTABLE_SYNC_URL = os.environ.get("AIRTABLE_SYNC_URL")
AIRTABLE_PAT = os.environ.get("AIRTABLE_PAT")

# Pull the last 2 hours each run (1h schedule + 1h overlap to avoid gaps)
WINDOW_HOURS = int(os.environ.get("WINDOW_HOURS", "2"))

# CSV columns you want in Airtable (flattened from attributes/relationships)
CSV_FIELDS = [
    "id",
    "start_date",
    "start_time",
    "start_datetime",
    "end_datetime",
    "public",
    "capacity",
    "available_spots_count",
    "vip_user_count",
    "first_time_user_count",
    "checked_in_user_count",
    "standard_reservation_user_count",
    "waitlist_reservation_user_count",
    "waitlist_capacity",
    "public_waitlist_count",
    "duration",
    "layout_format",
    "location_display",
    "classroom_display",
    "class_type_display",
    "instructor_names",          # comma-joined
    "recurring_status",
    "recurring_id",
    "is_change_spots_enabled",
    "has_waitlist",
    "kiosk_check_in_start_datetime",
    "kiosk_check_in_end_datetime",
    # Relationship IDs (useful for joins)
    "location_id",
    "classroom_id",
    "layout_id",
    "class_session_type_id",
    "instructor_ids",            # comma-joined
    "tag_ids"                    # comma-joined
]

def auth_headers():
    if not MT_TOKEN:
        print("Missing MT_TOKEN env var", file=sys.stderr)
        sys.exit(1)
    return {
        "Authorization": f"Bearer {MT_TOKEN}",
        "Accept": "application/json"
    }

def extract_rel_id(rel):
    if not rel or not isinstance(rel, dict): 
        return None
    data = rel.get("data")
    if isinstance(data, dict):
        return data.get("id")
    return None

def extract_rel_ids(rel):
    if not rel or not isinstance(rel, dict): 
        return []
    data = rel.get("data")
    if isinstance(data, list):
        return [d.get("id") for d in data if isinstance(d, dict)]
    return []

def flatten(record):
    # JSON:API style: { type, id, attributes, relationships }
    rid = record.get("id")
    attr = record.get("attributes", {}) or {}
    rels = record.get("relationships", {}) or {}

    row = {
        "id": rid,
        "start_date": attr.get("start_date"),
        "start_time": attr.get("start_time"),
        "start_datetime": attr.get("start_datetime"),
        "end_datetime": attr.get("end_datetime"),
        "public": attr.get("public"),
        "capacity": attr.get("capacity"),
        "available_spots_count": len(attr.get("available_spots") or []),
        "vip_user_count": attr.get("vip_user_count"),
        "first_time_user_count": attr.get("first_time_user_count"),
        "checked_in_user_count": attr.get("checked_in_user_count"),
        "standard_reservation_user_count": attr.get("standard_reservation_user_count"),
        "waitlist_reservation_user_count": attr.get("waitlist_reservation_user_count"),
        "waitlist_capacity": attr.get("waitlist_capacity"),
        "public_waitlist_count": attr.get("public_waitlist_count"),
        "duration": attr.get("duration"),
        "layout_format": attr.get("layout_format"),
        "location_display": attr.get("location_display"),
        "classroom_display": attr.get("classroom_display"),
        "class_type_display": attr.get("class_type_display"),
        "instructor_names": ", ".join(attr.get("instructor_names") or []),
        "recurring_status": attr.get("recurring_status"),
        "recurring_id": attr.get("recurring_id"),
        "is_change_spots_enabled": attr.get("is_change_spots_enabled"),
        "has_waitlist": attr.get("has_waitlist"),
        "kiosk_check_in_start_datetime": attr.get("kiosk_check_in_start_datetime"),
        "kiosk_check_in_end_datetime": attr.get("kiosk_check_in_end_datetime"),
        # relationships
        "location_id": extract_rel_id(rels.get("location")),
        "classroom_id": extract_rel_id(rels.get("classroom")),
        "layout_id": extract_rel_id(rels.get("layout")),
        "class_session_type_id": extract_rel_id(rels.get("class_session_type")),
        "instructor_ids": ", ".join(extract_rel_ids(rels.get("instructors"))),
        "tag_ids": ", ".join(extract_rel_ids(rels.get("tags")))
    }
    return row

def fetch_page(since_iso, page, per_page=500):
    # Prefer filtering on start_datetime (ISO8601 Z). Adjust if you need a different field.
    # Many JSON:API implementations accept comma-separated filters; you already showed $filter=...
params = None  # we'll build manually
url = f"{BASE_URL}/{ENDPOINT}?$filter=start_time ge {since_iso}&page={page}&per_page={per_page}"
r = requests.get(url, headers=auth_headers(), timeout=60)
    url = f"{BASE_URL}/{ENDPOINT}"
    r = requests.get(url, headers=auth_headers(), params=params, timeout=60)
    r.raise_for_status()
    return r.json()

def pull_window(hours=2):
    # Window with overlap so you never miss late creations
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).strftime("%Y-%m-%dT%H:%M:%SZ")
    page = 1
    per_page = 500  # try 500; if the API rejects it, lower to 200/100

    rows = []
    while True:
        payload = fetch_page(since, page, per_page)
        data = payload.get("data") or []
        for rec in data:
            rows.append(flatten(rec))

        meta = payload.get("meta", {}).get("pagination", {}) or {}
        total_pages = meta.get("pages") or (page if len(data) == 0 else page+1)
        if page >= total_pages or len(data) == 0:
            break
        page += 1
        time.sleep(0.2)  # be kind to the API
    return rows

def to_csv_string(rows):
    buf = StringIO()
    w = csv.DictWriter(buf, fieldnames=CSV_FIELDS, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow(r)
    return buf.getvalue()

def upload_to_airtable(csv_text):
    if not AIRTABLE_SYNC_URL or not AIRTABLE_PAT:
        print("Skipping Airtable upload (missing AIRTABLE env vars)")
        return
    resp = requests.post(
        AIRTABLE_SYNC_URL,
        headers={
            "Authorization": f"Bearer {AIRTABLE_PAT}",
            "Content-Type": "text/csv"
        },
        data=csv_text.encode("utf-8")
    )
    if resp.status_code >= 400:
        print(resp.text, file=sys.stderr)
        resp.raise_for_status()
    print("Airtable upload OK")

if __name__ == "__main__":
    rows = pull_window(hours=WINDOW_HOURS)
    print(f"Fetched {len(rows)} class_sessions in the last {WINDOW_HOURS}h")
    csv_text = to_csv_string(rows)   # builds header even if rows == 0
    upload_to_airtable(csv_text)     # always post so Airtable initializes
