#!/usr/bin/env python3
"""
youtube_fetch_simple.py

Simpler, human-friendly script that:
- stores channel info (including subscriber_count and next_page_token)
- stores video rows with metrics

Two tables:
- channels (id PK) 
- videos (id PK, channel_ref INTEGER -> channels.id)

Fields captured per your request:
Duration (seconds), Title, Views, Likes, View/Like ratio (views divided by likes),
Comment count, Publishing date, Subscriber count (in channels table).
"""

import os
import re
import sqlite3
import requests
import argparse
from typing import Optional, List

API_KEY = os.getenv("YOUTUBE_API_KEY")
DB_DEFAULT = "youtube_simple.db"
MAX_DEFAULT = 25

YT_SEARCH = "https://www.googleapis.com/youtube/v3/search"
YT_VIDEOS = "https://www.googleapis.com/youtube/v3/videos"
YT_CHANNELS = "https://www.googleapis.com/youtube/v3/channels"

DUR_RE = re.compile(r'P(?:([0-9]+)D)?T?(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+)S)?')

def parse_duration_iso(d: Optional[str]) -> int:
    if not d:
        return 0
    m = DUR_RE.match(d)
    if not m:
        return 0
    days, hours, mins, secs = m.groups(default="0")
    return int(days)*86400 + int(hours)*3600 + int(mins)*60 + int(secs)

def init_db(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS channels(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT UNIQUE,
        title TEXT,
        subscriber_count INTEGER,
        next_page_token TEXT
      )
    """)
    cur.execute("""
      CREATE TABLE IF NOT EXISTS videos(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_id TEXT UNIQUE,
        channel_ref INTEGER,
        title TEXT,
        duration_seconds INTEGER,
        view_count INTEGER,
        like_count INTEGER,
        view_like_ratio REAL,
        comment_count INTEGER,
        published_at TEXT,
        FOREIGN KEY(channel_ref) REFERENCES channels(id)
      )
    """)
    conn.commit()

def get_channel_row(conn: sqlite3.Connection, channel_id: str) -> Optional[sqlite3.Row]:
    cur = conn.cursor()
    cur.execute("SELECT id, next_page_token FROM channels WHERE channel_id = ?", (channel_id,))
    return cur.fetchone()

def upsert_channel(conn: sqlite3.Connection, channel_id: str, title: str, subs: Optional[int], next_token: Optional[str]) -> int:
    cur = conn.cursor()
    cur.execute("""
      INSERT INTO channels(channel_id, title, subscriber_count, next_page_token)
      VALUES(?, ?, ?, ?)
      ON CONFLICT(channel_id) DO UPDATE SET
        title=excluded.title,
        subscriber_count=excluded.subscriber_count,
        next_page_token=excluded.next_page_token
    """, (channel_id, title, subs, next_token))
    conn.commit()
    cur.execute("SELECT id FROM channels WHERE channel_id = ?", (channel_id,))
    return cur.fetchone()[0]

def save_channel_token(conn: sqlite3.Connection, channel_id: str, token: Optional[str]):
    cur = conn.cursor()
    cur.execute("UPDATE channels SET next_page_token = ? WHERE channel_id = ?", (token, channel_id))
    conn.commit()

def fetch_search_ids(api_key: str, channel_id: str, max_results: int, page_token: Optional[str]) -> tuple[List[str], Optional[str]]:
    params = {
        "key": api_key, "channelId": channel_id, "part": "id",
        "order": "date", "type": "video", "maxResults": max_results
    }
    if page_token:
        params["pageToken"] = page_token
    r = requests.get(YT_SEARCH, params=params, timeout=15)
    r.raise_for_status()
    j = r.json()
    ids = [it["id"]["videoId"] for it in j.get("items", []) if it.get("id", {}).get("videoId")]
    return ids, j.get("nextPageToken")

def fetch_videos(api_key: str, ids: List[str]):
    if not ids:
        return []
    params = {"key": api_key, "id": ",".join(ids), "part": "snippet,contentDetails,statistics"}
    r = requests.get(YT_VIDEOS, params=params, timeout=20)
    r.raise_for_status()
    return r.json().get("items", [])

def fetch_channel_info(api_key: str, channel_id: str) -> Optional[dict]:
    params = {"key": api_key, "id": channel_id, "part": "snippet,statistics"}
    r = requests.get(YT_CHANNELS, params=params, timeout=15)
    r.raise_for_status()
    items = r.json().get("items", [])
    return items[0] if items else None

def fetch_and_store(api_key: str, db_file: str, channel_id: str, max_per_run: int = 25):
    if not api_key:
        raise RuntimeError("Provide a YOUTUBE_API_KEY environment variable or pass --key")
    if max_per_run < 1 or max_per_run > 25:
        raise ValueError("max_per_run must be 1..25")

    conn = sqlite3.connect(db_file)
    conn.row_factory = sqlite3.Row
    init_db(conn)

    # get channel info and store/update it
    ch = fetch_channel_info(api_key, channel_id)
    title = ch.get("snippet", {}).get("title", "") if ch else ""
    subs_raw = ch.get("statistics", {}).get("subscriberCount") if ch else None
    subs = int(subs_raw) if subs_raw and str(subs_raw).isdigit() else None

    # find existing progress token if present
    existing = get_channel_row(conn, channel_id)
    page_token = existing["next_page_token"] if existing and existing["next_page_token"] else None

    # we update channel row with the current subs and token (token updated later)
    channel_row_id = upsert_channel(conn, channel_id, title, subs, page_token)

    ids, next_token = fetch_search_ids(api_key, channel_id, max_per_run, page_token)
    if not ids:
        print("No video ids returned. Clearing progress token.")
        save_channel_token(conn, channel_id, None)
        conn.close()
        return

    items = fetch_videos(api_key, ids)
    cur = conn.cursor()
    inserted = 0
    skipped = 0
    for it in items:
        vid = it.get("id")
        snip = it.get("snippet", {})
        cd = it.get("contentDetails", {})
        st = it.get("statistics", {})

        title = snip.get("title", "")
        published = snip.get("publishedAt", "")
        dur = parse_duration_iso(cd.get("duration"))
        views = int(st.get("viewCount") or 0)
        likes = int(st.get("likeCount") or 0)
        comments = int(st.get("commentCount") or 0)
        view_like_ratio = (views / likes) if likes > 0 else None

        try:
            cur.execute("""INSERT INTO videos(
                video_id, channel_ref, title, duration_seconds, view_count,
                like_count, view_like_ratio, comment_count, published_at
            ) VALUES(?,?,?,?,?,?,?,?,?)""",
            (vid, channel_row_id, title, dur, views, likes, view_like_ratio, comments, published))
            conn.commit()
            inserted += 1
        except sqlite3.IntegrityError:
            skipped += 1
            continue

    # save next page token for next run
    save_channel_token(conn, channel_id, next_token)
    conn.close()

    print(f"Done. API returned {len(ids)} ids. Inserted {inserted}. Skipped (duplicates) {skipped}.")
    if next_token:
        print("Saved nextPageToken for the channel — next run will continue.")
    else:
        print("No nextPageToken returned (end reached or token expired).")

if __name__ == "__main__":
    p = argparse.ArgumentParser("simple youtube fetch")
    p.add_argument("--key", default=None, help="YouTube API key or set YOUTUBE_API_KEY")
    p.add_argument("--db", default=DB_DEFAULT, help="SQLite filename")
    p.add_argument("--channel", required=True, help="Channel ID (starts with UC...)")
    p.add_argument("--max", type=int, default=MAX_DEFAULT, help="Max results per run (≤25)")
    args = p.parse_args()
    key = args.key or API_KEY
    fetch_and_store(key, args.db, args.channel, args.max)
