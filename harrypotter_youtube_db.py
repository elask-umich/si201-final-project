
import sqlite3
import argparse
from typing import List, Dict, Optional

# ------------------ Helper DB functions ------------------

def create_final_schema(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS channels (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        channel_id TEXT UNIQUE,
        title TEXT,
        subscriber_count INTEGER
    )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS videos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        video_id TEXT UNIQUE,
        channel_ref INTEGER,
        title TEXT,
        duration_seconds INTEGER,
        published_at TEXT,
        FOREIGN KEY(channel_ref) REFERENCES channels(id)
    )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS video_stats (
        video_ref INTEGER PRIMARY KEY,
        view_count INTEGER,
        like_count INTEGER,
        comment_count INTEGER,
        view_like_ratio REAL,
        FOREIGN KEY(video_ref) REFERENCES videos(id)
    )
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS characters (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        character_id TEXT UNIQUE,
        name TEXT,
        house TEXT,
        alt_names TEXT,
        species TEXT,
        role TEXT,
        patronus TEXT,
        gender TEXT,
        age TEXT
    )
    """)
    conn.commit()

def get_final_channel_id(conn: sqlite3.Connection, channel_id: str) -> Optional[int]:
    cur = conn.cursor()
    cur.execute("SELECT id FROM channels WHERE channel_id = ?", (channel_id,))
    r = cur.fetchone()
    return r[0] if r else None

def upsert_final_channel(conn: sqlite3.Connection, channel_id: str, title: Optional[str], subs: Optional[int]) -> int:
    cur = conn.cursor()
    # insert or update
    cur.execute(
    """
    INSERT INTO channels(channel_id, title, subscriber_count)
    VALUES(?, ?, ?)
    ON CONFLICT(channel_id) DO UPDATE SET title=excluded.title, subscriber_count=excluded.subscriber_count
    """,
    (channel_id, title, subs)
    )
    conn.commit()
    return get_final_channel_id(conn, channel_id)

def fetch_unimported_videos_from_source(src_conn: sqlite3.Connection, final_conn: sqlite3.Connection, limit: int) -> List[Dict]:
    """Return up to `limit` video rows from source that are not yet in final (by video_id).
        The function expects the source DB to have tables named `channels` and `videos`
        with a schema similar to the one used in the youtube fetcher (channel_id available).
    """
    src_cur = src_conn.cursor()
    # Attempt to select videos that final doesn't have yet
    query = (
        "SELECT v.*, c.channel_id as source_channel_id, c.title as source_channel_title, c.subscriber_count as source_channel_subs "
        "FROM videos v JOIN channels c ON v.channel_ref = c.id "
        "WHERE NOT EXISTS (SELECT 1 FROM main.videos f WHERE f.video_id = v.video_id) "
        "LIMIT ?"
    )
    # Note: we'll attach final DB as 'main' when opening connections so this subquery works.
    src_cur.execute(query, (limit,))
    cols = [d[0] for d in src_cur.description]
    rows = src_cur.fetchall()
    result = []
    for r in rows:
        result.append({cols[i]: r[i] for i in range(len(cols))})
    return result

def import_youtube_from_source(src_db_path: str, final_db_path: str, limit: int = 25) -> None:
    """Import up to `limit` new videos from src_db_path into final_db_path."""
    src_conn = sqlite3.connect(src_db_path)
    final_conn = sqlite3.connect(final_db_path)

# Make final conn the "main" and attach src as 'src' so we can reference main in queries
    create_final_schema(final_conn)

# We'll query the source using its connection but use final to insert
    videos = fetch_unimported_videos_from_source(src_conn, final_conn, limit)
    if not videos:
        print("No new videos to import from source.")
        src_conn.close()
        final_conn.close()
        return
    fcur = final_conn.cursor()
    inserted = 0

    for v in videos:
        vid = v.get('video_id')
        title = v.get('title')
        duration = v.get('duration_seconds') if 'duration_seconds' in v else v.get('duration')
        published = v.get('published_at') if 'published_at' in v else v.get('publishedAt')

        # channel mapping: ensure channel exists in final
        source_channel_id = v.get('source_channel_id')
        source_channel_title = v.get('source_channel_title')
        source_subs = v.get('source_channel_subs')
        final_channel_row_id = get_final_channel_id(final_conn, source_channel_id)

        if final_channel_row_id is None:
            final_channel_row_id = upsert_final_channel(final_conn, source_channel_id, source_channel_title, source_subs)

        try:
            fcur.execute(
            "INSERT INTO videos(video_id, channel_ref, title, duration_seconds, published_at) VALUES (?, ?, ?, ?, ?)",
            (vid, final_channel_row_id, title, duration, published)
            )
            final_vid_id = fcur.lastrowid
            # insert stats if available in source table
            # Try common stat column names
            view_count = v.get('view_count') or v.get('viewCount') or 0
            like_count = v.get('like_count') or v.get('likeCount') or 0
            comment_count = v.get('comment_count') or v.get('commentCount') or 0
            view_like_ratio = (float(view_count) / float(like_count)) if like_count and like_count != 0 else None

            fcur.execute(
            "INSERT OR REPLACE INTO video_stats(video_ref, view_count, like_count, comment_count, view_like_ratio) VALUES (?, ?, ?, ?, ?)",
            (final_vid_id, view_count, like_count, comment_count, view_like_ratio)
            )

            final_conn.commit()
            inserted += 1

        except sqlite3.IntegrityError:
            # video_id already exists in final (race condition) — skip
            continue


    print(f"Imported {inserted} videos into {final_db_path} from {src_db_path}.")
    src_conn.close()
    final_conn.close()



def import_hp_placeholder(hp_db_path: str, final_db_path: str, limit: int = 25): 
    """Placeholder for importing HP data from partner DB."""
    #gets data from fetch harry potter!! so it copies 25 characters from the database into the final joined database. CHAT WE ARE MERGING!!!!
    hp_conn = sqlite3.connect(hp_db_path)
    final_conn = sqlite3.connect(final_db_path)
    hp_cur = hp_conn.cursor() 
    final_cur = final_conn.cursor() 
    create_final_schema(final_conn)
    hp_cur.execute("SELECT name, house, species, role, patronus, gender, age, alternative_names FROM characters")
    all_hp_rows = hp_cur.fetchall() 
    counter = 0 
    for row in all_hp_rows: 
        if counter >= limit: 
            break 
        name = row[0] 
        final_cur.execute("SELECT id FROM characters WHERE name = ?", (name,))
        existing = final_cur.fetchone() #starts checking for duplicate names 
        if existing: #creates if scenario if the name is already present 
            continue #continues to not include dcuplicate names hehehe
        house = row[1]
        species = row[2]
        role = row[3]
        patronus = row[4]
        gender = row[5] 
        age = row[6]
        alt_names = row[7]
        final_cur.execute("""INSERT INTO characters(name, house, species, role, patronus, gender, age, alt_names)VALUES(?,?,?,?,?,?,?,?)""", (name, house, species, role, patronus, gender, age, alt_names,)) 
        final_conn.commit() 
        counter += 1 
    hp_conn.close()
    final_conn.close() 
    #safety printing confirmation, currently manifesting this stuff works please omg 
    print(f"imported{counter} hp characters into combined base")
    print("run until all copied into final")




    pass




# ------------- calculations for both (placeholder start) ------------------


def main():
    p = argparse.ArgumentParser("final_db_builder")
    p.add_argument("--youtube-src", required=True, help="Path to teammate's youtube_db.db")
    p.add_argument("--final", default="final.db", help="Final combined DB filename")
    p.add_argument("--limit", type=int, default=25, help="Max rows to import per run from each source (<=25)")
    p.add_argument("--import-hp", default=None, help="(optional) partner DB path to import HP data")
    args = p.parse_args()


    if args.limit < 1 or args.limit > 25:
        raise SystemExit("limit must be between 1 and 25")

    import_youtube_from_source(args.youtube_src, args.final, args.limit)

    if args.import_hp:
        import_hp_placeholder(args.import_hp, args.final, args.limit)




if __name__ == '__main__':
    main()