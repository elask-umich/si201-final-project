
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
    cur.execute("""
    CREATE TABLE IF NOT EXISTS character_mentions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        character_ref INTEGER,
        video_id INTEGER,
        mention_count INTEGER,
        FOREIGN KEY(character_ref) REFERENCES characters(id),
        FOREIGN KEY(video_id) REFERENCES videos(id)
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
            "SELECT v.*, "
            "c.channel_id AS source_channel_id, "
            "c.title AS source_channel_title, "
            "c.subscriber_count AS source_channel_subs "
            "FROM videos v "
            "JOIN channels c ON v.channel_ref = c.id "
            "WHERE NOT EXISTS ("
            "    SELECT 1 FROM final.videos f WHERE f.video_id = v.video_id"
            ") "
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

    # Attach final DB inside the source connection so the query can reference final.videos
    src_conn.execute(f"ATTACH DATABASE '{final_db_path}' AS final")

    create_final_schema(final_conn)

    # Fetch videos that are not already in final
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

        # channel mapping
        source_channel_id = v.get('source_channel_id')
        source_channel_title = v.get('source_channel_title')
        source_subs = v.get('source_channel_subs')

        final_channel_row_id = get_final_channel_id(final_conn, source_channel_id)
        if final_channel_row_id is None:
            final_channel_row_id = upsert_final_channel(
                final_conn, source_channel_id, source_channel_title, source_subs
            )

        try:
            fcur.execute(
                "INSERT INTO videos(video_id, channel_ref, title, duration_seconds, published_at) VALUES (?, ?, ?, ?, ?)",
                (vid, final_channel_row_id, title, duration, published)
            )
            final_vid_id = fcur.lastrowid

            # Stats
            view_count = v.get('view_count') or v.get('viewCount') or 0
            like_count = v.get('like_count') or v.get('likeCount') or 0
            comment_count = v.get('comment_count') or v.get('commentCount') or 0
            view_like_ratio = (float(view_count) / float(like_count)) if like_count else None

            fcur.execute(
                "INSERT OR REPLACE INTO video_stats(video_ref, view_count, like_count, comment_count, view_like_ratio) VALUES (?, ?, ?, ?, ?)",
                (final_vid_id, view_count, like_count, comment_count, view_like_ratio)
            )

            final_conn.commit()
            inserted += 1

        except sqlite3.IntegrityError:
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
    try:
        hp_cur.execute("SELECT name, house, species, role, patronus, gender, age, alt_names FROM characters")
    except sqlite3.OperationalError:
        hp_cur.execute("SELECT name, house, species, role, patronus, gender, age, NULL as alt_names FROM characters")

    all_hp_rows = hp_cur.fetchall() 
    counter = 0 
    for row in all_hp_rows: 
        if counter >= limit: 
            break 
        name = row[0] 
        final_cur.execute("SELECT id FROM characters WHERE name = ?", (name,))
        existing = final_cur.fetchone() #starts checking for duplicate names 
        if existing: #creates if scenario if the name is already present 
            continue #continues to not include duplicate names hehehe
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

def build_char_mentions(final_db_path: str, limit: int = 25):
    #goes over the final combined base and finds yt videos from ytber where the title contains the hp char name - uses that to fill the char mentions table
    conn = sqlite3.connect(final_db_path)
    cur = conn.cursor() 
    create_final_schema(conn)
    cur.execute("SELECT id, name FROM characters")
    characters = cur.fetchall() 
    cur.execute("SELECT id, title FROM videos")
    videos = cur.fetchall()
    added = 0 
    for char in characters: 
        char_id = char[0]
        char_name = char[1]
        if char_name is None: 
            continue 
        char_name_lower = char_name.lower() 
        for vid in videos: 
            if added>=limit: 
                break 
            video_id_in_table= vid[0]
            video_title = vid[1]
            if video_title is None: 
                continue 
            title_lower = video_title.lower() 
            if char_name_lower in title_lower: 
                cur.execute(""" SELECT id FROM character_mentions WHERE character_ref = ? AND video_id = ? """, (char_id, video_id_in_table))
                exists = cur.fetchone() 
                if exists: 
                    continue 
                mention_count = 1 
                cur.execute("""INSERT INTO character_mentions(character_ref, video_id, mention_count) VALUES(?,?,?)""", (char_id, video_id_in_table, mention_count))
                conn.commit() 
                added += 1 
        if added >= limit: 
            break
    conn.close() 
    print(f"added {added} new character mention rows")
    print("run again to add to mention table")






# ------------- calculations for both (placeholder start) ------------------

def calc_character_popularity(final_db_path: str, filename: str = "character_popularity.txt"):
    conn = sqlite3.connect(final_db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            characters.name,
            COUNT(character_mentions.video_id) AS mention_count,
            SUM(video_stats.view_count) AS total_views
        FROM characters
        LEFT JOIN character_mentions
            ON characters.id = character_mentions.character_ref
        LEFT JOIN videos
            ON character_mentions.video_id = videos.id
        LEFT JOIN video_stats
            ON videos.id = video_stats.video_ref
        GROUP BY characters.id
        ORDER BY mention_count DESC
    """)

    rows = cur.fetchall()
    conn.close()

    with open(filename, "w", encoding="utf-8") as f:
        f.write("=== Character Popularity (Mentions + Total Views) ===\n\n")
        for row in rows:
            name = row[0]
            mentions = row[1] or 0
            views = row[2] or 0
            f.write(f"{name}\n")
            f.write(f"  Mentions: {mentions}\n")
            f.write(f"  Total Views: {views}\n\n")

    print(f"Saved popularity results to {filename}")


def calc_character_appearances_in_yt_videotitle(final_db_path: str, filename: str = "character_appearances.txt"):
    conn = sqlite3.connect(final_db_path)
    cur = conn.cursor()

    cur.execute("SELECT id, name FROM characters")
    characters = cur.fetchall()

    cur.execute("SELECT id, title FROM videos")
    videos = cur.fetchall()

    conn.close()

    results = []

    for char_id, name in characters:
        count = 0
        if name is None:
            continue
        name_lower = name.lower()

        for _, title in videos:
            if title and name_lower in title.lower():
                count += 1

        results.append((name, count))

    # Sort by number of appearances, descending
    results.sort(key=lambda x: x[1], reverse=True)

    # Write results to file
    with open(filename, "w", encoding="utf-8") as f:
        f.write("=== Character Appearances in YouTube Video Titles ===\n\n")
        for name, count in results:
            f.write(f"{name}: {count} video title matches\n")

    print(f"Saved title-appearance results to {filename}")

# -------------------- Return Calc to TXT files --------------------

def export_calculations_to_txt(final_db_path: str, output_file: str = "hp_stats.txt"):
    """
    Runs both HP + YT calculations and writes results to a .txt file.
    """
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("--- Character Popularity (Mentions + Views) ---\n\n")
        conn = sqlite3.connect(final_db_path)
        cur = conn.cursor()
        cur.execute("""
            SELECT 
                characters.name,
                COUNT(character_mentions.video_id) AS mention_count,
                SUM(video_stats.view_count) AS total_views
            FROM characters
            LEFT JOIN character_mentions
                ON characters.id = character_mentions.character_ref
            LEFT JOIN videos
                ON character_mentions.video_id = videos.id
            LEFT JOIN video_stats
                ON videos.id = video_stats.video_ref
            GROUP BY characters.id
            ORDER BY mention_count DESC
        """)
        for name, mentions, views in cur.fetchall():
            f.write(f"{name} — mentions: {mentions or 0}, views: {views or 0}\n")

        f.write("\n\n--- Character Appearances in YouTube Titles ---\n\n")
        conn.close()

        results = calc_character_appearances_in_yt_videotitle(final_db_path)
        for name, count in results:
            f.write(f"{name}: {count}\n")

    print(f"\nExport complete → {output_file}\n")






# ------------------ Main script execution ------------------

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