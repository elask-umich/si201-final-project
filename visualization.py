#visualization code for both of us using matplotlib 
import matplotlib.pyplot as plt
import sqlite3

def plot_character_popularity(final_db_path: str):
    conn = sqlite3.connect(final_db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT 
            characters.name,
            SUM(video_stats.view_count) AS total_views
        FROM characters
        LEFT JOIN character_mentions
            ON characters.id = character_mentions.character_ref
        LEFT JOIN videos
            ON character_mentions.video_id = videos.id
        LEFT JOIN video_stats
            ON videos.id = video_stats.video_ref
        GROUP BY characters.id
        ORDER BY total_views DESC
    """)

    rows = cur.fetchall()
    conn.close()
    names = []
    views = []
    for row in rows:
        names.append(row[0])
        if row[1] is None:
            views.append(0)
        else:
            views.append(row[1])
    plt.figure(figsize=(12,6))
    plt.bar(names, views)
    plt.ylabel("Total Views Across YouTube")
    plt.title("Harry Potter Character Popularity (Total YouTube Views)")
    plt.xticks(rotation=75, ha='right')
    plt.tight_layout()
    plt.show()
def plot_character_title_mentions(final_db_path: str):
    conn = sqlite3.connect(final_db_path)
    cur = conn.cursor()

    cur.execute("""
        SELECT
            characters.name,
            COUNT(character_mentions.video_id) AS mention_count
        FROM characters
        LEFT JOIN character_mentions
            ON characters.id = character_mentions.character_ref
        GROUP BY characters.id
        ORDER BY mention_count DESC
    """)
    rows = cur.fetchall()
    conn.close()
    names = []
    mentions = []
    for row in rows:
        names.append(row[0])
        if row[1] is None:
            mentions.append(0)
        else:
            mentions.append(row[1])
    plt.figure(figsize=(12,6))
    plt.bar(names, mentions)
    plt.ylabel("Total Mentions in Video Titles")
    plt.title("Harry Potter Character Frequency in YouTube Titles")
    plt.xticks(rotation=75, ha='right')
    plt.tight_layout()
    plt.show()
    
plot_character_popularity("final.db")
plot_character_title_mentions("final.db")