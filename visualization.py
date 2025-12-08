import matplotlib.pyplot as plt
import sqlite3
import numpy as np


BAR_COLORS = [
    "red", "blue", "green", "purple", "orange",
    "pink", "cyan", "brown", "yellow", "gray"
]
def plot_character_popularity_pie(db_path="combined.db"):
    conn = sqlite3.connect(db_path)
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
        name = row[0]
        total_views = row[1]

        if total_views is None:
            continue
        if total_views < 1:
            continue

        names.append(name)
        views.append(total_views)

    colors_used = []
    for i in range(len(names)):
        colors_used.append(BAR_COLORS[i % len(BAR_COLORS)])
    plt.figure(figsize=(10, 10))
    plt.pie(views, labels=names, colors=colors_used, autopct="%1.1f%%", startangle=140)
    plt.title("Harry Potter Character Popularity (% of Total Views)")

    legend_labels = []
    for i in range(len(names)):
        legend_labels.append(f"{names[i]} ({colors_used[i]})")

    plt.legend(legend_labels, title="Legend", loc="upper right", bbox_to_anchor=(1.2, 1))
    plt.tight_layout()
    plt.show()



def plot_character_title_mentions_bar(db_path="combined.db"):
    conn = sqlite3.connect(db_path)
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
    counts = []

    for row in rows:
        name = row[0]
        count = row[1]

        if count is None or count < 1:
            continue

        names.append(name)
        counts.append(count)
    colors_used = []
    for i in range(len(names)):
        colors_used.append(BAR_COLORS[i % len(BAR_COLORS)])
    plt.figure(figsize=(12, 6))
    plt.bar(names, counts, color=colors_used)

    plt.ylabel("Mentions in Video Titles")
    plt.title("HP Characters Mentioned in YouTube Titles")

    max_val = max(counts)
    step = 1
    plt.yticks(np.arange(0, max_val + step, step))

    plt.xticks(rotation=75, ha='right')
    plt.tight_layout()

    legend_labels = []
    for i in range(len(names)):
        legend_labels.append(f"{names[i]} ({colors_used[i]})")

    plt.legend(legend_labels, title="Legend", bbox_to_anchor=(1.03, 1), loc="upper left")
    plt.show()

plot_character_popularity_pie("combined.db")
plot_character_title_mentions_bar("combined.db")
