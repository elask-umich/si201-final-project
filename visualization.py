import matplotlib.pyplot as plt
import sqlite3
import numpy as np


BAR_COLORS = [
    "red", "blue", "green", "purple", "orange",
    "pink", "cyan", "brown", "yellow", "gray"]

def pie_harry_vs_rest(db_path="combined.db"):
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
    
    harry_views = 0
    other_total = 0
    
    for name, views in rows:
        if views is None:
            continue
        if "harry potter" in name.lower():
            harry_views = views
        else:
            other_total += views
    
    labels = ["Harry Potter", "All Other Characters"]
    values = [harry_views, other_total]
    colors = ["red", "gray"]
    
    plt.figure(figsize=(7,7))
    plt.pie(values, labels=labels, autopct='%1.1f%%', colors=colors, startangle=140)
    plt.title("Harry Potter vs. All Others (Total Views)")
    plt.tight_layout()
    plt.show()



def pie_other_characters(db_path="combined.db"):
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
        WHERE characters.name NOT LIKE '%Harry Potter%'
        GROUP BY characters.id
        HAVING total_views >= 1
        ORDER BY total_views DESC
    """)
    
    rows = cur.fetchall()
    conn.close()
    
    names = []
    views = []
    
    for name, v in rows:
        names.append(name)
        views.append(v)
    

    colors = ['blue', 'green', 'purple', 'orange', 'yellow', 'pink',
              'cyan', 'brown', 'gray', 'lime']

    slice_colors = []
    idx = 0
    for _ in range(len(names)):
        slice_colors.append(colors[idx % len(colors)])
        idx += 1

    plt.figure(figsize=(9,9))
    plt.pie(
        views,
        labels=names,
        autopct='%1.1f%%',
        colors=slice_colors,
        startangle=140,
        pctdistance=0.8,     
        labeldistance=1.1    
    )
    plt.title("Popularity of Other HP Characters (Total Views)")
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
    mentions = []
    for row in rows:
        char_name = row[0]
        count = row[1]

        if count is not None and count >= 1:
            names.append(char_name)
            mentions.append(count)

    base_colors = ['red', 'blue', 'green', 'orange', 'purple', 'cyan',
                   'yellow', 'pink', 'brown', 'gray', 'olive', 'magenta']

    colors = []
    idx = 0
    for _ in names:
        colors.append(base_colors[idx])
        idx = idx + 1
        if idx == len(base_colors):
            idx = 0

    plt.figure(figsize=(14, 7))
    plt.bar(names, mentions, color=colors)

    plt.ylabel("Mentions in Video Titles")
    plt.title("Harry Potter Characters Mentioned in YouTube Titles (Distinct Colors)")

    if len(mentions) > 0:
        max_val = max(mentions)
    else:
        max_val = 0

    step = 10
    plt.yticks(np.arange(0, max_val + step, step))

    plt.xticks(rotation=75, ha='right')
    plt.tight_layout()
    plt.show()

pie_harry_vs_rest("combined.db")
pie_other_characters("combined.db")
plot_character_title_mentions_bar("combined.db")