import os 
import re 
import sqlite3
import requests 
import argparse 
import json 

db_default = "hp_db.db" 
max_default = 25 
hp_api_url = "https://hp-api.onrender.com/api/characters"

#first function that does the tables in database for harry potter, conn is the connection to sq database, table 1 is the one that stores hp characters and all the columns about them (name, age, house, etc) table 2 will be for JOINS that will work with youtube data but reference first table integer key 
# def init_db(conn): 
#     cur = conn.cursor() 
#     cur.execute("""CREATE TABLE IF NOT EXISTS characters(id INTEGER PRIMARY KEY AUTOINCREMENT, 
#                 name TEXT UNIQUE, 
#                 house TEXT,
#                 species TEXT, 
#                 role TEXT,
#                 patronus TEXT,
#                 gender TEXT, 
#                 age INTEGER, 
#                 alternate_names TEXT)
#                 """)
#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS character_mentions(
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         character_ref INTEGER, 
#         video_id TEXT,
#         mention_count INTEGER,
#         FOREIGN KEY (character_ref) REFERENCES characters(id)
#     )
# """)
#     conn.commit() 

def init_db(conn):
    cur = conn.cursor()

    # Lookup tables (ALL strings stored ONCE)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS houses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS genders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS species (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS roles (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS patronuses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE
        );
    """)

    # Characters table (ONLY integer references)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS characters (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            house_id INTEGER,
            gender_id INTEGER,
            species_id INTEGER,
            role_id INTEGER,
            patronus_id INTEGER,
            age INTEGER,
            alternate_names TEXT,
            FOREIGN KEY (house_id) REFERENCES houses(id),
            FOREIGN KEY (gender_id) REFERENCES genders(id),
            FOREIGN KEY (species_id) REFERENCES species(id),
            FOREIGN KEY (role_id) REFERENCES roles(id),
            FOREIGN KEY (patronus_id) REFERENCES patronuses(id)
        );
    """)

    # Join table for YouTube integration
    cur.execute("""
        CREATE TABLE IF NOT EXISTS character_mentions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            character_ref INTEGER,
            video_id TEXT,
            mention_count INTEGER,
            FOREIGN KEY (character_ref) REFERENCES characters(id)
        );
    """)

    conn.commit()

#second function gets characters from api, gets full list of characters and turns them into python list 
# def get_hp_char(): 
#     response = requests.get(hp_api_url)
#     response.raise_for_status() #lifeline stopping the program bc this is probably full of errors DO NOT TOUCH
#     return response.json() 
def get_hp_char():
    response = requests.get(hp_api_url)
    response.raise_for_status()
    return response.json()

def get_or_create_id(cur, table, value):
    """
    Ensures a categorical string is stored ONCE
    and returns its integer ID.
    """
    if not value:
        return None

    cur.execute(
        f"INSERT OR IGNORE INTO {table} (name) VALUES (?)",
        (value,)
    )
    cur.execute(
        f"SELECT id FROM {table} WHERE name = ?",
        (value,)
    )
    return cur.fetchone()[0]

#third function that stores 25 runs (characters in our case! I hope you're reading this Emily I feel like this is a niche way of communicating)--> basically this stuff gathers the characters and stores max 25 into database whenever run, figuring out this function was actually so difficult lol 
# def gather_store_hp(db_file, max_per_run = 25): 
#     if max_per_run < 1 or max_per_run > 25:
#         max_per_run = 25 
#     conn = sqlite3.connect(db_file)
#     init_db(conn)
#     cur = conn.cursor() 
#     all_chars = get_hp_char() 
#     inserted_rows = 0 
#     for char in all_chars: 
#         if inserted_rows >= max_per_run: 
#             break
#         name = char.get("name", "").strip() 
#         if name == "": 
#             continue 
#         cur.execute("SELECT id FROM characters WHERE name = ?", (name,))
#         char_if_alr_present = cur.fetchone()
#         if char_if_alr_present: 
#             continue
#         house = char.get("house", "")
#         species = char.get("species", "")
#         patronus = char.get("patronus", "")
#         gender = char.get("gender", "")
#         role = "student" if char.get("hogwartsStudent") else ("staff" if char.get("hogwartsStaff") else "none") 
#         age = char.get("yearOfBirth")
#         alt_names = json.dumps(char.get("alternate_names", []))
#         cur.execute(""" INSERT INTO characters(
#                     name, house, species, role, patronus, gender, age, alternate_names) VALUES(?,?,?,?,?,?,?,?)""", 
#                     (name, house, species, role, patronus, gender, age, alt_names))
#         inserted_rows += 1 
#         conn.commit() 
#     conn.close() 
#     print(f"Added {inserted_rows} new characters to the database.")
#     print("Run the file again to add 25 more until you reach 100.")

# if __name__ == "__main__":
#     gather_store_hp("hp_db.db", 25)

# HELPER: NORMALIZE A STRING VALUE
# --------------------------------------------------
# def get_or_create_id(cur, table, value):
#     if not value:
#         return None
#     cur.execute(
#         f"INSERT OR IGNORE INTO {table} (name) VALUES (?)",
#         (value,)
#     )
#     cur.execute(
#         f"SELECT id FROM {table} WHERE name = ?",
#         (value,)
#     )
#     return cur.fetchone()[0]


# --------------------------------------------------
# STORE DATA (25 PER RUN, FULLY NORMALIZED)
# --------------------------------------------------
def gather_store_hp(db_name, max_per_run=25):

    if max_per_run < 1 or max_per_run > 25:
        max_per_run = 25

    # Force DB to live next to this script
    db_path = os.path.join(os.path.dirname(__file__), db_name)

    conn = sqlite3.connect(db_path)
    init_db(conn)
    cur = conn.cursor()

    all_chars = get_hp_char()
    inserted_rows = 0

    for char in all_chars:
        if inserted_rows >= max_per_run:
            break

        # -------------------------------
        # CHARACTER UNIQUENESS
        # -------------------------------
        name = char.get("name", "").strip()
        if not name:
            continue

        cur.execute("SELECT id FROM characters WHERE name = ?", (name,))
        if cur.fetchone():
            continue

        # -------------------------------
        # NORMALIZED CATEGORICAL FIELDS
        # -------------------------------
        house_id = get_or_create_id(
            cur, "houses", char.get("house", "").strip()
        )

        gender_id = get_or_create_id(
            cur, "genders", char.get("gender", "").strip()
        )

        species_id = get_or_create_id(
            cur, "species", char.get("species", "").strip()
        )

        role_value = (
            "student" if char.get("hogwartsStudent")
            else "staff" if char.get("hogwartsStaff")
            else "none"
        )
        role_id = get_or_create_id(cur, "roles", role_value)

        patronus_value = char.get("patronus", "").strip()
        patronus_id = get_or_create_id(cur, "patronuses", patronus_value)

        # -------------------------------
        # OTHER FIELDS
        # -------------------------------
        age = char.get("yearOfBirth")
        alternate_names = json.dumps(char.get("alternate_names", []))

        # -------------------------------
        # INSERT CHARACTER
        # -------------------------------
        cur.execute("""
            INSERT INTO characters (
                name, house_id, gender_id, species_id,
                role_id, patronus_id, age, alternate_names
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            name, house_id, gender_id, species_id,
            role_id, patronus_id, age, alternate_names
        ))

        inserted_rows += 1
        conn.commit()

    conn.close()
    print(f"Added {inserted_rows} new characters to the database.")
    print("Run the file again to add 25 more until you reach 100.")

if __name__ == "__main__":
    gather_store_hp(db_default, max_default)