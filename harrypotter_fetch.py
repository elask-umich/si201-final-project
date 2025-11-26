import os 
import re 
import sqlite3
import requests 
import argparse 
import json 
import time 

db_default = "hp_data.db" 
max_default = 25 
hp_api_url = "https://hp-api.onrender.com/api/characters"

#first function that does the tables in database for harry potter, conn is the connection to sq database, table 1 is the one that stores hp characters and all the columns about them (name, age, house, etc) table 2 will be for JOINS that will work with youtube data but reference first table integer key 
def init_db(conn): 
    cur = conn.cursor() 
    cur.execute("""CREATE TABLE IF NOT EXISTS characters(id INTEGER PRIMARY KEY AUTOINCREMENT, 
                name TEXT UNIQUE, 
                house TEXT,
                species TEXT, 
                role TEXT,
                patronus TEXT,
                gender TEXT, 
                age INTEGER, 
                next_index INTEGER)
                """)
    cur.execute("""CREATE TABLE IF NOT EXISTS character_mentions(
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                character_ref INTEGER, 
                video_id TEXT,
                mention_count INTEGER, 
                FOREIGN KEY (character_ref) REFERENCES chracters(id))"""
                )
    conn.commit() 
#second function gets characters from api, gets full list of characters and turns them into python list 
    def get_hp_char(): 
        response = requests.get(hp_api_url)
        response.raise_for_status() #lifeline stopping the program bc this is probably full of errors DO NOT TOUCH
        return response.json() 





