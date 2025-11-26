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

#first function that does the tables in database for harry potter, conn is the connection to sq database 
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
    cur.execute()
    conn.commit() 



