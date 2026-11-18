"""
SeriesGraph Database Service
All-in-one: SQLite DB + FastAPI server + background enumerator
"""
import os
import sqlite3
import json
import time
import threading
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
import requests
from concurrent.futures import ThreadPoolExecutor, as_completed

# Database setup
DB_PATH = "data/seriesgraph.db"
CHECKPOINT_FILE = "data/enum_checkpoint.txt"
DATA_DIR = Path("data")
DATA_DIR.mkdir(exist_ok=True)

BASE_URL = "https://seriesgraph.com/api"
MAX_ID = 300000

app = FastAPI(title="SeriesGraph DB", version="1.0.0")

# Global state
enumeration_state = {
    'running': False,
    'progress': 0,
    'total': 0,
    'rate': 0,
    'last_id': 0,
    'found': 0,
    'start_time': None
}
enum_lock = threading.Lock()

def init_db():
    """Initialize SQLite database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS shows (
        id INTEGER PRIMARY KEY,
        name TEXT,
        avg_rating REAL,
        total_episodes INTEGER,
        created_at TIMESTAMP
    )''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS seasons (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        show_id INTEGER,
        season_num INTEGER,
        avg_rating REAL,
        num_episodes INTEGER,
        FOREIGN KEY (show_id) REFERENCES shows(id)
    )''')
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS episodes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        season_id INTEGER,
        show_id INTEGER,
        ep_num INTEGER,
        name TEXT,
        rating REAL,
        votes INTEGER,
        FOREIGN KEY (season_id) REFERENCES seasons(id),
        FOREIGN KEY (show_id) REFERENCES shows(id)
    )''')
    
    conn.commit()
    conn.close()

def load_checkpoint():
    """Load enumeration checkpoint"""
    if os.path.exists(CHECKPOINT_FILE):
        try:
            with open(CHECKPOINT_FILE, 'r') as f:
                return int(f.read().strip())
        except:
            return 1
    return 1

def save_checkpoint(show_id):
    """Save enumeration checkpoint"""
    try:
        with open(CHECKPOINT_FILE, 'w') as f:
            f.write(str(show_id))
    except:
        pass

def save_show_data(show_id, show_name, seasons_data):
    """Save show and all its season/episode data to DB"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    try:
        # Calculate show average
        all_ratings = []
        for season in seasons_data:
            for ep in season.get('episodes', []):
                if ep.get('vote_average', 0) > 0:
                    all_ratings.append(ep['vote_average'])
        
        avg_rating = sum(all_ratings) / len(all_ratings) if all_ratings else 0
        
        # Insert show
        cursor.execute('''INSERT OR REPLACE INTO shows (id, name, avg_rating, total_episodes, created_at)
                         VALUES (?, ?, ?, ?, ?)''',
                      (show_id, show_name, avg_rating, len(all_ratings), datetime.now()))
        
        # Insert seasons and episodes
        for season in seasons_data:
            season_num = season.get('season_number', 0)
            episodes = season.get('episodes', [])
            
            if not episodes:
                continue
            
            # Calculate season average
            season_ratings = [ep.get('vote_average', 0) for ep in episodes if ep.get('vote_average', 0) > 0]
            season_avg = sum(season_ratings) / len(season_ratings) if season_ratings else 0
            
            cursor.execute('''INSERT INTO seasons (show_id, season_num, avg_rating, num_episodes)
                            VALUES (?, ?, ?, ?)''',
                          (show_id, season_num, season_avg, len(episodes)))
            
            season_id = cursor.lastrowid
            
            # Insert episodes
            for ep in episodes:
                cursor.execute('''INSERT INTO episodes 
                                (season_id, show_id, ep_num, name, rating, votes)
                                VALUES (?, ?, ?, ?, ?, ?)''',
                              (season_id, show_id, ep.get('episode_number', 0),
                               ep.get('name', ''), ep.get('vote_average', 0),
                               ep.get('num_votes', 0)))
        
        conn.commit()
    finally:
        conn.close()

def fetch_show_data(show_id, thread_id):
    """Fetch show data and name"""
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    try:
        # Fetch season ratings
        url = f"{BASE_URL}/shows/{show_id}/season-ratings"
        response = session.get(url, timeout=10)
        
        if response.status_code != 200:
            return None
        
        seasons_data = response.json()
        if not isinstance(seasons_data, list):
            return None
        
        # Fetch show name
        show_name = f"Show {show_id}"
        try:
            page_url = f"https://seriesgraph.com/show/{show_id}"
            page_resp = session.get(page_url, timeout=10)
            if page_resp.status_code == 200:
                import re
                match = re.search(r'<title>([^-]*) ratings by episode', page_resp.text)
                if match:
                    show_name = match.group(1).strip()
        except:
            pass
        
        return (show_id, show_name, seasons_data)
    except:
        return None

def enumerator_worker(thread_id, start_id, num_threads):
    """Worker thread for enumeration"""
    session = requests.Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0'})
    
    for show_id in range(start_id + thread_id, MAX_ID + 1, num_threads):
        result = fetch_show_data(show_id, thread_id)
        
        if result:
            show_id, show_name, seasons_data = result
            save_show_data(show_id, show_name, seasons_data)
            
            with enum_lock:
                enumeration_state['found'] += 1
                enumeration_state['last_id'] = show_id
                
                if enumeration_state['start_time']:
                    elapsed = time.time() - enumeration_state['start_time']
                    enumeration_state['rate'] = enumeration_state['progress'] / elapsed if elapsed > 0 else 0
        
        with enum_lock:
            enumeration_state['progress'] += 1
        
        if enumeration_state['progress'] % 2000 == 0:
            save_checkpoint(show_id)
        
        time.sleep(0.1)  # Rate limiting per thread

def start_enumeration():
    """Start background enumeration (4 threads)"""
    with enum_lock:
        if enumeration_state['running']:
            return False
        
        enumeration_state['running'] = True
        enumeration_state['start_time'] = time.time()
        enumeration_state['progress'] = 0
        enumeration_state['found'] = 0
        enumeration_state['rate'] = 0
    
    start_id = load_checkpoint()
    
    def run_enum():
        try:
            with ThreadPoolExecutor(max_workers=4) as executor:
                futures = [executor.submit(enumerator_worker, i, start_id, 4) for i in range(4)]
                for future in as_completed(futures):
                    future.result()
        finally:
            with enum_lock:
                enumeration_state['running'] = False
            save_checkpoint(MAX_ID)
    
    thread = threading.Thread(target=run_enum, daemon=True)
    thread.start()

def db_has_data():
    """Check if database has show data"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM shows")
    count = cursor.fetchone()[0]
    conn.close()
    return count > 0

@app.on_event("startup")
async def startup():
    """Initialize DB and start enumeration if needed"""
    init_db()
    if not db_has_data():
        start_enumeration()

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.get("/api/enumerate/status")
def enum_status():
    """Get enumeration progress"""
    with enum_lock:
        status = enumeration_state.copy()
    
    status['total'] = MAX_ID
    status['percent'] = (status['progress'] / MAX_ID * 100) if status['progress'] > 0 else 0
    return status

@app.post("/api/enumerate/start")
def enum_start():
    """Start enumeration manually"""
    with enum_lock:
        if enumeration_state['running']:
            return {"status": "already running"}
    
    start_enumeration()
    return {"status": "started"}

@app.get("/api/shows")
def list_shows(limit: int = 100, offset: int = 0):
    """List all shows"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name, avg_rating, total_episodes FROM shows ORDER BY avg_rating DESC LIMIT ? OFFSET ?",
                  (limit, offset))
    shows = [dict(zip(['id', 'name', 'avg_rating', 'total_episodes'], row)) for row in cursor.fetchall()]
    
    cursor.execute("SELECT COUNT(*) FROM shows")
    total = cursor.fetchone()[0]
    
    conn.close()
    return {"shows": shows, "total": total}

@app.get("/api/shows/{show_id}")
def get_show(show_id: int):
    """Get show details with all seasons/episodes"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get show
    cursor.execute("SELECT id, name, avg_rating, total_episodes FROM shows WHERE id = ?", (show_id,))
    show_row = cursor.fetchone()
    if not show_row:
        raise HTTPException(status_code=404, detail="Show not found")
    
    show = dict(zip(['id', 'name', 'avg_rating', 'total_episodes'], show_row))
    
    # Get seasons
    cursor.execute("SELECT id, season_num, avg_rating, num_episodes FROM seasons WHERE show_id = ? ORDER BY season_num",
                  (show_id,))
    seasons = []
    for season_row in cursor.fetchall():
        season = dict(zip(['id', 'season_num', 'avg_rating', 'num_episodes'], season_row))
        season_id = season['id']
        
        # Get episodes
        cursor.execute("SELECT ep_num, name, rating, votes FROM episodes WHERE season_id = ? ORDER BY ep_num",
                      (season_id,))
        season['episodes'] = [dict(zip(['ep_num', 'name', 'rating', 'votes'], row)) for row in cursor.fetchall()]
        seasons.append(season)
    
    show['seasons'] = seasons
    conn.close()
    return show

@app.get("/api/search")
def search_shows(q: str):
    """Search shows by name"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT id, name, avg_rating FROM shows WHERE name LIKE ? ORDER BY avg_rating DESC LIMIT 50",
                  (f"%{q}%",))
    shows = [dict(zip(['id', 'name', 'avg_rating'], row)) for row in cursor.fetchall()]
    
    conn.close()
    return {"shows": shows}

@app.get("/api/analysis")
def get_analysis():
    """Get analysis: highest/lowest seasons, biggest changes"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Highest rated season
    cursor.execute('''SELECT s.show_id, s.season_num, s.avg_rating, s.num_episodes, sh.name
                     FROM seasons s JOIN shows sh ON s.show_id = sh.id
                     WHERE s.avg_rating > 0 ORDER BY s.avg_rating DESC LIMIT 1''')
    highest_season = cursor.fetchone()
    
    # Highest rated show
    cursor.execute("SELECT id, name, avg_rating FROM shows WHERE avg_rating > 0 ORDER BY avg_rating DESC LIMIT 1")
    highest_show = cursor.fetchone()
    
    # Lowest rated season
    cursor.execute('''SELECT s.show_id, s.season_num, s.avg_rating, s.num_episodes, sh.name
                     FROM seasons s JOIN shows sh ON s.show_id = sh.id
                     WHERE s.avg_rating > 0 ORDER BY s.avg_rating ASC LIMIT 1''')
    lowest_season = cursor.fetchone()
    
    # Lowest rated show
    cursor.execute("SELECT id, name, avg_rating FROM shows WHERE avg_rating > 0 ORDER BY avg_rating ASC LIMIT 1")
    lowest_show = cursor.fetchone()
    
    conn.close()
    
    return {
        "highest_season": {
            "show_id": highest_season[0],
            "season_num": highest_season[1],
            "rating": highest_season[2],
            "episodes": highest_season[3],
            "show_name": highest_season[4]
        } if highest_season else None,
        "highest_show": {
            "id": highest_show[0],
            "name": highest_show[1],
            "rating": highest_show[2]
        } if highest_show else None,
        "lowest_season": {
            "show_id": lowest_season[0],
            "season_num": lowest_season[1],
            "rating": lowest_season[2],
            "episodes": lowest_season[3],
            "show_name": lowest_season[4]
        } if lowest_season else None,
        "lowest_show": {
            "id": lowest_show[0],
            "name": lowest_show[1],
            "rating": lowest_show[2]
        } if lowest_show else None
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
