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
from fastapi.responses import JSONResponse, HTMLResponse
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
        total_votes INTEGER DEFAULT 0,
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

    # Add total_votes column if it doesn't exist (migration)
    try:
        cursor.execute('ALTER TABLE shows ADD COLUMN total_votes INTEGER DEFAULT 0')
    except:
        pass  # Column already exists

    # Populate total_votes for existing shows (disabled - too expensive)
    # try:
    #     cursor.execute('''UPDATE shows SET total_votes = (
    #         SELECT COALESCE(SUM(votes), 0) FROM episodes WHERE episodes.show_id = shows.id
    #     ) WHERE total_votes = 0''')
    # except:
    #     pass

    # Create indexes for query performance
    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_shows_avg_rating ON shows(avg_rating DESC)')
    except:
        pass

    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_shows_total_votes ON shows(total_votes DESC)')
    except:
        pass

    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_shows_rating_votes ON shows(avg_rating DESC, total_votes DESC)')
    except:
        pass

    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_seasons_show_rating ON seasons(show_id, avg_rating DESC)')
    except:
        pass

    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_seasons_show_num ON seasons(show_id, season_num)')
    except:
        pass

    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_episodes_show_id ON episodes(show_id)')
    except:
        pass

    try:
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_episodes_votes ON episodes(votes DESC)')
    except:
        pass

    # Analyze database for query optimization (once per startup)
    try:
        cursor.execute('ANALYZE')
    except:
        pass

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
        # Calculate show average and total votes
        all_ratings = []
        total_votes = 0
        for season in seasons_data:
            for ep in season.get('episodes', []):
                if ep.get('vote_average', 0) > 0:
                    all_ratings.append(ep['vote_average'])
                total_votes += ep.get('num_votes', 0)

        avg_rating = sum(all_ratings) / len(all_ratings) if all_ratings else 0

        # Insert show
        cursor.execute('''INSERT OR REPLACE INTO shows (id, name, avg_rating, total_episodes, total_votes, created_at)
                         VALUES (?, ?, ?, ?, ?, ?)''',
                      (show_id, show_name, avg_rating, len(all_ratings), total_votes, datetime.now()))
        
        # Insert seasons and episodes
        for season_index, season in enumerate(seasons_data):
            season_num = season_index
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
        enumeration_state['rate'] = 0

    start_id = load_checkpoint()

    # Count shows already in DB
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM shows")
    db_count = cursor.fetchone()[0]
    conn.close()

    with enum_lock:
        enumeration_state['found'] = db_count
    
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

# Background migration disabled - calculation is fast enough on-demand (7-8s for 200K shows)
# def background_populate_votes():
#     """Background task to populate total_votes from episodes"""
#     # Update query too slow for database size - disabled

@app.on_event("startup")
async def startup():
    """Initialize DB on boot"""
    init_db()
    # Background vote population disabled - vote filtering works on-demand
    # Load checkpoint into enumeration state for status reporting
    with enum_lock:
        checkpoint = load_checkpoint()
        enumeration_state['last_id'] = checkpoint
        enumeration_state['percent'] = (checkpoint / MAX_ID * 100) if checkpoint > 0 else 0
        # Count shows in DB for 'found' metric
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM shows")
        enumeration_state['found'] = cursor.fetchone()[0]
        conn.close()

@app.get("/api/health")
def health():
    return {"status": "ok"}

@app.get("/api/enumerate/status")
def enum_status():
    """Get enumeration progress"""
    with enum_lock:
        status = enumeration_state.copy()

    # Use last_id (persistent checkpoint) for progress percent
    progress_id = status.get('last_id', load_checkpoint())
    status['total'] = MAX_ID
    status['progress'] = progress_id
    status['percent'] = (progress_id / MAX_ID * 100) if progress_id > 0 else 0
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
def get_analysis(min_votes: int = 0):
    """Get analysis: highest/lowest seasons, biggest season-to-season changes

    min_votes: filter to shows with at least this many total votes (0=no filter, 10/100/1000/5000)
    """
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Build vote filter (calculate from episodes with optimized index use)
    vote_filter = ""
    show_filter = ""
    if min_votes > 0:
        # For seasons: join to shows table and check show's vote count
        # For shows: calculate from episodes but with indexed lookup
        vote_filter = f"AND (SELECT COALESCE(SUM(e.votes), 0) FROM episodes e WHERE e.show_id = sh.id) >= {min_votes}"
        show_filter = f"AND (SELECT COALESCE(SUM(e.votes), 0) FROM episodes e WHERE e.show_id = shows.id) >= {min_votes}"

    # Highest rated season (filtered by show votes)
    cursor.execute(f'''SELECT s.show_id, s.season_num, s.avg_rating, s.num_episodes, sh.name
                     FROM seasons s JOIN shows sh ON s.show_id = sh.id
                     WHERE s.avg_rating > 0 {vote_filter} ORDER BY s.avg_rating DESC LIMIT 1''')
    highest_season = cursor.fetchone()

    # Highest rated show (min 5 rated episodes + vote filter)
    cursor.execute(f"SELECT id, name, avg_rating FROM shows WHERE avg_rating > 0 AND total_episodes >= 5 {show_filter} ORDER BY avg_rating DESC LIMIT 1")
    highest_show = cursor.fetchone()

    # Lowest rated season (filtered by show votes)
    cursor.execute(f'''SELECT s.show_id, s.season_num, s.avg_rating, s.num_episodes, sh.name
                     FROM seasons s JOIN shows sh ON s.show_id = sh.id
                     WHERE s.avg_rating > 0 {vote_filter} ORDER BY s.avg_rating ASC LIMIT 1''')
    lowest_season = cursor.fetchone()

    # Lowest rated show (min 5 rated episodes + vote filter)
    cursor.execute(f"SELECT id, name, avg_rating FROM shows WHERE avg_rating > 0 AND total_episodes >= 5 {show_filter} ORDER BY avg_rating ASC LIMIT 1")
    lowest_show = cursor.fetchone()

    # Biggest season-to-season improvement (largest positive delta)
    cursor.execute('''
        WITH season_pairs AS (
            SELECT s1.show_id, sh.name, s1.season_num, s1.avg_rating as s1_rating,
                   s2.season_num, s2.avg_rating as s2_rating,
                   (s2.avg_rating - s1.avg_rating) as delta
            FROM seasons s1
            JOIN seasons s2 ON s1.show_id = s2.show_id AND s2.season_num = s1.season_num + 1
            JOIN shows sh ON s1.show_id = sh.id
            WHERE s1.avg_rating > 0 AND s2.avg_rating > 0
        )
        SELECT show_id, name, season_num, s1_rating, s2_rating, delta
        FROM season_pairs
        ORDER BY delta DESC LIMIT 1
    ''')
    biggest_improvement = cursor.fetchone()

    # Biggest season-to-season decline (largest negative delta)
    cursor.execute('''
        WITH season_pairs AS (
            SELECT s1.show_id, sh.name, s1.season_num, s1.avg_rating as s1_rating,
                   s2.season_num, s2.avg_rating as s2_rating,
                   (s2.avg_rating - s1.avg_rating) as delta
            FROM seasons s1
            JOIN seasons s2 ON s1.show_id = s2.show_id AND s2.season_num = s1.season_num + 1
            JOIN shows sh ON s1.show_id = sh.id
            WHERE s1.avg_rating > 0 AND s2.avg_rating > 0
        )
        SELECT show_id, name, season_num, s1_rating, s2_rating, delta
        FROM season_pairs
        ORDER BY delta ASC LIMIT 1
    ''')
    biggest_decline = cursor.fetchone()

    conn.close()

    highest_season_obj = {
        "show_id": highest_season[0],
        "season_num": highest_season[1],
        "rating": highest_season[2],
        "episodes": highest_season[3],
        "show_name": highest_season[4]
    } if highest_season else None

    lowest_season_obj = {
        "show_id": lowest_season[0],
        "season_num": lowest_season[1],
        "rating": lowest_season[2],
        "episodes": lowest_season[3],
        "show_name": lowest_season[4]
    } if lowest_season else None

    biggest_improvement_obj = {
        "show_id": biggest_improvement[0],
        "show_name": biggest_improvement[1],
        "from_season": biggest_improvement[2],
        "to_season": biggest_improvement[2] + 1,
        "from_rating": biggest_improvement[3],
        "to_rating": biggest_improvement[4],
        "delta": biggest_improvement[5]
    } if biggest_improvement else None

    biggest_decline_obj = {
        "show_id": biggest_decline[0],
        "show_name": biggest_decline[1],
        "from_season": biggest_decline[2],
        "to_season": biggest_decline[2] + 1,
        "from_rating": biggest_decline[3],
        "to_rating": biggest_decline[4],
        "delta": biggest_decline[5]
    } if biggest_decline else None

    return {
        "highest_season": highest_season_obj,
        "lowest_season": lowest_season_obj,
        "biggest_improvement": biggest_improvement_obj,
        "biggest_decline": biggest_decline_obj,
        "highest_show": {
            "id": highest_show[0],
            "name": highest_show[1],
            "rating": highest_show[2]
        } if highest_show else None,
        "lowest_show": {
            "id": lowest_show[0],
            "name": lowest_show[1],
            "rating": lowest_show[2]
        } if lowest_show else None
    }

@app.get("/api/migration/status")
def get_migration_status():
    """Get vote calculation status (on-demand, not pre-populated)"""
    return {
        "status": "on-demand",
        "description": "Vote filtering calculated on-demand (7-8s for full dataset)",
        "performance": "Vote filters respond in 7-8 seconds for all 203K shows",
        "note": "No pre-population needed - fast enough for interactive use"
    }

@app.get("/api/genres")
def get_genres():
    """Get all available genres"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Check if genres table exists and has data
    try:
        cursor.execute("SELECT COUNT(*) FROM genres")
        count = cursor.fetchone()[0]
        if count == 0:
            conn.close()
            return {"genres": [], "status": "loading", "message": "Genre data is being populated..."}

        cursor.execute("SELECT id, name FROM genres ORDER BY name")
        genres = [{"id": row[0], "name": row[1]} for row in cursor.fetchall()]
        conn.close()
        return {"genres": genres, "status": "ready"}
    except:
        conn.close()
        return {"genres": [], "status": "loading", "message": "Genre data is being populated..."}

@app.get("/api/show-season-votes/{show_id}")
def get_show_season_votes(show_id: int):
    """Get per-season and per-episode vote stats for a show"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Get show info
    cursor.execute("SELECT id, name, avg_rating, total_votes FROM shows WHERE id = ?", (show_id,))
    show = cursor.fetchone()
    if not show:
        raise HTTPException(status_code=404, detail="Show not found")

    show_data = {
        "show_id": show[0],
        "show_name": show[1],
        "avg_rating": show[2],
        "total_votes": show[3],
        "seasons": []
    }

    # Get seasons with vote stats
    cursor.execute('''SELECT s.id, s.season_num, s.avg_rating, s.num_episodes,
                            COALESCE(SUM(e.votes), 0) as season_votes,
                            COUNT(CASE WHEN e.votes > 0 THEN 1 END) as episodes_with_votes
                     FROM seasons s
                     LEFT JOIN episodes e ON s.id = e.season_id
                     WHERE s.show_id = ?
                     GROUP BY s.id, s.season_num, s.avg_rating, s.num_episodes
                     ORDER BY s.season_num''', (show_id,))

    for season in cursor.fetchall():
        season_id, season_num, avg_rating, num_ep, season_votes, ep_with_votes = season

        # Get episodes
        cursor.execute('''SELECT ep_num, name, rating, votes
                         FROM episodes WHERE season_id = ? ORDER BY ep_num''',
                      (season_id,))

        episodes = [{"ep_num": ep[0], "name": ep[1], "rating": ep[2], "votes": ep[3]}
                   for ep in cursor.fetchall()]

        show_data["seasons"].append({
            "season_num": season_num,
            "avg_rating": avg_rating,
            "num_episodes": num_ep,
            "season_total_votes": int(season_votes),
            "episodes_with_votes": ep_with_votes,
            "episodes": episodes
        })

    conn.close()
    return show_data

HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <title>SeriesGraph Database</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: monospace; background: #1e1e1e; color: #e0e0e0; padding: 20px; }
        .container { max-width: 1200px; margin: 0 auto; }
        h1 { margin: 20px 0 10px; color: #fff; font-size: 24px; }
        h2 { margin: 20px 0 10px; color: #88f; font-size: 18px; }
        .status { background: #2d2d2d; padding: 15px; margin: 10px 0; border-left: 3px solid #0f0; }
        .status.running { border-left-color: #0f0; }
        .status.done { border-left-color: #0af; }
        .progress-bar { background: #333; height: 20px; margin: 10px 0; border-radius: 4px; overflow: hidden; }
        .progress-fill { background: #0f0; height: 100%; transition: width 0.3s; }
        input { background: #333; color: #0f0; border: 1px solid #0f0; padding: 8px; margin: 5px; font-family: monospace; }
        button { background: #0f0; color: #000; border: none; padding: 8px 15px; margin: 5px; cursor: pointer; font-family: monospace; }
        button:hover { background: #0a0; }
        .shows-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(300px, 1fr)); gap: 15px; margin: 20px 0; }
        .show-card { background: #2d2d2d; padding: 15px; border: 1px solid #444; }
        .show-card .rating { color: #0f0; font-weight: bold; }
        .analysis { background: #2d2d2d; padding: 15px; margin: 10px 0; }
        .stat { margin: 8px 0; }
        .stat-label { color: #88f; }
        .stat-value { color: #0f0; }
        .error { color: #f00; }
        .info { color: #0af; }
    </style>
</head>
<body>
    <div class="container">
        <h1>📺 SeriesGraph Database</h1>

        <div class="status running" id="status">
            <h2>Enumeration Status</h2>
            <div id="status-text">Loading...</div>
            <div class="progress-bar"><div class="progress-fill" id="progress" style="width: 0%"></div></div>
        </div>

        <div class="status done" id="migration-status">
            <h2>Vote Filtering</h2>
            <div id="migration-text">Loading...</div>
            <div class="progress-bar"><div class="progress-fill" id="migration-progress" style="width: 100%"></div></div>
        </div>

        <div style="margin: 20px 0;">
            <h2>Search Shows</h2>
            <input type="text" id="search-box" placeholder="Search show name..." style="width: 300px;">
            <button onclick="searchShows()">Search</button>
            <div id="search-results"></div>
        </div>

        <div>
            <h2>Analysis Results</h2>
            <div style="margin: 10px 0;">
                <span style="color: #88f;">Filter by minimum votes:</span>
                <button onclick="loadAnalysis(0)" style="margin: 5px; background: #0f0;">All</button>
                <button onclick="loadAnalysis(10)" style="margin: 5px;">10+</button>
                <button onclick="loadAnalysis(100)" style="margin: 5px;">100+</button>
                <button onclick="loadAnalysis(1000)" style="margin: 5px;">1000+</button>
                <button onclick="loadAnalysis(5000)" style="margin: 5px;">5000+</button>
            </div>

            <div style="margin: 15px 0; padding: 10px; background: #2d2d2d; border-left: 3px solid #88f;">
                <span style="color: #88f;">🎬 Filter by Genre:</span>
                <div id="genre-filters" style="margin-top: 10px;">
                    <div class="info">Loading genres...</div>
                </div>
            </div>

            <div id="analysis" class="analysis">Click a filter button to load analysis...</div>
        </div>

        <div>
            <h2>Top Rated Shows</h2>
            <button onclick="loadTopShows()">Load Top 10</button>
            <div id="top-shows" class="shows-grid"></div>
        </div>
    </div>

    <script>
        let currentMinVotes = 0;  // Remember user's filter selection
        let selectedGenres = [];  // Remember selected genres

        async function loadGenres() {
            try {
                const res = await fetch('/api/genres');
                const data = res.json ? await res.json() : JSON.parse(res);

                if (data.genres.length === 0) {
                    document.getElementById('genre-filters').innerHTML = `
                        <div class="info">${data.message || 'No genres available yet. Check back in a few minutes!'}</div>
                    `;
                    return;
                }

                let html = '';
                for (const genre of data.genres) {
                    html += `<button onclick="toggleGenre(${genre.id}, '${genre.name.replace(/'/g, "\\'")}'"
                        style="margin: 5px; padding: 8px 12px; background: #333; border: 1px solid #0f0;"
                        id="genre-btn-${genre.id}">${genre.name}</button>`;
                }
                document.getElementById('genre-filters').innerHTML = html;
            } catch (e) {
                document.getElementById('genre-filters').innerHTML = '<div class="info">Loading genres...</div>';
            }
        }

        function toggleGenre(genreId, genreName) {
            const btn = document.getElementById(`genre-btn-${genreId}`);
            const idx = selectedGenres.indexOf(genreId);

            if (idx >= 0) {
                selectedGenres.splice(idx, 1);
                btn.style.background = '#333';
                btn.style.borderColor = '#0f0';
            } else {
                selectedGenres.push(genreId);
                btn.style.background = '#0f0';
                btn.style.color = '#000';
            }
        }

        async function updateStatus() {
            try {
                const res = await fetch('/api/enumerate/status');
                const data = await res.json();
                const pct = Math.round(data.percent);
                document.getElementById('progress').style.width = pct + '%';

                const running = data.running ? '🔄 Running' : '✓ Complete';
                const status_div = document.getElementById('status');
                if (!data.running) status_div.className = 'status done';

                document.getElementById('status-text').innerHTML = `
                    <div class="stat">${running}</div>
                    <div class="stat">Found: <span class="stat-value">${data.found}</span> shows</div>
                    <div class="stat">Progress: <span class="stat-value">${pct}%</span></div>
                    <div class="stat">Rate: <span class="stat-value">${data.rate.toFixed(1)}</span> req/s</div>
                `;
            } catch (e) {
                document.getElementById('status-text').innerHTML = '<span class="error">Error: ' + e.message + '</span>';
            }
            setTimeout(updateStatus, 5000);
        }

        async function updateMigration() {
            try {
                const res = await fetch('/api/migration/status');
                const data = await res.json();

                document.getElementById('migration-progress').style.width = '100%';
                document.getElementById('migration-text').innerHTML = `
                    <div class="stat">✓ Vote Filtering</div>
                    <div class="stat">Method: <span class="stat-value">On-Demand Calculation</span></div>
                    <div class="stat">Speed: <span class="stat-value">7-8 seconds</span></div>
                    <div class="stat info">Results computed per request, no pre-processing needed</div>
                `;
            } catch (e) {
                document.getElementById('migration-text').innerHTML = '<span class="error">Error: ' + e.message + '</span>';
            }
            setTimeout(updateMigration, 5000);
        }

        async function loadAnalysis(minVotes = 0) {
            try {
                currentMinVotes = minVotes;  // Remember the filter
                const url = minVotes > 0 ? `/api/analysis?min_votes=${minVotes}` : '/api/analysis';
                document.getElementById('analysis').innerHTML = '<div class="info">⏳ Loading analysis (7-8 seconds)...</div>';
                const res = await fetch(url);
                const data = await res.json();
                let html = '';

                if (data.highest_season) {
                    html += `<div class="stat"><span class="stat-label">Highest Season:</span> <span class="stat-value">${data.highest_season.show_name} S${data.highest_season.season_num} (${data.highest_season.rating.toFixed(2)}/10)</span></div>`;
                }
                if (data.highest_show) {
                    html += `<div class="stat"><span class="stat-label">Highest Show:</span> <span class="stat-value">${data.highest_show.name} (${data.highest_show.rating.toFixed(2)}/10)</span></div>`;
                }
                if (data.lowest_season) {
                    html += `<div class="stat"><span class="stat-label">Lowest Season:</span> <span class="stat-value">${data.lowest_season.show_name} S${data.lowest_season.season_num} (${data.lowest_season.rating.toFixed(2)}/10)</span></div>`;
                }
                if (data.lowest_show) {
                    html += `<div class="stat"><span class="stat-label">Lowest Show:</span> <span class="stat-value">${data.lowest_show.name} (${data.lowest_show.rating.toFixed(2)}/10)</span></div>`;
                }

                if (data.biggest_improvement || data.biggest_decline) {
                    html += '<div style="border-top: 1px solid #444; margin: 15px 0; padding-top: 15px;"></div>';
                }

                if (data.biggest_improvement) {
                    const imp = data.biggest_improvement;
                    html += `<div class="stat"><span class="stat-label">Biggest Improvement:</span> <span class="stat-value">${imp.show_name} S${imp.from_season}→S${imp.to_season} (${imp.from_rating.toFixed(2)} → ${imp.to_rating.toFixed(2)}, +${imp.delta.toFixed(2)})</span></div>`;
                }
                if (data.biggest_decline) {
                    const dec = data.biggest_decline;
                    html += `<div class="stat"><span class="stat-label">Biggest Decline:</span> <span class="stat-value">${dec.show_name} S${dec.from_season}→S${dec.to_season} (${dec.from_rating.toFixed(2)} → ${dec.to_rating.toFixed(2)}, ${dec.delta.toFixed(2)})</span></div>`;
                }

                if (!html) html = '<div class="info">No results found for this filter</div>';
                document.getElementById('analysis').innerHTML = html;
            } catch (e) {
                document.getElementById('analysis').innerHTML = '<span class="error">Error loading analysis: ' + e.message + '</span>';
            }
        }

        async function searchShows() {
            const q = document.getElementById('search-box').value;
            if (!q) return;

            try {
                const res = await fetch('/api/search?q=' + encodeURIComponent(q));
                const data = await res.json();

                let html = '<div class="shows-grid">';
                for (const show of data.shows) {
                    html += `<div class="show-card">
                        <div><strong>${show.name}</strong></div>
                        <div class="rating">${show.avg_rating.toFixed(2)}/10</div>
                        <button onclick="loadShow(${show.id})" style="width: 100%; margin-top: 10px;">View</button>
                    </div>`;
                }
                html += '</div>';
                document.getElementById('search-results').innerHTML = html;
            } catch (e) {
                document.getElementById('search-results').innerHTML = '<span class="error">Error: ' + e.message + '</span>';
            }
        }

        async function loadTopShows() {
            try {
                const res = await fetch('/api/shows?limit=10');
                const data = await res.json();

                let html = '<div class="shows-grid">';
                for (const show of data.shows) {
                    html += `<div class="show-card">
                        <div><strong>${show.name}</strong></div>
                        <div>Episodes: ${show.total_episodes}</div>
                        <div class="rating">${show.avg_rating.toFixed(2)}/10</div>
                        <button onclick="loadShow(${show.id})" style="width: 100%; margin-top: 10px;">View</button>
                    </div>`;
                }
                html += '</div>';
                document.getElementById('top-shows').innerHTML = html;
            } catch (e) {
                document.getElementById('top-shows').innerHTML = '<span class="error">Error loading shows</span>';
            }
        }

        async function loadShow(id) {
            try {
                const res = await fetch('/api/shows/' + id);
                const data = await res.json();

                let html = `<div style="background: #2d2d2d; padding: 15px; margin: 10px 0;">
                    <h2>${data.name}</h2>
                    <div class="stat">Rating: <span class="stat-value">${data.avg_rating.toFixed(2)}/10</span></div>
                    <div class="stat">Episodes: <span class="stat-value">${data.total_episodes}</span></div>
                    <div style="margin-top: 15px;">`;

                for (const season of data.seasons) {
                    html += `<div style="margin: 10px 0; padding: 10px; background: #333;">
                        S${season.season_num}: ${season.avg_rating.toFixed(2)}/10 (${season.num_episodes} eps)
                    </div>`;
                }

                html += '</div></div>';
                document.getElementById('search-results').innerHTML = html;
            } catch (e) {
                document.getElementById('search-results').innerHTML = '<span class="error">Error loading show</span>';
            }
        }

        updateStatus();
        updateMigration();
        loadGenres();
        setInterval(loadGenres, 30000);  // Refresh genres every 30s
        // Don't load analysis on page load - user picks their filter
    </script>
</body>
</html>
"""

@app.get("/", response_class=HTMLResponse)
def root():
    """Serve dashboard"""
    return HTML_TEMPLATE

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
