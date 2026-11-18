# SeriesGraph Database Service

All-in-one service: SQLite database + FastAPI server + background enumerator

## Features

- 📊 **SQLite Database** - All SeriesGraph data stored locally
- 🧵 **4-thread Enumerator** - Fetches all show data in parallel
- 🔄 **Checkpoint Resume** - Continues from last position if interrupted
- 🚀 **FastAPI Server** - REST API for queries and analysis
- 🐳 **Docker Ready** - Single-command deployment

## Data Structure

```
shows: id, name, avg_rating, total_episodes
seasons: id, show_id, season_num, avg_rating, num_episodes
episodes: id, season_id, show_id, ep_num, name, rating, votes
```

## API Endpoints

- `GET /api/health` - Service health check
- `GET /api/shows` - List all shows (paginated)
- `GET /api/shows/{id}` - Get show details with all seasons/episodes
- `GET /api/search?q=breaking` - Search shows by name
- `GET /api/analysis` - Get all 7 analysis metrics
- `GET /api/enumerate/status` - Check enumeration progress
- `POST /api/enumerate/start` - Start enumeration manually

## Quick Start

```bash
# Start service (auto-enumerates if DB is empty)
docker-compose up -d

# Monitor progress
curl http://localhost:8000/api/enumerate/status

# Query data
curl http://localhost:8000/api/search?q=breaking

# Get analysis
curl http://localhost:8000/api/analysis
```

## How It Works

1. **Startup** → Checks if DB has data
2. **If empty** → Starts 4-thread enumerator automatically
3. **Enumerator** → Fetches all shows + seasons + episodes
4. **Checkpoint** → Saves progress every 2000 requests
5. **Resume** → On restart, continues from checkpoint
6. **API** → Serves data from local SQLite database

## Timeline

- **4 threads + checkpoint resume** → ~4-8 hours to complete
- **After completion** → Instant queries from local database
- **No more API hammering** → All data cached locally

## Development

```bash
# Install dependencies
pip install -r requirements.txt

# Run locally
python app.py

# API docs available at http://localhost:8000/docs
```

---

Built for efficient local data storage and analysis of the entire SeriesGraph database.
