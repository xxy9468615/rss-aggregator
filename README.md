# RSS Aggregator

42+ RSS sources across 7 categories, auto-refresh every 10 minutes. Feeds are generated as static XML files and served via GitHub raw URLs.

## Public URL

Feed files are available at:
```
https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/<name>.xml
```

## Tech Stack

- Python 3.12 + FastAPI + uvicorn
- feedparser + httpx (concurrent fetching)
- Playwright headless Chromium (fallback for blocked sources)
- GitHub Actions (10-min interval refresh via cron)
- Static XML files committed to repo

## Project Structure

| File | Purpose |
|------|---------|
| `main.py` | FastAPI app, scheduler, fetcher, RSS XML builder, `--cron` mode (~680 lines, single-file) |
| `rrs_config.json` | Source config, refresh interval, retry settings |
| `Dockerfile` | python:3.12-slim + Chromium deps + Playwright |
| `requirements.txt` | Python dependencies |
| `.github/workflows/refresh.yml` | CI workflow: cron every 10 min → `--cron` → commit feeds/*.xml |

## Quick Start

```bash
# Local development
pip install -r requirements.txt
playwright install chromium
uvicorn main:app --reload --port 8000

# Local cron test (one-shot refresh)
python3 main.py --cron

# Docker local build and test
docker build -t rss-agg .
docker run -p 8000:8000 -v $(pwd)/data:/app/data rss-agg
```

## Configuration

| Key | Default | Description |
|-----|---------|-------------|
| `refresh_interval_minutes` | 10 | Auto-refresh interval |
| `max_items_per_feed` | 50 | Max items kept per source |
| `max_concurrent_fetches` | 15 | Concurrent fetch limit |
| `http_timeout_seconds` | 20 | HTTP request timeout |
| `cache_max_days` | 7 | Cache expiry window |
| `max_attempts` | 2 | Retry count on failure |
| `retry_delay_base` | 3 | Base retry delay in seconds |

### Per-Feed Options

| Field | Effect |
|-------|--------|
| `"browser": true` | Force Playwright headless browser (skip httpx) |
| `"type": "json"` | Discourse JSON API, bypasses feedparser |
| `"disabled": true` | Skip this source |

### Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `NODELOC_COOKIE` | optional | Cookie for NodeLoc forum auth |
| `DATA_DIR` | optional | Cache directory (default: `./data`) |
| `PORT` | auto | Port for uvicorn (default: 8000) |

## Deployment

Feeds are refreshed automatically via GitHub Actions cron (every 10 minutes). The workflow:
1. Checks out the repo
2. Sets up Python 3.12 + dependencies (including Playwright Chromium)
3. Runs `python3 main.py --cron` to fetch all feeds and generate static XML files
4. Commits the `feeds/*.xml` files to the main branch

### Feed URLs

Consumers can subscribe to individual feeds:
- Tech news: `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/tech.xml`
- Deals: `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/deals.xml`
- Forums: `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/forums.xml`
- Self-hosted: `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/selfhosted.xml`
- Hacker News: `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/hn.xml`
- AI: `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/ai.xml`
- YouTube: `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/youtube.xml`

Or import all at once via OPML:
- `https://raw.githubusercontent.com/xxy9468615/rss-aggregator/main/feeds/subscriptions.opml`

## Architecture

```
RSS Sources (42+)
  ↓  httpx (concurrent=15, retry with backoff)
  ↓  Playwright Chromium (fallback for blocked sources)
feedparser → cache.json (7-day cache)
  ↓
GitHub Actions cron (every 10 min) → feeds/*.xml (static XML files)
  ↓
GitHub raw URL → Consumer RSS readers (Folo, FreshRSS, etc.)
```

## Privacy

- DNT + Sec-GPC headers on all requests
- Strips tracking params (utm_*, gclid, fbclid, etc.) from item links

## Security Notes

- No secrets in code — all via environment variables
- `NODELOC_COOKIE` env var required for NodeLoc forum feed
- `.gitignore` excludes `data/`, `__pycache__/`, `.env`

## Endpoints (local server mode)

| Route | Description |
|-------|-------------|
| `GET /` | Dashboard (source list, stats, last refresh) |
| `GET /feeds/{name}` | RSS XML output per category |
| `GET /health` | Health check (503 if stale) |
| `GET /livez` | Liveness probe (always 200) |
| `GET /readyz` | Readiness probe (200 after first refresh) |
| `POST /refresh` | Manual refresh trigger |
