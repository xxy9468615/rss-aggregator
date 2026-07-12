# RSS Aggregator

42+ RSS sources across 7 categories, auto-refresh every 10 minutes. Self-hosted on Railway with a custom domain.

## Public URL

https://huike.indevs.in

## Tech Stack

- Python 3.12 + FastAPI + uvicorn
- feedparser + httpx (concurrent fetching)
- Playwright headless Chromium (fallback for blocked sources)
- APScheduler (10-min interval refresh)
- Docker → Railway

## Project Structure

| File | Purpose |
|------|---------|
| `main.py` | FastAPI app, scheduler, fetcher, RSS XML builder (~530 lines, single-file) |
| `rrs_config.json` | Source config, refresh interval, retry settings |
| `Dockerfile` | python:3.12-slim + Chromium deps + Playwright |
| `requirements.txt` | Python dependencies |

## Quick Start

```bash
# Local test
docker build -t rss-agg .
docker run -p 8000:8000 -v $(pwd)/data:/app/data rss-agg

# Deploy
git push origin main  # triggers Railway auto-deploy
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
| `DATA_DIR` | optional | Cache directory (default: `/app/data`) |
| `PORT` | auto | Railway provides this automatically |

## Deployment

Deployed on [Railway](https://railway.app) with Docker builder.

1. Connect repo to Railway (new project → import from GitHub)
2. Railway auto-detects `Dockerfile` and builds
3. Add a custom domain in Railway settings (e.g. `huike.indevs.in`)
4. Push to `main` branch triggers automatic redeploy

Build includes Playwright Chromium (~100MB), so first deploy takes ~2-3 minutes. Subsequent deploys are cached.

Resources: 1G RAM, 2 vCPU is sufficient.

## Architecture

```
RSS Sources (42+)
  ↓  httpx (concurrent=15, retry with backoff)
  ↓  Playwright Chromium (fallback for blocked sources)
feedparser → /app/data/cache.json (7-day cache)
  ↓
FastAPI → RSS XML output (/feeds/{filename})
  ↓
huike.indevs.in → Consumers (FreshRSS, etc.)
```

## Privacy

- DNT + Sec-GPC headers on all requests
- Strips tracking params (utm_*, gclid, fbclid, etc.) from item links

## Security Notes

- No secrets in code — all via environment variables
- `NODELOC_COOKIE` env var required for NodeLoc forum feed
- `.gitignore` excludes `data/`, `__pycache__/`, `.env`

## Endpoints

| Route | Description |
|-------|-------------|
| `GET /` | Dashboard (source list, stats, last refresh) |
| `GET /feeds/{name}` | RSS XML output per category |
| `GET /health` | Health check (503 if stale) |
| `GET /livez` | Liveness probe (always 200) |
| `GET /readyz` | Readiness probe (200 after first refresh) |
| `POST /refresh` | Manual refresh trigger |
