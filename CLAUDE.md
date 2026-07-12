# CLAUDE.md — RSS Aggregator

## Project Overview
RSS 聚合服务 — 45+ 中文/英文源，缓存 7 天，10 分钟自动刷新。
部署在 Railway，Docker 容器运行。

## Tech Stack
- **Runtime:** Python 3.12, FastAPI, uvicorn
- **Parsing:** feedparser
- **HTTP:** httpx
- **Scheduling:** APScheduler (AsyncIOScheduler)
- **Deploy:** Docker → Railway

## Architecture
```
RSS Sources (45+)
  ↓  httpx (concurrent=15)
feedparser → /app/data/cache.json (7-day cache)
  ↓
FastAPI → Output RSS XML (deals.xml, etc.)
  ↓
Public URL → Consumers (freshRSS, etc.)
```

## Commands
```bash
# Local test
docker build -t rss-agg .
docker run -p 8000:8000 -v $(pwd)/data:/app/data rss-agg

# Deploy to Railway (from project dir)
railway up
```

## Key Files
| File | Purpose |
|------|---------|
| `main.py` | FastAPI app + scheduler + feed fetcher + RSS XML builder |
| `rrs_config.json` | 45+ source config, refresh interval, limits |
| `Dockerfile` | python:3.12-slim, uvicorn CMD |
| `railway.json` | Docker builder config |
| `RRs订阅源/` | Additional source list files |

## Config (rrs_config.json)
| Key | Value | Note |
|-----|-------|------|
| `refresh_interval_minutes` | 10 | 自动刷新间隔 |
| `max_items_per_feed` | 50 | 每源最多保留 |
| `max_concurrent_fetches` | 15 | 并发抓取上限 |
| `http_timeout_seconds` | 20 | HTTP 超时 |
| `cache_max_days` | 7 | 缓存过期天数 |

## Gotchas
- 缓存文件在 `/app/data/cache.json`，Railway 重启后容器文件系统重置，`DATA_DIR` 配了持久化目录才保留
- 1G 2vCPU 优化配置，`max_concurrent_fetches` 不超 30

## Related
- Vaultwarden 密码管理器在同个 Railway 账号下