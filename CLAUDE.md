# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
RSS 聚合服务 — 42 个中英文源，7 分类，10 分钟自动刷新。部署在 Railway，Docker 容器运行。

## Architecture
单文件 `main.py`：FastAPI app + APScheduler + httpx 并发抓取 + Playwright 无头浏览器 fallback。
Sources → fetch (httpx → browser fallback) → feedparser → cache.json → RSS XML → consumers。

## Key Files
| File | Purpose |
|------|---------|
| `main.py` | 全部逻辑：配置、抓取、缓存、RSS 输出、路由 |
| `rrs_config.json` | 源配置（URL、type、browser 标记、disabled） |
| `Dockerfile` | python:3.12-slim + Chromium deps + playwright install |
| `railway.json` | Docker builder 配置 |
| `requirements.txt` | 依赖（含 playwright） |

## Commands
```bash
# 本地测试
docker build -t rss-agg .
docker run -p 8000:8000 -v $(pwd)/data:/app/data rss-agg

# Railway 部署（push 即触发自动部署）
git push origin main
```

## Config (rrs_config.json)
| Key | Default | Note |
|-----|---------|------|
| `refresh_interval_minutes` | 10 | 自动刷新间隔 |
| `max_items_per_feed` | 50 | 每源最多保留 |
| `max_concurrent_fetches` | 15 | 并发抓取上限 |
| `http_timeout_seconds` | 20 | HTTP 超时 |
| `cache_max_days` | 7 | 缓存过期天数 |
| `max_attempts` | 2 | 失败重试次数 |
| `retry_delay_base` | 3 | 重试基础延迟（秒） |

## Feed Config Fields
| Field | Type | Note |
|-------|------|------|
| `type: "json"` | optional | Discourse JSON API，不走 feedparser |
| `browser: true` | optional | 强制走 Playwright 无头浏览器 |
| `disabled: true` | optional | 跳过此源 |

## Key Behaviors
- **三层 fallback**：httpx → 指数退避重试（429/403/503/502/500）→ Playwright 浏览器
- **Discourse JSON**：NodeLoc 用 `/latest.json`，需要 `NODELOC_COOKIE` env var
- **浏览器 UA 池**：`_BROWSER_UAS` 随机轮换，`linux.do`、`reddit.com`、`nodeloc.com` 自动命中
- **缓存持久化**：`DATA_DIR=/app/data`（Railway volume），重启不丢
- **资源**：Chromium ~150-200MB，1G 内存够用，`max_concurrent_fetches` 不超 30

## Endpoints
- `GET /` — 管理面板（源列表 + 状态）
- `GET /feeds/{filename}` — RSS XML 输出
- `GET /health` — 健康检查（stale 时 503）
- `GET /livez` / `GET /readyz` — K8s 探针
- `POST /refresh` — 手动刷新
