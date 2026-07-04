"""RSS Aggregator — FastAPI + feedparser + APScheduler"""

import asyncio
import hashlib
import json
import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from xml.etree.ElementTree import Element, SubElement, tostring

import feedparser
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse, Response

# ── Config ──────────────────────────────────────────────
CONFIG_PATH = Path(__file__).parent / "rrs_config.json"
cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
REFRESH_MINUTES = cfg.get("refresh_interval_minutes", 30)
MAX_ITEMS = cfg.get("max_items_per_feed", 50)

# ── State ───────────────────────────────────────────────
feeds_cache: dict[str, list[dict]] = {}   # category_key → [item, ...]
last_refresh: float = 0
refreshing = False
stats = {"fetched": 0, "failed": 0, "items_total": 0}

# ── Logging ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rss-agg")

# ── App ─────────────────────────────────────────────────
app = FastAPI(title="RSS Aggregator", version="1.0")
scheduler = AsyncIOScheduler()

# ── Fetch logic ─────────────────────────────────────────
async def fetch_one(client: httpx.AsyncClient, feed_cfg: dict) -> list[dict]:
    """Fetch a single RSS feed, return normalized items."""
    url = feed_cfg["url"]
    source_name = feed_cfg.get("name", url)
    source_tag = feed_cfg.get("source", "")
    try:
        resp = await client.get(url, follow_redirects=True, timeout=15)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.text)
        items = []
        for entry in parsed.entries[:MAX_ITEMS]:
            link = entry.get("link", "")
            title = entry.get("title", "")
            summary = entry.get("summary", entry.get("description", ""))
            # pubDate
            pub = entry.get("published_parsed") or entry.get("updated_parsed")
            if pub:
                pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
            else:
                pub_dt = datetime.now(timezone.utc)
            # guid = link hash (dedup by URL)
            guid = hashlib.md5(link.encode()).hexdigest() if link else hashlib.md5(title.encode()).hexdigest()
            items.append({
                "title": title,
                "link": link,
                "description": summary,
                "pubDate": pub_dt.strftime("%a, %d %b %Y %H:%M:%S GMT"),
                "pub_ts": pub_dt.timestamp(),
                "guid": guid,
                "source": source_name,
                "source_tag": source_tag,
            })
        return items
    except Exception as e:
        log.warning("Fetch failed %s: %s", url, e)
        return []


async def refresh_all():
    """Refresh all feeds from config."""
    global feeds_cache, last_refresh, refreshing, stats
    if refreshing:
        return
    refreshing = True
    t0 = time.time()
    log.info("Starting refresh...")
    new_cache: dict[str, list[dict]] = {}
    total_items = 0
    fetched = 0
    failed = 0

    async with httpx.AsyncClient(headers={"User-Agent": "RSS-Aggregator/1.0"}) as client:
        tasks = []
        for cat_key, cat_cfg in cfg["sources"].items():
            for feed_cfg in cat_cfg["feeds"]:
                tasks.append((cat_key, feed_cfg))
        # Fetch all in parallel (limit concurrency via httpx pool)
        sem = asyncio.Semaphore(10)
        async def limited_fetch(cat_key, feed_cfg):
            nonlocal fetched, failed
            async with sem:
                items = await fetch_one(client, feed_cfg)
                if items:
                    fetched += 1
                else:
                    failed += 1
                return cat_key, items

        results = await asyncio.gather(*[limited_fetch(k, f) for k, f in tasks])

    # Merge & dedup per category
    for cat_key, cat_cfg in cfg["sources"].items():
        all_items: list[dict] = []
        seen_guids: set[str] = set()
        for ck, items in results:
            if ck != cat_key:
                continue
            for item in items:
                if item["guid"] not in seen_guids:
                    seen_guids.add(item["guid"])
                    all_items.append(item)
        # Sort by pubDate desc, limit
        all_items.sort(key=lambda x: x["pub_ts"], reverse=True)
        all_items = all_items[:MAX_ITEMS]
        new_cache[cat_key] = all_items
        total_items += len(all_items)

    feeds_cache = new_cache
    last_refresh = time.time()
    refreshing = False
    stats = {"fetched": fetched, "failed": failed, "items_total": total_items}
    log.info("Refresh done in %.1fs: %d fetched, %d failed, %d items", time.time()-t0, fetched, failed, total_items)


# ── RSS XML output ──────────────────────────────────────
def build_rss_xml(cat_key: str) -> str:
    """Build RSS 2.0 XML for a category."""
    cat_cfg = cfg["sources"][cat_key]
    items = feeds_cache.get(cat_key, [])

    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")
    SubElement(channel, "title").text = cat_cfg["name"]
    SubElement(channel, "link").text = "https://rss-aggregator.local"
    SubElement(channel, "description").text = f"Aggregated feed: {cat_cfg['name']}"
    SubElement(channel, "lastBuildDate").text = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")

    for item in items:
        i = SubElement(channel, "item")
        SubElement(i, "title").text = item["title"]
        SubElement(i, "link").text = item["link"]
        SubElement(i, "description").text = item["description"]
        SubElement(i, "pubDate").text = item["pubDate"]
        SubElement(i, "guid").text = item["guid"]
        SubElement(i, "source").text = item["source"]

    return '<?xml version="1.0" encoding="UTF-8"?>\n' + tostring(rss, encoding="unicode")


# ── Routes ──────────────────────────────────────────────
@app.get("/")
async def index():
    """Feed list page."""
    rows = ""
    for cat_key, cat_cfg in cfg["sources"].items():
        count = len(feeds_cache.get(cat_key, []))
        rows += f'<tr><td><a href="/feeds/{cat_cfg["output_feed"]}">{cat_cfg["name"]}</a></td><td>{cat_cfg["output_feed"]}</td><td>{count}</td></tr>\n'
    elapsed = f"{(time.time()-last_refresh):.0f}s ago" if last_refresh else "never"
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>RSS Aggregator</title>
<style>body{{font-family:system-ui;max-width:800px;margin:2em auto;padding:0 1em}}table{{border-collapse:collapse;width:100%}}td,th{{padding:8px 12px;text-align:left;border-bottom:1px solid #eee}}a{{color:#0366d6;text-decoration:none}}</style></head>
<body><h1>RSS Aggregator</h1><p>45 sources · 7 categories · refresh every {REFRESH_MINUTES}min · last: {elapsed}</p>
<p>Stats: {stats["fetched"]} ok / {stats["failed"]} fail / {stats["items_total"]} items</p>
<table><tr><th>Feed</th><th>File</th><th>Items</th></tr>{rows}</table></body></html>"""
    return HTMLResponse(html)


@app.get("/feeds/{filename}")
async def get_feed(filename: str):
    """Return RSS XML for a category."""
    for cat_key, cat_cfg in cfg["sources"].items():
        if cat_cfg["output_feed"] == filename:
            xml = build_rss_xml(cat_key)
            return Response(content=xml, media_type="application/rss+xml; charset=utf-8")
    return Response(content="Feed not found", status_code=404)


@app.get("/health")
async def health():
    return {"status": "ok", "last_refresh": last_refresh, "stats": stats}


@app.post("/refresh")
async def manual_refresh():
    await refresh_all()
    return {"status": "ok", "stats": stats}


# ── Startup / Shutdown ──────────────────────────────────
@app.on_event("startup")
async def startup():
    await refresh_all()
    scheduler.add_job(refresh_all, "interval", minutes=REFRESH_MINUTES, id="refresh")
    scheduler.start()
    log.info("Scheduler started, refresh every %d min", REFRESH_MINUTES)


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ── Entry ───────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(__import__("os").environ.get("PORT", "8000"))
    uvicorn.run(app, host="0.0.0.0", port=port)