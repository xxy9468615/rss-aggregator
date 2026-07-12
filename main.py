import asyncio
import hashlib
import json
import logging
import os
import random
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urlparse
from xml.etree.ElementTree import Element, SubElement, tostring

import feedparser
import httpx
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, Response
from playwright.async_api import async_playwright

# ── Config ──────────────────────────────────────────────
APP_DIR = Path(__file__).parent
CONFIG_PATH = APP_DIR / "rrs_config.json"
cfg = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
REFRESH_MINUTES = cfg.get("refresh_interval_minutes", 30)
MAX_ITEMS = cfg.get("max_items_per_feed", 50)
MAX_CONCURRENT = min(cfg.get("max_concurrent_fetches", 15), 30)
HTTP_TIMEOUT = cfg.get("http_timeout_seconds", 20)
CACHE_MAX_DAYS = cfg.get("cache_max_days", 7)
DATA_DIR = Path(os.environ.get("DATA_DIR", str(APP_DIR / "data")))
CACHE_PATH = DATA_DIR / "cache.json"
NODELOC_COOKIE = os.environ.get("NODELOC_COOKIE", "").strip()
MAX_ATTEMPTS = cfg.get("max_attempts", 2)
RETRY_DELAY_BASE = cfg.get("retry_delay_base", 3)

# ── State ───────────────────────────────────────────────
feeds_cache: dict[str, list[dict]] = {}
last_refresh: float = 0
refreshing = False
stats = {"fetched": 0, "failed": 0, "items_total": 0}

# ── Logging ─────────────────────────────────────────────
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("rss-agg")


def _handle_exception(loop, context):
    msg = context.get("exception", context.get("message", "Unknown"))
    log.error("Unhandled exception in event loop: %s", msg)


asyncio.get_event_loop().set_exception_handler(_handle_exception)

# ── App ─────────────────────────────────────────────────
app = FastAPI(title="RSS Aggregator", version="1.4")
scheduler = AsyncIOScheduler()

_http_limits = httpx.Limits(
    max_connections=MAX_CONCURRENT * 2,
    max_keepalive_connections=10,
)
_http_timeout = httpx.Timeout(HTTP_TIMEOUT, connect=10, pool=5)


# ── Privacy / anti-tracking ─────────────────────────────
_TRACKING_PARAMS = {"utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
                    "gclid", "fbclid", "ref", "_ga", "_gl", "msclkid", "igshid"}


def strip_tracking_params(url: str) -> str:
    """Remove common tracking query parameters from a URL."""
    try:
        from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
        parsed = urlparse(url)
        qs = parse_qs(parsed.query, keep_blank_values=False)
        qs = {k: v for k, v in qs.items() if k not in _TRACKING_PARAMS}
        return urlunparse(parsed._replace(query=urlencode(qs, doseq=True)))
    except Exception:
        return url


_TRACKER_DOMAINS = {
    "google-analytics.com", "googletagmanager.com", "googlesyndication.com",
    "doubleclick.net", "facebook.net", "connect.facebook.net",
    "twitter.com", "platform.twitter.com", "x.com",
    "tiktok.com", "ads-twitter.com", "snapchat.com",
    "hotjar.com", "clarity.ms", "mixpanel.com",
    "segment.com", "amplitude.com", "newrelic.com",
    "scorecardresearch.com", "quantserve.com",
}


def _privacy_headers(url: str) -> dict:
    """Add privacy-friendly headers."""
    host = urlparse(url).hostname or ""
    return {
        "DNT": "1",
        "Sec-Fetch-Dest": "empty",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "none",
        "Sec-GPC": "1",
    }


# ── Playwright browser (headless Chromium) ───────────────
_browser = None
_playwright_ctx = None


async def init_browser():
    """Launch headless Chromium at startup."""
    global _browser, _playwright_ctx
    try:
        _playwright_ctx = await async_playwright().start()
        _browser = await _playwright_ctx.chromium.launch(headless=True)
        log.info("Playwright headless browser ready")
    except Exception as e:
        log.warning("Playwright init failed: %s", e)


async def close_browser():
    """Shutdown browser at shutdown."""
    global _browser, _playwright_ctx
    if _browser:
        await _browser.close()
        _browser = None
    if _playwright_ctx:
        await _playwright_ctx.stop()
        _playwright_ctx = None
        log.info("Playwright browser closed")


async def fetch_with_browser(url: str) -> str | None:
    """Fetch a URL via headless Chromium, return HTML body or None."""
    global _browser
    if not _browser:
        return None
    page = None
    try:
        page = await _browser.new_page(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            extra_http_headers={
                "DNT": "1",
                "Sec-GPC": "1",
            },
        )
        await page.goto(url, wait_until="domcontentloaded", timeout=HTTP_TIMEOUT * 1000)
        await asyncio.sleep(2)
        html = await page.content()
        return html
    except Exception as e:
        log.warning("Browser fetch failed %s: %s", url, e)
        return None
    finally:
        if page:
            await page.close()


# ── Cache persistence ────────────────────────────────────
_CACHE_DIR_PERSISTED = False


def _ensure_cache_dir():
    global _CACHE_DIR_PERSISTED
    if not _CACHE_DIR_PERSISTED:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        _CACHE_DIR_PERSISTED = True


def save_cache():
    _ensure_cache_dir()
    payload = {
        "feeds_cache": feeds_cache,
        "last_refresh": last_refresh,
        "stats": stats,
        "saved_at": time.time(),
    }
    CACHE_PATH.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    log.debug("Cache saved to %s", CACHE_PATH)


def load_cache():
    global feeds_cache, last_refresh, stats
    if not CACHE_PATH.exists():
        log.info("No cache file found at %s", CACHE_PATH)
        return
    try:
        payload = json.loads(CACHE_PATH.read_text(encoding="utf-8"))
        feeds_cache = payload.get("feeds_cache", {})
        last_refresh = payload.get("last_refresh", 0)
        stats = payload.get("stats", {"fetched": 0, "failed": 0, "items_total": 0})
        log.info(
            "Loaded cache from disk: %d categories, %d total items",
            len(feeds_cache), stats.get("items_total", 0),
        )
    except Exception as e:
        log.warning("Failed to load cache: %s", e)


# ── Helpers ─────────────────────────────────────────────
def _default_headers(url: str) -> dict:
    """Build request headers, adding cookie for nodeloc.com only."""
    hdrs: dict[str, str] = {}
    host = urlparse(url).hostname or ""
    if NODELOC_COOKIE and (host == "nodeloc.com" or host.endswith(".nodeloc.com")):
        hdrs["Cookie"] = NODELOC_COOKIE
    return hdrs


# Browser-like UA pool for sites that block cloud IPs
_BROWSER_UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:132.0) Gecko/20100101 Firefox/132.0",
]

_BROWSER_UA_HOSTS = {"linux.do", "reddit.com", "nodeloc.com"}


def _is_retryable(status_code: int) -> bool:
    """Check if HTTP status is worth retrying."""
    return status_code in (429, 403, 503, 502, 500)


def _parse_discourse_json(text: str, source_name: str, source_tag: str) -> list[dict]:
    """Parse Discourse /latest.json topic list into RSS items."""
    items: list[dict] = []
    try:
        d = json.loads(text)
    except (json.JSONDecodeError, ValueError):
        log.warning("Discourse JSON decode failed")
        return items

    try:
        topics = d.get("topic_list", {}).get("topics", [])
        for t in topics:
            tid = t.get("id", 0)
            title = t.get("title", "")
            slug = t.get("slug", "")
            link = strip_tracking_params(f"https://www.nodeloc.com/t/{slug}/{tid}") if tid else ""
            excerpt = t.get("excerpt", "") or ""
            excerpt = re.sub(r"<[^>]+>", "", excerpt).strip()
            pub_str = t.get("created_at", "").replace("Z", "+00:00")
            pub_ts = datetime.fromisoformat(pub_str).timestamp()
            items.append({
                "title": title,
                "link": link,
                "description": excerpt,
                "pubDate": datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT"),
                "pub_ts": pub_ts,
                "guid": f"nodeloc-{tid}",
                "source": source_name,
                "source_tag": source_tag,
            })
    except Exception as e:
        log.warning("Failed to parse Discourse JSON topics: %s", e)
    return items


# ── Fetch logic with retry + browser fallback ────────────
async def fetch_one(client: httpx.AsyncClient, feed_cfg: dict) -> list[dict]:
    """Fetch a single feed with retry + browser fallback, return normalized items."""
    url = feed_cfg["url"]
    source_name = feed_cfg.get("name", url)
    source_tag = feed_cfg.get("source", "")
    feed_type = feed_cfg.get("type", "xml")
    force_browser = feed_cfg.get("browser", False)
    host = urlparse(url).hostname or ""

    if force_browser:
        # Skip httpx, go straight to browser
        html = await fetch_with_browser(url)
        if not html:
            return []
        parsed = feedparser.parse(html)
        return _build_items(parsed, source_name, source_tag)

    for attempt in range(MAX_ATTEMPTS):
        extra_headers = {}
        if host in _BROWSER_UA_HOSTS and "reddit.com" not in url:
            extra_headers["User-Agent"] = random.choice(_BROWSER_UAS)
        elif "reddit.com" in url:
            extra_headers["User-Agent"] = _BROWSER_UAS[0]

        hdrs = {**_default_headers(url), **_privacy_headers(url), **extra_headers}
        html = None
        try:
            resp = await client.get(url, follow_redirects=True, headers=hdrs)

            if _is_retryable(resp.status_code) and attempt < MAX_ATTEMPTS - 1:
                wait = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 2)
                log.warning("Retry %d/%d for %s (status %d, wait %.1fs)",
                           attempt + 1, MAX_ATTEMPTS, url, resp.status_code, wait)
                await asyncio.sleep(wait)
                continue

            resp.raise_for_status()
            html = resp.text

        except httpx.HTTPStatusError as e:
            if _is_retryable(e.response.status_code) and attempt < MAX_ATTEMPTS - 1:
                wait = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 2)
                log.warning("Retry %d/%d for %s (status %d, wait %.1fs)",
                           attempt + 1, MAX_ATTEMPTS, url, e.response.status_code, wait)
                await asyncio.sleep(wait)
                continue
            # Last attempt or non-retryable → try browser fallback
            if attempt == MAX_ATTEMPTS - 1:
                log.info("httpx failed for %s, trying browser...", url)
                html = await fetch_with_browser(url)
                if not html:
                    log.warning("Fetch failed %s (status %d): %s", url, e.response.status_code, e)
                    return []
            else:
                log.warning("Fetch failed %s (status %d): %s", url, e.response.status_code, e)
                return []
        except Exception as e:
            if attempt < MAX_ATTEMPTS - 1:
                wait = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 2)
                log.warning("Retry %d/%d for %s (error: %s, wait %.1fs)",
                           attempt + 1, MAX_ATTEMPTS, url, e, wait)
                await asyncio.sleep(wait)
                continue
            # Last attempt → try browser fallback
            if attempt == MAX_ATTEMPTS - 1:
                log.info("httpx error for %s, trying browser...", url)
                html = await fetch_with_browser(url)
                if not html:
                    log.warning("Fetch failed %s: %s", url, e)
                    return []
            else:
                log.warning("Fetch failed %s: %s", url, e)
                return []

        if html:
            if feed_type == "json" or urlparse(url).path.endswith(".json"):
                return _parse_discourse_json(html, source_name, source_tag)

            parsed = feedparser.parse(html)
            return _build_items(parsed, source_name, source_tag)

    return []


def _build_items(parsed, source_name: str, source_tag: str) -> list[dict]:
    """Normalize feedparser results into item dicts."""
    items = []
    for entry in parsed.entries[:MAX_ITEMS]:
        link = strip_tracking_params(entry.get("link", ""))
        title = entry.get("title", "")
        summary = entry.get("summary", entry.get("description", ""))
        pub = entry.get("published_parsed") or entry.get("updated_parsed")
        if pub:
            pub_dt = datetime(*pub[:6], tzinfo=timezone.utc)
        else:
            pub_dt = datetime.now(timezone.utc)
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


async def refresh_all():
    global feeds_cache, last_refresh, refreshing, stats
    if refreshing:
        return
    refreshing = True
    t0 = time.time()
    active = sum(1 for s in cfg["sources"].values() for f in s["feeds"] if not f.get("disabled"))
    log.info("Starting refresh (%d sources, max_attempts=%d)...", active, MAX_ATTEMPTS)
    new_cache: dict[str, list[dict]] = {}
    total_items = 0
    fetched = 0
    failed = 0

    async with httpx.AsyncClient(
        headers={"User-Agent": "RSS-Aggregator/1.4"},
        limits=_http_limits,
        timeout=_http_timeout,
    ) as client:
        tasks = []
        for cat_key, cat_cfg in cfg["sources"].items():
            for feed_cfg in cat_cfg["feeds"]:
                if feed_cfg.get("disabled"):
                    continue
                tasks.append((cat_key, feed_cfg))

        sem = asyncio.Semaphore(MAX_CONCURRENT)

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

    cutoff = time.time() - CACHE_MAX_DAYS * 86400
    for cat_key, cat_cfg in cfg["sources"].items():
        all_items: list[dict] = []
        seen_guids: set[str] = set()
        for ck, items in results:
            if ck != cat_key:
                continue
            for item in items:
                if item["pub_ts"] < cutoff:
                    continue
                if item["guid"] not in seen_guids:
                    seen_guids.add(item["guid"])
                    all_items.append(item)
        all_items.sort(key=lambda x: x["pub_ts"], reverse=True)
        all_items = all_items[:MAX_ITEMS]
        new_cache[cat_key] = all_items
        total_items += len(all_items)

    feeds_cache = new_cache
    last_refresh = time.time()
    refreshing = False
    stats = {"fetched": fetched, "failed": failed, "items_total": total_items}
    log.info(
        "Refresh done in %.1fs: %d ok / %d fail / %d items (cache max %d days)",
        time.time() - t0, fetched, failed, total_items, CACHE_MAX_DAYS,
    )
    save_cache()


# ── RSS XML output ──────────────────────────────────────
def build_rss_xml(cat_key: str) -> str:
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
    rows = ""
    for cat_key, cat_cfg in cfg["sources"].items():
        count = len(feeds_cache.get(cat_key, []))
        rows += f'<tr><td><a href="/feeds/{cat_cfg["output_feed"]}">{cat_cfg["name"]}</a></td><td>{cat_cfg["output_feed"]}</td><td>{count}</td></tr>\n'
    elapsed = f"{(time.time()-last_refresh):.0f}s ago" if last_refresh else "never"
    cookie_status = "set" if NODELOC_COOKIE else "not set"
    html = f"""<!DOCTYPE html><html><head><meta charset="utf-8"><title>RSS Aggregator</title>
<style>body{{font-family:system-ui;max-width:800px;margin:2em auto;padding:0 1em}}table{{border-collapse:collapse;width:100%}}td,th{{padding:8px 12px;text-align:left;border-bottom:1px solid #eee}}a{{color:#0366d6;text-decoration:none}}</style></head>
<body><h1>RSS Aggregator</h1><p>42 sources · 7 categories · refresh every {REFRESH_MINUTES}min · retry {MAX_ATTEMPTS}x · last: {elapsed}</p>
<p>Stats: {stats["fetched"]} ok / {stats["failed"]} fail / {stats["items_total"]} items · cookie: {cookie_status}</p>
<table><tr><th>Feed</th><th>File</th><th>Items</th></tr>{rows}</table></body></html>"""
    return HTMLResponse(html)


@app.get("/feeds/{filename}")
async def get_feed(filename: str):
    for cat_key, cat_cfg in cfg["sources"].items():
        if cat_cfg["output_feed"] == filename:
            xml = build_rss_xml(cat_key)
            return Response(content=xml, media_type="application/rss+xml; charset=utf-8")
    return Response(content="Feed not found", status_code=404)


# ── Monitoring ───────────────────────────────────────────
START_TIME = time.time()


@app.get("/health")
@app.head("/health")
async def health():
    elapsed = time.time() - last_refresh if last_refresh else 0
    uptime = time.time() - START_TIME
    stale_limit = max(REFRESH_MINUTES * 60 * 3, 600)
    healthy = not last_refresh or elapsed < stale_limit
    status_code = 200 if healthy else 503
    return Response(
        content=json.dumps({
            "status": "ok" if healthy else "stale",
            "uptime_seconds": int(uptime),
            "last_refresh_seconds_ago": int(elapsed) if last_refresh else None,
            "refreshing": refreshing,
            "stats": stats,
            "cache_on_disk": CACHE_PATH.exists(),
        }, ensure_ascii=False),
        status_code=status_code,
        media_type="application/json",
    )


@app.get("/livez")
async def livez():
    return Response(
        content=json.dumps({"status": "alive"}, ensure_ascii=False),
        media_type="application/json",
    )


@app.get("/readyz")
async def readyz():
    ready = last_refresh > 0 or CACHE_PATH.exists()
    status_code = 200 if ready else 503
    return Response(
        content=json.dumps({
            "status": "ready" if ready else "not_ready",
            "last_refresh_seconds_ago": int(time.time() - last_refresh) if last_refresh else None,
        }, ensure_ascii=False),
        status_code=status_code,
        media_type="application/json",
    )


@app.post("/refresh")
async def manual_refresh():
    await refresh_all()
    return {"status": "ok", "stats": stats}


# ── Startup / Shutdown ──────────────────────────────────
@app.on_event("startup")
async def startup():
    await init_browser()
    load_cache()
    if feeds_cache:
        log.info("Serving cached data while refresh runs in background")
        asyncio.create_task(start_refresh_loop())
    else:
        log.info("No cached data, performing initial refresh...")
        await refresh_all()
        scheduler.add_job(refresh_all, "interval", minutes=REFRESH_MINUTES, id="refresh")
        scheduler.start()
        log.info("Scheduler started, refresh every %d min", REFRESH_MINUTES)


async def start_refresh_loop():
    await refresh_all()
    scheduler.add_job(refresh_all, "interval", minutes=REFRESH_MINUTES, id="refresh")
    scheduler.start()
    log.info("Scheduler started, refresh every %d min", REFRESH_MINUTES)


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()
    await close_browser()


# ── Entry ───────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", "8000"))
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=port,
        workers=1,
    )
