import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Set
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

import httpx
from playwright.async_api import async_playwright, Page
from tenacity import retry, stop_after_attempt, wait_exponential


# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADLESS = True
CONCURRENCY = int(os.getenv("CONCURRENCY", "3"))

BASE_URL = "https://www.newworld.com.fj"
HOME_URL = f"{BASE_URL}/"

# Store slug is required for New World to render correct catalog/prices
DEFAULT_STORE = os.getenv("NEWWORLD_STORE", "newworld-suva-damodar-city-id-S0017")

# Category fallback IDs (seed). You can extend this list as you discover more.
# You can also set env var NEWWORLD_CATEGORY_IDS="1154,1234,..." to override.
FALLBACK_CATEGORY_IDS = [1154]

# Pagination params (New World uses skip/take style on some category pages)
TAKE = int(os.getenv("NEWWORLD_TAKE", "40"))
MAX_PAGES_PER_CATEGORY = int(os.getenv("NEWWORLD_MAX_PAGES_PER_CATEGORY", "200"))


# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# ------------------------------------------------------------
# SUPABASE
# ------------------------------------------------------------

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}


def normalize_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    clean = re.sub(r"[^\d.]", "", str(text))
    try:
        return float(clean)
    except ValueError:
        return None


def generate_dedupe_key(source: str, url: str) -> str:
    raw = f"{source}|{url}".lower().strip()
    return hashlib.sha256(raw.encode()).hexdigest()


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def supabase_upsert(table: str, payload: Dict):
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=HEADERS,
            json=payload
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f"Supabase Error {r.status_code}: {r.text}")
        r.raise_for_status()


# ------------------------------------------------------------
# URL HELPERS
# ------------------------------------------------------------

def with_store(url: str, store: str) -> str:
    """Ensure store=<store> exists in URL query params."""
    try:
        parts = list(urlparse(url))
        qs = parse_qs(parts[4])
        qs["store"] = [store]
        parts[4] = urlencode(qs, doseq=True)
        return urlunparse(parts)
    except Exception:
        # If parsing fails, fallback: append ?store=
        if "?" in url:
            return url + f"&store={store}"
        return url + f"?store={store}"


def canonicalize(url: str) -> str:
    """Strip fragments, keep query (store matters)."""
    parts = list(urlparse(url))
    parts[5] = ""  # fragment
    return urlunparse(parts)


# ------------------------------------------------------------
# DISCOVERY
# ------------------------------------------------------------

async def discover_categories(page: Page) -> List[str]:
    """
    New World is SPA-ish; categories may not exist as direct links in DOM.
    We try best-effort DOM discovery; otherwise fallback to seed IDs.
    """
    logger.info("Discovering New World categories from home page...")
    try:
        await page.goto(with_store(HOME_URL, DEFAULT_STORE), wait_until="networkidle", timeout=60000)

        # Try to find category links in DOM
        links = await page.evaluate("""() => {
            const a = Array.from(document.querySelectorAll('a[href*="/category/"]'));
            return a.map(x => x.href);
        }""")

        categories = sorted({
            canonicalize(u) for u in links
            if "/category/" in u and u.startswith("http")
        })

        # Filter out obvious non-category junk
        categories = [c for c in categories if re.search(r"/category/\d+", c)]

        logger.info(f"Discovered {len(categories)} categories")
        if categories:
            return
