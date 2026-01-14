import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Set, Any
from urllib.parse import urljoin, urlparse

import httpx
from playwright.async_api import async_playwright, Page
from tenacity import retry, stop_after_attempt, wait_exponential

print("BOOT: mh_scraper.py started", flush=True)

# -----------------------
# CONFIG (ENV)
# -----------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADLESS = True
CONCURRENCY = int(os.getenv("CONCURRENCY", "10"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "100"))

MH_MAX_PAGES_PER_CATEGORY = int(os.getenv("MH_MAX_PAGES_PER_CATEGORY", "250"))
MH_MAX_CATEGORIES = int(os.getenv("MH_MAX_CATEGORIES", "500"))
MH_MAX_PRODUCTS_PER_RUN = int(os.getenv("MH_MAX_PRODUCTS_PER_RUN", "1200"))

BASE_URL = "https://mh.com.fj"
SHOP_URL = f"{BASE_URL}/shop/"

# -----------------------
# LOGGING
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("mh")

# -----------------------
# SUPABASE HEADERS
# -----------------------
RAW_HEADERS = {
    "apikey": SUPABASE_KEY or "",
    "Authorization": f"Bearer {SUPABASE_KEY or ''}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}
HISTORY_HEADERS = {
    "apikey": SUPABASE_KEY or "",
    "Authorization": f"Bearer {SUPABASE_KEY or ''}",
    "Content-Type": "application/json",
}

# -----------------------
# HELPERS
# -----------------------
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

def _same_domain(url: str) -> bool:
    try:
        return urlparse(url).netloc.endswith("mh.com.fj")
    except Exception:
        return False

# -----------------------
# SUPABASE BATCH WRITES
# -----------------------
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def supabase_upsert_raw_batch(client: httpx.AsyncClient, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    r = await client.post(f"{SUPABASE_URL}/rest/v1/raw_products", headers=RAW_HEADERS, json=rows)
    if r.status_code not in (200, 201, 204):
        logger.error(f"Supabase raw batch error {r.status_code}: {r.text}")
    r.raise_for_status()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def supabase_insert_history_batch(client: httpx.AsyncClient, rows: List[Dict[str, Any]]) -> None:
    if not rows:
        return
    r = await client.post(f"{SUPABASE_URL}/rest/v1/price_history", headers=HISTORY_HEADERS, json=rows)
    if r.status_code not in (200, 201, 204):
        logger.error(f"Supabase history batch error {r.status_code}: {r.text}")
    r.raise_for_status()

async def batch_flusher(queue: asyncio.Queue, client: httpx.AsyncClient) -> None:
    raw_batch: List[Dict[str, Any]] = []
    hist_batch: List[Dict[str, Any]] = []

    while True:
        item = await queue.get()
        if item is None:
            queue.task_done()
            break

        data = item
        raw_batch.append(data)
        hist_batch.append({
            "dedupe_key": data["dedupe_key"],
            "source": data["source"],
            "product_url": data["product_url"],
            "name": data["name"],
            "price_numeric": data["price_numeric"],
            "currency": data["currency"],
            "seen_at": data["scrape_timestamp"],
        })

        if len(raw_batch) >= BATCH_SIZE:
            await supabase_upsert_raw_batch(client, raw_batch)
            await supabase_insert_history_batch(client, hist_batch)
            logger.info(f"Flushed batch: raw={len(raw_batch)} history={len(hist_batch)}")
            raw_batch.clear()
            hist_batch.clear()

        queue.task_done()

    if raw_batch:
        await supabase_upsert_raw_batch(client, raw_batch)
        await supabase_insert_history_batch(client, hist_batch)
        logger.info(f"Final flush: raw={len(raw_batch)} history={len(hist_batch)}")

# -----------------------
# DISCOVERY + PAGINATION
# -----------------------
async def discover_categories(page: Page) -> List[str]:
    logger.info(f"Discovering categories from {SHOP_URL}")
    await page.goto(SHOP_URL, wait_until="domcontentloaded", timeout=60000)

    links = await page.evaluate(
        """() => Array.from(document.querySelectorAll('a[href*="/product-category/"]'))
              .map(a => a.href)
              .filter(Boolean)"""
    )

    unique: List[str] = []
    seen = set()
    for u in links:
        if not isinstance(u, str) or "/product-category/" not in u:
            continue
        u = u.split("#")[0]
        if _same_domain(u) and u not in seen:
            seen.add(u)
            unique.append(u)

    if len(unique) > MH_MAX_CATEGORIES:
        unique = unique[:MH_MAX_CATEGORIES]

    logger.info(f"Discovered {len(unique)} categories")
    return unique

async def collect_product_urls_from_listing(page: Page) -> Set[str]:
    hrefs = await page.evaluate(
        """() => {
            const sels = [
              'a.woocommerce-LoopProduct-link',
              'a.woocommerce-loop-product__link',
              'li.product a[href*="/product/"]'
            ];
            const out = new Set();
            for (const sel of sels) {
              document.querySelectorAll(sel).forEach(a => {
                if (a && a.href) out.add(a.href);
              });
            }
            return Array.from(out);
        }"""
    )

    urls: Set[str] = set()
    for u in hrefs:
        if isinstance(u, str) and _same_domain(u) and "/product/" in u:
            urls.add(u.split("#")[0])
    return urls

async def crawl_paginated_listing(page: Page, start_url: str) -> List[str]:
    logger.info(f"Crawling listing: {start_url}")
    product_urls: Set[str] = set()

    if not start_url.endswith("/"):
        start_url += "/"

    for n in range(1, MH_MAX_PAGES_PER_CATEGORY + 1):
        url = start_url if n == 1 else urljoin(start_url, f"page/{n}/")
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        batch = await collect_product_urls_from_listing(page)
        if not batch:
            logger.info(f"Listing ended (no products) at page={n}: {url}")
            break

        before = len(product_urls)
        product_urls |= batch
        logger.info(f"page={n}: +{len(product_urls)-before} (total={len(product_urls)})")

    return sorted(product_urls)

# -----------------------
# EXTRACTION
# -----------------------
async def extract_product_data(page: Page, url: str) -> Optional[Dict[str, Any]]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        html = await page.content()

        json_data: Dict[str, Any] = {}
        scripts = await page.locator("script[type='application/ld+json']").all_inner_texts()
        for script in scripts:
            try:
                data = json.loads(script)
                nodes = data.get("@graph", [data]) if isinstance(data, dict) else [data]
                for node in nodes:
                    if isinstance(node, dict) and node.get("@type") == "Product":
                        json_data = node
                        break
                if json_data:
                    break
            except Exception:
                continue

        dom_name = await page.title()
        h1 = await page.query_selector("h1.product_title")
        if h1:
            dom_name = (await h1.inner_text()).strip()

        price_text = None
        selectors = [
            "p.price ins span.woocommerce-Price-amount bdi",
            "p.price span.woocommerce-Price-amount bdi",
            "span.price span.woocommerce-Price-amount bdi",
        ]
        for sel in selectors:
            el = await page.query_selector(sel)
            if el:
                t = (await el.inner_text()).strip()
                if t:
                    price_text = t
                    break

        img_url = None
        img_el = await page.query_selector(".woocommerce-product-gallery__image img")
        if img_el:
            img_url = await img_el.get_attribute("src")

        dom_categories: List[str] = []
        cat_els = await page.query_selector_all("span.posted_in a")
        if cat_els:
            dom_categories = [(await c.inner_text()).strip() for c in cat_els]

        final_name = (json_data.get("name") or dom_name or "").strip()

        offers = json_data.get("offers", {})
        if isinstance(offers, list) and offers:
            offers = offers[0]

        currency = "FJD"
        final_price_raw = price_text
        if isinstance(offers, dict):
            if offers.get("price") is not None:
                final_price_raw = str(offers.get("price"))
            currency = offers.get("priceCurrency") or currency

        price_numeric = normalize_price(final_price_raw)

        brand = "MH"
        if json_data.get("brand"):
            b = json_data.get("brand")
            if isinstance(b, dict):
                brand = b.get("name") or brand
            elif isinstance(b, str):
                brand = b

        images: List[str] = []
        if json_data.get("image"):
            img = json_data.get("image")
            images = img if isinstance(img, list) else [img]
        elif img_url:
            images = [img_url]

        ts = datetime.now(timezone.utc).isoformat()
        dedupe = generate_dedupe_key("mh", url)

        return {
            "source": "mh",
            "product_url": url,
            "name": final_name,
            "brand": brand,
            "price_display": final_price_raw,
            "price_numeric": price_numeric,
            "currency": currency,
            "categories": dom_categories,
            "images": images,
            "raw_html": html,
            "scrape_timestamp": ts,
            "dedupe_key": dedupe,
        }
    except Exception as e:
        logger.error(f"Extract failed: {url} :: {e}")
        return None

# -----------------------
# WORKER
# -----------------------
async def worker(sem: asyncio.Semaphore, context, queue: asyncio.Queue, url: str) -> None:
    async with sem:
        page = await context.new_page()
        try:
            data = await extract_product_data(page, url)
            if data:
                await queue.put(data)
        finally:
            await page.close()

# -----------------------
# MAIN
# -----------------------
async def main() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY env vars")

    async with httpx.AsyncClient(timeout=60) as supa_client:
        async with async_playwright() as p:
            browser = await p.chromium.launch(headless=HEADLESS)
            context = await browser.new_context(
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/120.0.0.0 Safari/537.36"
                ),
                viewport={"width": 1280, "height": 720},
            )

            # Block heavy resources (speed!)
            async def block_heavy(route):
                r = route.request
                if r.resource_type in ("image", "font", "media"):
                    await route.abort()
                else:
                    await route.continue_()

            await context.route("**/*", block_heavy)

            page = await context.new_page()

            categories = await discover_categories(page)

            master_urls: Set[str] = set()

            # Safety net: crawl /shop/ pages (captures everything)
            shop_urls = await crawl_paginated_listing(page, SHOP_URL)
            master_urls |= set(shop_urls)
            logger.info(f"After /shop crawl, master_urls={len(master_urls)}")

            for cat in categories:
                try:
                    cat_urls = await crawl_paginated_listing(page, cat)
                    master_urls |= set(cat_urls)
                    logger.info(f"After category, master_urls={len(master_urls)}")
                except Exception as e:
                    logger.error(f"Category crawl failed: {cat} :: {e}")

            await page.close()

            urls = sorted(master_urls)
            logger.info(f"Total unique product URLs discovered: {len(urls)}")

            # Chunking (prevents cancellations)
            if MH_MAX_PRODUCTS_PER_RUN > 0:
                urls = urls[:MH_MAX_PRODUCTS_PER_RUN]
                logger.info(f"Processing {len(urls)} products this run (MH_MAX_PRODUCTS_PER_RUN)")

            queue: asyncio.Queue = asyncio.Queue(maxsize=CONCURRENCY * 4)
            flusher_task = asyncio.create_task(batch_flusher(queue, supa_client))

            sem = asyncio.Semaphore(CONCURRENCY)
            tasks = [worker(sem, context, queue, u) for u in urls]
            await asyncio.gather(*tasks)

            await queue.put(None)
            await queue.join()
            await flusher_task

            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
