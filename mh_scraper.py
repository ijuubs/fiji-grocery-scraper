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
# CONFIG
# -----------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADLESS = True
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))

BASE_URL = "https://mh.com.fj"
SHOP_URL = f"{BASE_URL}/shop/"

# Safety brakes
MAX_PAGES_PER_CATEGORY = int(os.getenv("MH_MAX_PAGES_PER_CATEGORY", "250"))
MAX_CATEGORIES = int(os.getenv("MH_MAX_CATEGORIES", "500"))

# -----------------------
# LOGGING
# -----------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("mh")

# -----------------------
# SUPABASE
# -----------------------
HEADERS = {
    "apikey": SUPABASE_KEY or "",
    "Authorization": f"Bearer {SUPABASE_KEY or ''}",
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
async def supabase_upsert(table: str, payload: Dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.post(f"{SUPABASE_URL}/rest/v1/{table}", headers=HEADERS, json=payload)
        if r.status_code not in (200, 201, 204):
            logger.error(f"Supabase Error {r.status_code}: {r.text}")
        r.raise_for_status()

# -----------------------
# DISCOVERY
# -----------------------
def _same_domain(url: str) -> bool:
    try:
        return urlparse(url).netloc.endswith("mh.com.fj")
    except Exception:
        return False

async def discover_categories(page: Page) -> List[str]:
    """
    Pull category URLs from the /shop/ sidebar or any anchor containing /product-category/.
    """
    logger.info(f"Discovering categories from {SHOP_URL}")
    await page.goto(SHOP_URL, wait_until="domcontentloaded", timeout=60000)

    links = await page.evaluate(
        """() => Array.from(document.querySelectorAll('a[href*="/product-category/"]'))
              .map(a => a.href)
              .filter(Boolean)"""
    )

    # Deduplicate, keep only mh.com.fj
    unique = []
    seen = set()
    for u in links:
        if not _same_domain(u):
            continue
        if "/product-category/" not in u:
            continue
        # Normalize: remove fragments
        u = u.split("#")[0]
        if u not in seen:
            seen.add(u)
            unique.append(u)

    # Safety
    if len(unique) > MAX_CATEGORIES:
        unique = unique[:MAX_CATEGORIES]

    logger.info(f"Discovered {len(unique)} category URLs")
    return unique

# -----------------------
# PAGINATION + URL COLLECTION
# -----------------------
async def collect_product_urls_from_listing(page: Page) -> Set[str]:
    """
    Robustly collects product links from a listing page.
    WooCommerce commonly uses:
      - a.woocommerce-LoopProduct-link
      - a.woocommerce-loop-product__link
    """
    hrefs = await page.evaluate(
        """() => {
            const sels = [
              'a.woocommerce-LoopProduct-link',
              'a.woocommerce-loop-product__link',
              'li.product a[href*="/product/"]',
              'a[href*="/product/"]'
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
        if u and _same_domain(u) and "/product/" in u:
            urls.add(u.split("#")[0])
    return urls

async def crawl_paginated_listing(page: Page, start_url: str) -> List[str]:
    """
    Crawl /page/N/ until products stop appearing.
    Works for:
      - category pages: /product-category/xxx/page/2/
      - shop pages: /shop/page/2/
    """
    logger.info(f"Crawling listing: {start_url}")
    product_urls: Set[str] = set()

    # Ensure trailing slash consistency
    if not start_url.endswith("/"):
        start_url += "/"

    for n in range(1, MAX_PAGES_PER_CATEGORY + 1):
        if n == 1:
            url = start_url
        else:
            # WooCommerce pagination pattern
            url = urljoin(start_url, f"page/{n}/")

        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(500)

        batch = await collect_product_urls_from_listing(page)

        # If empty: stop
        if not batch:
            logger.info(f"Listing ended (no products) at page={n}: {url}")
            break

        before = len(product_urls)
        product_urls |= batch
        logger.info(f"page={n}: +{len(product_urls)-before} products (total={len(product_urls)})")

        # If we keep getting very small batches, you can optionally stop early,
        # but safest is to rely on empty page stop.

    return sorted(product_urls)

# -----------------------
# EXTRACTION
# -----------------------
async def extract_product_data(page: Page, url: str) -> Optional[Dict[str, Any]]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(250)

        html = await page.content()

        # JSON-LD first (Yoast/WooCommerce often places Product schema inside @graph)
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

        # DOM fallbacks
        dom_name = await page.title()
        h1 = await page.query_selector("h1.product_title")
        if h1:
            dom_name = (await h1.inner_text()).strip()

        # Price: handle normal + sale
        price_text = None
        price_sel = [
            "p.price ins span.woocommerce-Price-amount bdi",   # sale current
            "p.price span.woocommerce-Price-amount bdi",       # normal
            "span.price span.woocommerce-Price-amount bdi",
        ]
        for sel in price_sel:
            el = await page.query_selector(sel)
            if el:
                price_text = (await el.inner_text()).strip()
                if price_text:
                    break

        # Image
        img_url = None
        img_el = await page.query_selector(".woocommerce-product-gallery__image img")
        if img_el:
            img_url = await img_el.get_attribute("src")

        # Categories
        dom_categories: List[str] = []
        cat_els = await page.query_selector_all("span.posted_in a")
        if cat_els:
            dom_categories = [(await c.inner_text()).strip() for c in cat_els if (await c.inner_text())]

        # Merge + normalize
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
            img = json_data["image"]
            images = img if isinstance(img, list) else [img]
        elif img_url:
            images = [img_url]

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
            "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": generate_dedupe_key("mh", url),
        }

    except Exception as e:
        logger.error(f"Extract failed: {url} :: {e}")
        return None

# -----------------------
# WORKERS
# -----------------------
async def worker(sem: asyncio.Semaphore, context, url: str) -> None:
    async with sem:
        page = await context.new_page()
        try:
            data = await extract_product_data(page, url)
            if data:
                await supabase_upsert("raw_products", data)
                logger.info(f"Saved: {data['name'][:40]}... ({data.get('price_numeric')})")
            else:
                logger.warning(f"Skipped: {url}")
        finally:
            await page.close()

# -----------------------
# MAIN
# -----------------------
async def main() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

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

        # 1) Discover categories
        page = await context.new_page()
        categories = await discover_categories(page)

        # 2) Crawl categories -> product urls
        master_urls: Set[str] = set()

        # Also crawl shop-wide pagination as a safety net (covers items missing from category lists)
        shop_urls = await crawl_paginated_listing(page, SHOP_URL)
        master_urls |= set(shop_urls)
        logger.info(f"After /shop crawl, master_urls={len(master_urls)}")

        for cat in categories:
            cat_urls = await crawl_paginated_listing(page, cat)
            master_urls |= set(cat_urls)
            logger.info(f"After category, master_urls={len(master_urls)}")

        await page.close()

        logger.info(f"Total unique product URLs discovered: {len(master_urls)}")

        # 3) Extract + save concurrently
        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [worker(sem, context, u) for u in sorted(master_urls)]
        await asyncio.gather(*tasks)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
