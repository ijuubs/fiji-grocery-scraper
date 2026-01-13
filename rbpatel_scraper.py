import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Set

import httpx
from playwright.async_api import async_playwright, Page
from tenacity import retry, stop_after_attempt, wait_exponential

# ---------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADLESS = True
CONCURRENCY = 3  # safer for RB Patel
BASE_URL = "https://www.rbpatel.com.fj"
SHOP_ROOT = "https://www.rbpatel.com.fj/shop/"

# ---------------------------------------------------
# LOGGING
# ---------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# SUPABASE
# ---------------------------------------------------

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates"
}

def normalize_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    clean = re.sub(r"[^\d.]", "", text)
    try:
        return float(clean)
    except ValueError:
        return None

def generate_dedupe_key(source: str, url: str) -> str:
    raw = f"{source}|{url}".lower().strip()
    return hashlib.sha256(raw.encode()).hexdigest()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def supabase_upsert(table: str, payload: Dict):
    async with httpx.AsyncClient(timeout=15) as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=HEADERS,
            json=payload
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f"Supabase Error {r.status_code}: {r.text}")
        r.raise_for_status()

# ---------------------------------------------------
# CATEGORY DISCOVERY
# ---------------------------------------------------

async def discover_categories(page: Page) -> List[str]:
    logger.info("Discovering product categories...")
    await page.goto(SHOP_ROOT, wait_until="domcontentloaded", timeout=60000)

    links = await page.evaluate("""() => {
        return Array.from(document.querySelectorAll('a[href*="/product-category/"]'))
            .map(a => a.href);
    }""")

    # FILTER FIX: remove parent/index category junk
    categories = sorted({
        url for url in links
        if "/product-category/" in url
        and not url.endswith("/product-category/")
    })

    logger.info(f"Discovered {len(categories)} categories")
    return categories

# ---------------------------------------------------
# CATEGORY PAGINATION
# ---------------------------------------------------

async def crawl_category_pagination(page: Page, category_url: str) -> List[str]:
    logger.info(f"Crawling category: {category_url}")
    await page.goto(category_url, wait_until="domcontentloaded", timeout=60000)

    product_urls: Set[str] = set()
    visited_pages: Set[str] = set()
    current_url = page.url

    MAX_PAGES = 500
    page_count = 0

    while page_count < MAX_PAGES:
        page_count += 1
        visited_pages.add(current_url)

        products = await page.query_selector_all("a.woocommerce-LoopProduct-link")
        before = len(product_urls)

        for p in products:
            href = await p.get_attribute("href")
            if href:
                product_urls.add(href)

        logger.info(
            f"Page {page_count}: +{len(product_urls) - before} products "
            f"(total {len(product_urls)})"
        )

        next_btn = await page.query_selector(
            "a.next.page-numbers, a[rel='next']"
        )

        if not next_btn:
            break

        next_href = await next_btn.get_attribute("href")

        if not next_href or next_href in visited_pages or next_href == current_url:
            break

        await page.goto(next_href, wait_until="domcontentloaded", timeout=30000)
        current_url = page.url

    return list(product_urls)

# ---------------------------------------------------
# PRODUCT EXTRACTION
# ---------------------------------------------------

async def extract_product_data(page: Page, url: str) -> Optional[Dict]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        if page.url == BASE_URL or "404" in await page.title():
            return None

        html = await page.content()

        # ---- JSON-LD FIRST ----
        json_data = {}
        scripts = await page.locator(
            "script[type='application/ld+json']"
        ).all_inner_texts()

        for script in scripts:
            try:
                data = json.loads(script)
                nodes = data.get("@graph", [data])
                for node in nodes:
                    if node.get("@type") == "Product":
                        json_data = node
                        break
                if json_data:
                    break
            except Exception:
                continue

        # ---- DOM FALLBACK ----
        dom_name = None
        dom_price = None
        dom_img = None
        dom_categories = []

        h1 = await page.query_selector("h1.product_title")
        if h1:
            dom_name = (await h1.inner_text()).strip()

        price_el = await page.query_selector(
            "p.price span.woocommerce-Price-amount bdi"
        )
        if not price_el:
            price_el = await page.query_selector(
                "p.price ins span.woocommerce-Price-amount bdi"
            )
        if price_el:
            dom_price = await price_el.inner_text()

        img_el = await page.query_selector(
            ".woocommerce-product-gallery__image img"
        )
        if img_el:
            dom_img = await img_el.get_attribute("src")

        cats = await page.query_selector_all("span.posted_in a")
        dom_categories = [(await c.inner_text()).strip() for c in cats]

        # ---- NORMALIZE ----
        final_name = json_data.get("name") or dom_name
        if not final_name:
            return None

        offers = json_data.get("offers", {})
        if isinstance(offers, list):
            offers = offers[0]

        final_price_raw = offers.get("price") or dom_price
        final_currency = offers.get("priceCurrency", "FJD")
        price_numeric = normalize_price(str(final_price_raw))

        final_brand = "RB Patel"
        if json_data.get("brand"):
            b = json_data.get("brand")
            final_brand = b.get("name") if isinstance(b, dict) else b

        images = []
        if json_data.get("image"):
            img = json_data["image"]
            images = img if isinstance(img, list) else [img]
        elif dom_img:
            images = [dom_img]

        return {
            "source": "rbpatel",
            "product_url": url,
            "name": final_name,
            "brand": final_brand,
            "price_display": str(final_price_raw),
            "price_numeric": price_numeric,
            "currency": final_currency,
            "categories": dom_categories,
            "images": images,
            "raw_html": html,
            "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": generate_dedupe_key("rbpatel", url)
        }

    except Exception as e:
        logger.error(f"Failed product scrape {url}: {e}")
        return None

# ---------------------------------------------------
# WORKER
# ---------------------------------------------------

async def worker(sem: asyncio.Semaphore, context, url: str):
    async with sem:
        page = await context.new_page()
        try:
            data = await extract_product_data(page, url)
            if data:
                await supabase_upsert("raw_products", data)
                logger.info(f"Saved: {data['name'][:40]}")
        finally:
            await page.close()

# ---------------------------------------------------
# MAIN
# ---------------------------------------------------

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        context = await browser.new_context(
            user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1280, "height": 720}
        )

        # Discover categories
        page = await context.new_page()
        categories = await discover_categories(page)
        await page.close()

        if not categories:
            logger.error("No categories found. Exiting.")
            await browser.close()
            return

        # Crawl all categories
        master_urls: Set[str] = set()

        for cat in categories:
            cat_page = await context.new_page()
            try:
                urls = await crawl_category_pagination(cat_page, cat)
                master_urls.update(urls)
            finally:
                await cat_page.close()

        logger.info(f"Total unique products found: {len(master_urls)}")

        # Extract products
        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [worker(sem, context, url) for url in master_urls]
        await asyncio.gather(*tasks)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
