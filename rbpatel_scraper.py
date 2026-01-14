import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any

import httpx
from playwright.async_api import async_playwright, Page
from tenacity import retry, stop_after_attempt, wait_exponential

print("BOOT: rbpatel_scraper.py started", flush=True)

# -----------------------
# CONFIG
# -----------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADLESS = True
CONCURRENCY = int(os.getenv("CONCURRENCY", "5"))

# You can change this to start from another category root if needed
START_CATEGORY = os.getenv(
    "RBPATEL_START_CATEGORY",
    "https://www.rbpatel.com.fj/product-category/grocery-shopping/"
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("rbpatel")

RAW_HEADERS = {
    "apikey": SUPABASE_KEY or "",
    "Authorization": f"Bearer {SUPABASE_KEY or ''}",
    "Content-Type": "application/json",
    # ✅ Upsert behavior for raw_products
    "Prefer": "resolution=merge-duplicates",
}

HISTORY_HEADERS = {
    "apikey": SUPABASE_KEY or "",
    "Authorization": f"Bearer {SUPABASE_KEY or ''}",
    "Content-Type": "application/json",
    # ✅ NO Prefer header here: we want a time series (multiple rows)
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

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def supabase_upsert_raw(payload: Dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/raw_products",
            headers=RAW_HEADERS,
            json=payload,
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f"Supabase raw_products error {r.status_code}: {r.text}")
        r.raise_for_status()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def supabase_insert_history(payload: Dict[str, Any]) -> None:
    async with httpx.AsyncClient(timeout=25) as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/price_history",
            headers=HISTORY_HEADERS,
            json=payload,
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f"Supabase price_history error {r.status_code}: {r.text}")
        r.raise_for_status()

# -----------------------
# EXTRACTION
# -----------------------
async def extract_product_data(page: Page, url: str) -> Optional[Dict[str, Any]]:
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Basic sanity: WooCommerce product container
        try:
            await page.wait_for_selector(".product", timeout=6000)
        except Exception:
            logger.warning(f"Product container not found: {url}")
            return None

        html = await page.content()

        # JSON-LD extraction (WooCommerce/Yoast often uses @graph)
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

        dom_price = None
        price_el = await page.query_selector("p.price ins span.woocommerce-Price-amount bdi")
        if not price_el:
            price_el = await page.query_selector("p.price span.woocommerce-Price-amount bdi")
        if price_el:
            dom_price = (await price_el.inner_text()).strip()

        dom_img = None
        img_el = await page.query_selector(".woocommerce-product-gallery__image img")
        if img_el:
            dom_img = await img_el.get_attribute("src")

        dom_categories: List[str] = []
        cat_els = await page.query_selector_all("span.posted_in a")
        if cat_els:
            dom_categories = [(await c.inner_text()).strip() for c in cat_els]

        # Merge/normalize
        final_name = (json_data.get("name") or dom_name or "").strip()

        offers = json_data.get("offers", {})
        if isinstance(offers, list) and offers:
            offers = offers[0]

        final_price_raw = dom_price
        currency = "FJD"
        if isinstance(offers, dict):
            if offers.get("price") is not None:
                final_price_raw = str(offers.get("price"))
            currency = offers.get("priceCurrency") or currency

        price_numeric = normalize_price(final_price_raw)

        brand = "RB Patel"
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
        elif dom_img:
            images = [dom_img]

        ts = datetime.now(timezone.utc).isoformat()
        dedupe = generate_dedupe_key("rbpatel", url)

        return {
            "source": "rbpatel",
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
        logger.error(f"Failed to extract {url}: {e}")
        return None

# -----------------------
# CRAWL (categories + pagination)
# -----------------------
async def discover_categories(page: Page) -> List[str]:
    """
    Discover all category URLs from /shop/ page (WooCommerce typical).
    """
    shop_root = "https://www.rbpatel.com.fj/shop/"
    logger.info(f"Discovering categories from {shop_root}")

    await page.goto(shop_root, wait_until="domcontentloaded", timeout=60000)
    links = await page.evaluate("""() => {
      const a = Array.from(document.querySelectorAll('a[href*="/product-category/"]'));
      return a.map(x => x.href).filter(Boolean);
    }""")
    unique = sorted(set([u.split("#")[0] for u in links if isinstance(u, str) and "/product-category/" in u]))
    logger.info(f"Discovered {len(unique)} categories")
    return unique

async def crawl_category_pagination(page: Page, category_url: str, max_pages: int = 200) -> List[str]:
    """
    Crawl a category with hardened "next" logic.
    """
    logger.info(f"Crawling category: {category_url}")
    await page.goto(category_url, wait_until="domcontentloaded", timeout=60000)

    product_urls: Set[str] = set()
    visited_pages: Set[str] = set()
    current = page.url

    for n in range(1, max_pages + 1):
        visited_pages.add(current)

        # Collect product links
        hrefs = await page.evaluate("""() => {
          const out = new Set();
          document.querySelectorAll('a.woocommerce-LoopProduct-link, a.woocommerce-loop-product__link').forEach(a => {
            if (a && a.href) out.add(a.href);
          });
          return Array.from(out);
        }""")
        before = len(product_urls)
        for u in hrefs:
            if isinstance(u, str) and "/product/" in u:
                product_urls.add(u.split("#")[0])

        logger.info(f"Category page {n}: +{len(product_urls)-before} products (total={len(product_urls)})")

        # Find next
        next_href = await page.evaluate("""() => {
          const a = document.querySelector('a.next.page-numbers') || document.querySelector("a[rel='next']");
          return a ? a.href : null;
        }""")

        if not next_href:
            logger.info("No next page link found; category done.")
            break

        if next_href in visited_pages or next_href == current:
            logger.info("Pagination loop detected; stopping.")
            break

        await page.goto(next_href, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(800)
        current = page.url

    return sorted(product_urls)

# -----------------------
# WORKERS
# -----------------------
async def worker(sem: asyncio.Semaphore, context, url: str) -> None:
    async with sem:
        page = await context.new_page()
        try:
            data = await extract_product_data(page, url)
            if not data:
                logger.warning(f"Skipped (no data): {url}")
                return

            # 1) Upsert raw snapshot
            await supabase_upsert_raw(data)

            # 2) Insert price history row (time series)
            await supabase_insert_history({
                "dedupe_key": data["dedupe_key"],
                "source": data["source"],
                "product_url": data["product_url"],
                "name": data["name"],
                "price_numeric": data["price_numeric"],
                "currency": data["currency"],
                "seen_at": data["scrape_timestamp"],
            })

            logger.info(f"Saved: {data['name'][:50]}... ({data.get('price_numeric')})")

        finally:
            await page.close()

# -----------------------
# MAIN
# -----------------------
async def main() -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY env vars")

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

        page = await context.new_page()

        # Categories: discover all; if that fails, use START_CATEGORY as fallback
        try:
            categories = await discover_categories(page)
        except Exception:
            categories = []

        if not categories:
            logger.warning(f"No categories discovered; falling back to START_CATEGORY={START_CATEGORY}")
            categories = [START_CATEGORY]

        # Crawl all categories -> product URLs
        master_urls: Set[str] = set()
        for cat in categories:
            try:
                urls = await crawl_category_pagination(page, cat, max_pages=200)
                master_urls.update(urls)
                logger.info(f"After category, total unique products={len(master_urls)}")
            except Exception as e:
                logger.error(f"Category crawl failed: {cat} :: {e}")

        await page.close()

        logger.info(f"Total unique product URLs discovered: {len(master_urls)}")

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [worker(sem, context, u) for u in sorted(master_urls)]
        await asyncio.gather(*tasks)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
