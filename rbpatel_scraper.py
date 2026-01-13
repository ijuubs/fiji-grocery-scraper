import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Any

import httpx
from playwright.async_api import async_playwright, Page, TimeoutError as PlaywrightTimeout
from tenacity import retry, stop_after_attempt, wait_exponential

# --- CONFIGURATION ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
HEADLESS = True
CONCURRENCY = 5  # Number of parallel tabs
START_CATEGORY = "https://www.rbpatel.com.fj/product-category/grocery-shopping/"

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - [%(funcName)s] %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates" # Upsert behavior
}

# --- HELPERS ---

def normalize_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    # Remove FJD, $, commas, and whitespace
    clean = re.sub(r"[^\d.]", "", text)
    try:
        return float(clean)
    except ValueError:
        return None

def generate_dedupe_key(source: str, url: str) -> str:
    # URL-based dedupe is safer than Name-based for e-commerce
    # (prevents collision on generic names like "Apples 1kg")
    raw = f"{source}|{url}".lower().strip()
    return hashlib.sha256(raw.encode()).hexdigest()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
async def supabase_upsert(table: str, payload: Dict):
    """Resilient Supabase REST call with retries."""
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=HEADERS,
            json=payload
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f"Supabase Error {r.status_code}: {r.text}")
        r.raise_for_status()

# --- EXTRACTION LOGIC ---

def parse_json_ld(html_content: str) -> Optional[Dict]:
    """Extracts the first valid Product schema from HTML."""
    # Simple regex extraction to avoid loading a heavy HTML parser if possible,
    # but since we have Playwright context, we usually pass extracted text.
    # Here we iterate the list passed from Playwright.
    try:
        # This is a logical placeholder; actual extraction happens in `extract_product`
        # via Playwright selectors to avoid re-parsing HTML string manually.
        pass 
    except Exception:
        return None
    return None

async def extract_product_data(page: Page, url: str) -> Optional[Dict]:
    try:
        # Increase timeout for slow assets
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        
        # Human-like check: Ensure price element is visible or determine OOS
        try:
            await page.wait_for_selector(".product", timeout=5000)
        except:
            logger.warning(f"Product container not found: {url}")
            return None

        html = await page.content()

        # 1. JSON-LD Extraction (Priority)
        # ------------------------------------------------
        json_data = {}
        scripts = await page.locator("script[type='application/ld+json']").all_inner_texts()
        
        for script in scripts:
            try:
                data = json.loads(script)
                # Handle @graph array style (common in WooCommerce)
                nodes = data.get('@graph', [data])
                for node in nodes:
                    if node.get('@type') == 'Product':
                        json_data = node
                        break
                if json_data: break
            except json.JSONDecodeError:
                continue

        # 2. DOM Extraction (Fallback/Augment)
        # ------------------------------------------------
        dom_name = await page.title()
        dom_price = None
        dom_img = None
        dom_categories = []

        # Scrape Name
        h1 = await page.query_selector("h1.product_title")
        if h1: dom_name = await h1.inner_text()

        # Scrape Price (WooCommerce specific)
        price_el = await page.query_selector("p.price span.woocommerce-Price-amount bdi")
        if not price_el:
            # Try sale price location
            price_el = await page.query_selector("p.price ins span.woocommerce-Price-amount bdi")
        
        if price_el:
            dom_price = await price_el.inner_text()

        # Scrape Images
        img_el = await page.query_selector(".woocommerce-product-gallery__image img")
        if img_el:
            dom_img = await img_el.get_attribute("src")

        # Scrape Categories
        cat_els = await page.query_selector_all("span.posted_in a")
        dom_categories = [(await c.inner_text()).strip() for c in cat_els]

        # 3. Merge & Normalize
        # ------------------------------------------------
        
        # Prefer JSON-LD name, fallback to DOM
        final_name = json_data.get('name', dom_name).strip()
        
        # Prefer JSON-LD offers, fallback to DOM
        final_price_raw = dom_price
        final_currency = "FJD" # Default for this site
        
        offers = json_data.get('offers', {})
        if isinstance(offers, list): offers = offers[0] # Grab first offer
        
        if offers.get('price'):
            final_price_raw = str(offers.get('price'))
            final_currency = offers.get('priceCurrency', 'FJD')

        price_numeric = normalize_price(final_price_raw)
        
        # Brand extraction (often missing in basic DOM, check attributes)
        # Check specific table row for Brand
        final_brand = "RB Patel" # Default
        if json_data.get('brand'):
             b = json_data.get('brand')
             final_brand = b.get('name') if isinstance(b, dict) else b
        
        # Images
        final_images = []
        if json_data.get('image'):
            img = json_data.get('image')
            final_images = img if isinstance(img, list) else [img]
        elif dom_img:
            final_images = [dom_img]

        return {
            "source": "rbpatel",
            "product_url": url,
            "name": final_name,
            "brand": final_brand,
            "price_display": final_price_raw,
            "price_numeric": price_numeric,
            "currency": final_currency,
            "categories": dom_categories,
            "images": final_images,
            "raw_html": html,
            "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": generate_dedupe_key("rbpatel", url)
        }

    except Exception as e:
        logger.error(f"Failed to extract {url}: {e}")
        return None

async def worker(sem: asyncio.Semaphore, page: Page, url: str):
    """Concurrency worker to process a single product."""
    async with sem:
        data = await extract_product_data(page, url)
        if data:
            await supabase_upsert("raw_products", data)
            logger.info(f"Saved: {data['name'][:30]}... ({data['price_numeric']})")
        else:
            logger.warning(f"Skipped (Empty Data): {url}")

async def crawl_category_pagination(page: Page, start_url: str) -> List[str]:
    """Iterates pagination to collect all product URLs."""
    logger.info(f"Starting crawl: {start_url}")
    await page.goto(start_url, wait_until="domcontentloaded")
    
    product_urls = set()
    
    while True:
        # Collect URLs on current page
        elements = await page.query_selector_all("a.woocommerce-LoopProduct-link")
        current_batch_count = 0
        for el in elements:
            href = await el.get_attribute("href")
            if href:
                product_urls.add(href)
                current_batch_count += 1
        
        logger.info(f"Found {current_batch_count} products on this page. Total: {len(product_urls)}")

        # Check for Next button
        # Hardened selector: ensure it's not hidden or disabled
        next_btn = await page.query_selector("a.next.page-numbers")
        
        if next_btn:
            try:
                # Scroll into view to avoid obstruction
                await next_btn.scroll_into_view_if_needed()
                await next_btn.click()
                # Wait for staleness or specific network idle
                await page.wait_for_timeout(2000) # Simple debounce
                await page.wait_for_load_state("domcontentloaded")
            except Exception as e:
                logger.warning(f"Pagination error, stopping crawl: {e}")
                break
        else:
            logger.info("No next page found. Crawl complete.")
            break
            
    return list(product_urls)

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=HEADLESS)
        
        # Hardened Context
        context = await browser.new_context(
            user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            viewport={"width": 1280, "height": 720}
        )
        
        # 1. Crawl Phase
        page = await context.new_page()
        all_urls = await crawl_category_pagination(page, START_CATEGORY)
        await page.close()
        
        logger.info(f"Starting extraction for {len(all_urls)} products...")

        # 2. Extraction Phase (Concurrent)
        sem = asyncio.Semaphore(CONCURRENCY)
        
        # Reuse context but create fresh pages in workers is cleaner for state isolation,
        # but for speed, we can reuse one page per worker if careful. 
        # Here we spawn a new page per product to ensure clean DOM state (safer).
        
        tasks = []
        # We need a shared page generator or simply open/close in worker.
        # Opening/closing contexts is heavy, opening/closing pages is lighter.
        
        # Strategy: Create a pool of pages? Or one page per request?
        # One page per request is safest for isolation but slower. 
        # Let's open a new page in the worker.
        
        async def bound_worker(u):
            p = await context.new_page()
            try:
                await worker(sem, p, u)
            finally:
                await p.close()

        for url in all_urls:
            tasks.append(bound_worker(url))
        
        await asyncio.gather(*tasks)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
      
