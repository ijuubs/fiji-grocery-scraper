import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Set, Any
from urllib.parse import urljoin, urlparse, urlencode, parse_qs, urlunparse

import httpx
from playwright.async_api import async_playwright, Page, BrowserContext, Response
from tenacity import retry, stop_after_attempt, wait_exponential

print("BOOT: newworld_scraper.py started", flush=True)

# ------------------------------------------------------------
# CONFIG
# ------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADLESS = True
CONCURRENCY = int(os.getenv("CONCURRENCY", "3"))

BASE_URL = "https://www.newworld.com.fj"
HOME_URL = f"{BASE_URL}/"

# Store selection is critical on New World (catalog/prices depend on it)
DEFAULT_STORE = os.getenv("NEWWORLD_STORE", "newworld-suva-damodar-city-id-S0017")

# Fallback category IDs if DOM discovery returns 0
# You can override with: NEWWORLD_CATEGORY_IDS="1154,1234,..." (comma-separated)
FALLBACK_CATEGORY_IDS = [1154]

# Pagination
TAKE = int(os.getenv("NEWWORLD_TAKE", "40"))
MAX_PAGES_PER_CATEGORY = int(os.getenv("NEWWORLD_MAX_PAGES_PER_CATEGORY", "250"))

# ------------------------------------------------------------
# LOGGING
# ------------------------------------------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
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
    if text is None:
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

# ------------------------------------------------------------
# URL HELPERS
# ------------------------------------------------------------
def with_store(url: str, store: str) -> str:
    parts = list(urlparse(url))
    qs = parse_qs(parts[4])
    qs["store"] = [store]
    parts[4] = urlencode(qs, doseq=True)
    return urlunparse(parts)

def canonicalize(url: str) -> str:
    # Keep query (store matters), drop fragment
    parts = list(urlparse(url))
    parts[5] = ""
    return urlunparse(parts)

def category_page_url(category_url: str, skip: int, take: int) -> str:
    # Merge/override skip/take/search flags
    parts = list(urlparse(category_url))
    qs = parse_qs(parts[4])
    qs["skip"] = [str(skip)]
    qs["take"] = [str(take)]
    qs.setdefault("discount", ["false"])
    qs.setdefault("mixAndMatch", ["false"])
    qs.setdefault("search", [""])
    parts[4] = urlencode(qs, doseq=True)
    return urlunparse(parts)

# ------------------------------------------------------------
# NETWORK (XHR) CAPTURE FALLBACK
# ------------------------------------------------------------
def _extract_product_urls_from_json(obj: Any) -> Set[str]:
    """
    Best-effort recursive scan for product URLs/paths inside JSON responses.
    This handles cases where products render via XHR and DOM anchors are missing.
    """
    found: Set[str] = set()

    def walk(x: Any):
        if isinstance(x, dict):
            # Common shapes: { products: [...] } or { items: [...] }
            for k, v in x.items():
                # direct url fields
                if isinstance(v, str):
                    if "/product/" in v:
                        u = v
                        if u.startswith("/"):
                            u = urljoin(BASE_URL, u)
                        if u.startswith("http"):
                            found.add(canonicalize(u))
                else:
                    walk(v)
        elif isinstance(x, list):
            for it in x:
                walk(it)

    walk(obj)
    return found

async def _response_listener(response: Response, bucket: Set[str]) -> None:
    """
    Capture product URLs from XHR/JSON responses.
    """
    try:
        ct = (response.headers.get("content-type") or "").lower()
        url = response.url
        # Only bother with likely data endpoints
        if "application/json" not in ct and "json" not in ct:
            return
        if "category" not in url and "product" not in url and "search" not in url and "api" not in url:
            # still could be, but avoid noise
            return
        data = await response.json()
        urls = _extract_product_urls_from_json(data)
        if urls:
            before = len(bucket)
            bucket.update(urls)
            if len(bucket) != before:
                logger.info(f"XHR capture: +{len(bucket)-before} product URLs (total {len(bucket)})")
    except Exception:
        return

# ------------------------------------------------------------
# DISCOVERY
# ------------------------------------------------------------
async def discover_categories(page: Page) -> List[str]:
    """
    Try to discover category URLs from DOM.
    If none found (SPA), fallback to seed IDs.
    """
    logger.info("Discovering New World categories...")
    categories: List[str] = []
    try:
        await page.goto(with_store(HOME_URL, DEFAULT_STORE), wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1500)

        links = await page.evaluate("""() => {
          const a = Array.from(document.querySelectorAll('a[href*="/category/"]'));
          return a.map(x => x.href);
        }""")

        categories = sorted({
            canonicalize(u) for u in links
            if isinstance(u, str) and u.startswith("http") and re.search(r"/category/\\d+", u)
        })

        logger.info(f"Discovered {len(categories)} categories from DOM")
    except Exception as e:
        logger.warning(f"Category discovery error (DOM): {e}")

    if categories:
        return categories

    env_ids = os.getenv("NEWWORLD_CATEGORY_IDS", "").strip()
    ids: List[int] = []
    if env_ids:
        for part in env_ids.split(","):
            part = part.strip()
            if part.isdigit():
                ids.append(int(part))
    if not ids:
        ids = FALLBACK_CATEGORY_IDS

    logger.warning(f"No categories discovered from DOM. Using fallback seed category IDs: {ids}")
    return [with_store(f"{BASE_URL}/category/{cid}", DEFAULT_STORE) for cid in ids]

# ------------------------------------------------------------
# LISTING EXTRACTION (DOM + REGEX)
# ------------------------------------------------------------
async def extract_product_links_from_page(page: Page) -> Set[str]:
    found: Set[str] = set()

    # DOM anchors
    try:
        anchors = await page.evaluate("""() => {
          const a = Array.from(document.querySelectorAll('a[href*="/product/"]'));
          return a.map(x => x.href);
        }""")
        for u in anchors:
            if isinstance(u, str) and "/product/" in u:
                found.add(canonicalize(u))
    except Exception:
        pass

    # Regex fallback on HTML
    try:
        html = await page.content()
        for m in re.findall(r'href="([^"]*?/product/[^"]+)"', html, flags=re.IGNORECASE):
            u = m
            if u.startswith("/"):
                u = urljoin(BASE_URL, u)
            if u.startswith("http") and "/product/" in u:
                found.add(canonicalize(u))
    except Exception:
        pass

    return found

async def crawl_category_skip_take(context: BrowserContext, category_url: str) -> List[str]:
    """
    Crawl category listing pages using skip/take.
    Uses:
      - DOM+regex extraction
      - XHR capture fallback (response listener)
    """
    product_urls: Set[str] = set()
    xhr_bucket: Set[str] = set()

    page = await context.new_page()
    try:
        page.on("response", lambda r: asyncio.create_task(_response_listener(r, xhr_bucket)))

        empty_pages = 0
        for idx in range(MAX_PAGES_PER_CATEGORY):
            skip = idx * TAKE
            url = category_page_url(with_store(category_url, DEFAULT_STORE), skip=skip, take=TAKE)

            logger.info(f"[Category] page={idx+1} skip={skip} take={TAKE} :: {url}")
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2000)  # give SPA/XHR time

            links = await extract_product_links_from_page(page)

            # merge what we saw from XHR so far too
            combined = set(links) | set(xhr_bucket)

            if not combined:
                empty_pages += 1
                logger.info(f"No products found at skip={skip}. empty_pages={empty_pages}")
                if empty_pages >= 2:
                    logger.info("Stopping pagination due to consecutive empty pages.")
                    break
                continue

            empty_pages = 0
            before = len(product_urls)
            product_urls.update(combined)
            logger.info(f"Collected +{len(product_urls)-before} (total {len(product_urls)}) product URLs")

            # Heuristic: if we only got a tiny batch, likely end
            if len(combined) < max(10, TAKE // 4):
                logger.info("Heuristic end: small batch, stopping category pagination.")
                break

    finally:
        await page.close()

    return list(product_urls)

# ------------------------------------------------------------
# PRODUCT DETAIL EXTRACTION
# ------------------------------------------------------------
async def extract_product_data(page: Page, product_url: str) -> Optional[Dict[str, Any]]:
    url = with_store(product_url, DEFAULT_STORE)
    try:
        await page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await page.wait_for_timeout(1500)

        html = await page.content()

        # JSON-LD first
        json_product: Dict[str, Any] = {}
        scripts = await page.locator("script[type='application/ld+json']").all_inner_texts()
        for s in scripts:
            try:
                data = json.loads(s)
                nodes = data.get("@graph", [data]) if isinstance(data, dict) else [data]
                for node in nodes:
                    if isinstance(node, dict) and node.get("@type") == "Product":
                        json_product = node
                        break
                if json_product:
                    break
            except Exception:
                continue

        name = None
        price_raw = None
        currency = "FJD"
        images: List[str] = []
        categories: List[str] = []

        # DOM name fallback
        h1 = await page.query_selector("h1")
        if h1:
            t = (await h1.inner_text()).strip()
            if t:
                name = t

        # DOM price fallback (broad)
        price_selectors = [
            "[data-testid='product-price']",
            ".price",
            ".product-price",
            "span[class*='price']",
        ]
        for sel in price_selectors:
            el = await page.query_selector(sel)
            if el:
                t = (await el.inner_text()).strip()
                if t and re.search(r"\d", t):
                    price_raw = t
                    break

        # DOM images
        img_els = await page.query_selector_all("img")
        for img in img_els[:25]:
            src = await img.get_attribute("src")
            if src and src.startswith("http") and "/product" in src or "cdn" in src:
                images.append(src)
        images = list(dict.fromkeys(images))[:8]

        # Breadcrumb categories
        crumb = await page.query_selector_all("a[href*='/category/']")
        for c in crumb[:15]:
            t = (await c.inner_text()).strip()
            if t:
                categories.append(t)
        categories = list(dict.fromkeys(categories))[:12]

        # Merge JSON-LD when present
        if json_product:
            name = json_product.get("name") or name
            offers = json_product.get("offers", {})
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                if offers.get("price") is not None:
                    price_raw = str(offers.get("price"))
                currency = offers.get("priceCurrency", currency)
            img = json_product.get("image")
            if img:
                images = img if isinstance(img, list) else [img]

        if not name:
            return None

        payload = {
            "source": "newworld",
            "product_url": canonicalize(url),
            "name": name,
            "brand": "New World",
            "price_display": str(price_raw) if price_raw is not None else None,
            "price_numeric": normalize_price(price_raw),
            "currency": currency,
            "categories": categories,
            "images": images,
            "raw_html": html,
            "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": generate_dedupe_key("newworld", canonicalize(url)),
        }
        return payload
    except Exception as e:
        logger.error(f"Failed product scrape: {url} :: {e}")
        return None

async def worker(sem: asyncio.Semaphore, context: BrowserContext, url: str) -> None:
    async with sem:
        page = await context.new_page()
        try:
            data = await extract_product_data(page, url)
            if data:
                await supabase_upsert("raw_products", data)
                logger.info(f"Saved: {data['name'][:60]} ({data.get('price_numeric')})")
        finally:
            await page.close()

# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
async def main() -> None:
    logger.info("Starting New World scraper main()")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY environment variables")

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

        # ✅ Correct Playwright Python usage (single script string; no JS args)
        await context.add_init_script(script=f"""
(() => {{
  const store = {json.dumps(DEFAULT_STORE)};
  try {{
    localStorage.setItem('store', store);
    localStorage.setItem('selectedStore', store);
    localStorage.setItem('selected_store', store);
    localStorage.setItem('currentStore', store);
  }} catch (e) {{}}
}})();
""")

        # Warm-up
        warm = await context.new_page()
        try:
            await warm.goto(with_store(HOME_URL, DEFAULT_STORE), wait_until="domcontentloaded", timeout=60000)
            await warm.wait_for_timeout(1500)
        finally:
            await warm.close()

        # Discover categories
        page = await context.new_page()
        try:
            categories = await discover_categories(page)
        finally:
            await page.close()

        logger.info(f"Categories to crawl: {len(categories)}")

        master_urls: Set[str] = set()
        for cat in categories:
            try:
                urls = await crawl_category_skip_take(context, cat)
                master_urls.update(urls)
                logger.info(f"After category, total unique product URLs = {len(master_urls)}")
            except Exception as e:
                logger.error(f"Category crawl failed: {cat} :: {e}")

        logger.info(f"Total unique product URLs discovered: {len(master_urls)}")
        logger.info(f"Starting extraction with store={DEFAULT_STORE} and concurrency={CONCURRENCY}")

        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [worker(sem, context, u) for u in master_urls]
        await asyncio.gather(*tasks)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())# DISCOVERY
# ------------------------------------------------------------

async def discover_categories(page: Page) -> List[str]:
    """
    New World is SPA-ish; categories may not exist as direct links in DOM.
    We try best-effort DOM discovery; otherwise fallback to seed IDs.
    """
    logger.info("Discovering New World categories from home page...")
    categories: List[str] = []
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
    except Exception as e:
        logger.warning(f"Category discovery error (DOM): {e}")

    # Fallback if nothing found
    if not categories:
        env_ids = os.getenv("NEWWORLD_CATEGORY_IDS", "").strip()
        ids: List[int] = []
        if env_ids:
            for part in env_ids.split(","):
                part = part.strip()
                if part.isdigit():
                    ids.append(int(part))
        if not ids:
            ids = FALLBACK_CATEGORY_IDS

        logger.warning(f"No categories discovered from DOM. Using fallback seed category IDs: {ids}")
        for cid in ids:
            categories.append(with_store(f"{BASE_URL}/category/{cid}", DEFAULT_STORE))

    return categories
