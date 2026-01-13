import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List, Set
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

import httpx
from playwright.async_api import async_playwright, Page
from tenacity import retry, stop_after_attempt, wait_exponential

# ---------------------------------------------------
# CONFIGURATION (NEWWORLD)
# ---------------------------------------------------

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

HEADLESS = True

# New World is JS-heavy; start conservative and raise later
CONCURRENCY = int(os.getenv("CONCURRENCY", "3"))

BASE_URL = "https://www.newworld.com.fj"
HOME_URL = f"{BASE_URL}/"
# Category listing URLs look like: /category/<id>?skip=0&take=40...
CATEGORY_URL_PREFIX = f"{BASE_URL}/category/"
DEFAULT_TAKE = int(os.getenv("TAKE", "40"))

# New World pricing/content is store-dependent (query param `store=`).
# Pick a default store slug/id that you want to crawl.
# You can override in GitHub Actions env: NEWWORLD_STORE=...
DEFAULT_STORE = os.getenv("NEWWORLD_STORE", "newworld-suva-damodar-city-id-S0017")

# Safety brakes
MAX_CATEGORY_PAGES = int(os.getenv("MAX_CATEGORY_PAGES", "500"))

# ---------------------------------------------------
# LOGGING
# ---------------------------------------------------

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# ---------------------------------------------------
# SUPABASE
# ---------------------------------------------------

if not SUPABASE_URL or not SUPABASE_KEY:
    logger.warning("SUPABASE_URL or SUPABASE_KEY not set. Inserts will fail.")

HEADERS = {
    "apikey": SUPABASE_KEY or "",
    "Authorization": f"Bearer {SUPABASE_KEY}" if SUPABASE_KEY else "",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

def normalize_price(text: Optional[str]) -> Optional[float]:
    if not text:
        return None
    # Examples observed: "FJD$680", "$6.80", "FJD$ 6.80"
    clean = re.sub(r"[^\d.]", "", text)
    try:
        return float(clean)
    except ValueError:
        return None

def generate_dedupe_key(source: str, url: str) -> str:
    raw = f"{source}|{url}".lower().strip()
    return hashlib.sha256(raw.encode()).hexdigest()

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=12))
async def supabase_upsert(table: str, payload: Dict):
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.post(
            f"{SUPABASE_URL}/rest/v1/{table}",
            headers=HEADERS,
            json=payload,
        )
        if r.status_code not in (200, 201, 204):
            logger.error(f"Supabase Error {r.status_code}: {r.text}")
        r.raise_for_status()

# ---------------------------------------------------
# URL HELPERS
# ---------------------------------------------------

def ensure_store_param(url: str, store: str) -> str:
    """Ensure product URL has ?store=... (New World is store-specific)."""
    try:
        u = urlparse(url)
        q = parse_qs(u.query)
        if "store" not in q or not q["store"]:
            q["store"] = [store]
        new_q = urlencode({k: v[0] for k, v in q.items()}, doseq=False)
        return urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))
    except Exception:
        # Fallback: naive append
        if "store=" in url:
            return url
        joiner = "&" if "?" in url else "?"
        return f"{url}{joiner}store={store}"

def normalize_abs_url(href: str) -> Optional[str]:
    if not href:
        return None
    if href.startswith("http://") or href.startswith("https://"):
        return href
    if href.startswith("/"):
        return f"{BASE_URL}{href}"
    return f"{BASE_URL}/{href.lstrip('/')}"

# ---------------------------------------------------
# CATEGORY DISCOVERY
# ---------------------------------------------------

async def discover_categories(page: Page) -> List[str]:
    """
    New World is a SPA. Categories are typically linked as /category/<id>...
    We discover them by reading the rendered DOM after JS loads.
    """
    logger.info("Discovering New World categories from home page...")
    await page.goto(HOME_URL, wait_until="domcontentloaded", timeout=90000)

    # Give SPA a moment to render menu
    await page.wait_for_timeout(3000)

    links = await page.evaluate("""() => {
        const anchors = Array.from(document.querySelectorAll('a[href*="/category/"]'));
        return anchors.map(a => a.href);
    }""")

    # Filter to canonical category pages and strip noisy params except skip/take/search/discount/mixAndMatch
    cats: Set[str] = set()
    for url in links:
        if not url:
            continue
        if "/category/" not in url:
            continue
        absu = normalize_abs_url(url)
        if not absu:
            continue
        # Keep URL as-is; pagination function will manage skip/take
        cats.add(absu)

    categories = sorted(cats)
    logger.info(f"Discovered {len(categories)} categories")
    return categories

# ---------------------------------------------------
# CATEGORY PAGINATION (skip/take)
# ---------------------------------------------------

async def crawl_category_pagination(page: Page, category_url: str) -> List[str]:
    """
    New World category pages use skip/take query params.
    Example from the site: /category/1154?skip=0&take=40&search=&discount=false&mixAndMatch=false
    We force pagination by incrementing skip until no products appear.
    """
    logger.info(f"Crawling category (skip/take): {category_url}")

    product_urls: Set[str] = set()

    # Parse base + existing params, then override skip/take.
    u = urlparse(category_url)
    params = parse_qs(u.query)
    # Ensure required params exist (site accepts empty strings for some)
    params.setdefault("discount", ["false"])
    params.setdefault("mixAndMatch", ["false"])
    params.setdefault("search", [""])
    take = int(params.get("take", [str(DEFAULT_TAKE)])[0] or DEFAULT_TAKE)

    for page_idx in range(MAX_CATEGORY_PAGES):
        skip = page_idx * take
        params["skip"] = [str(skip)]
        params["take"] = [str(take)]
        new_q = urlencode({k: v[0] for k, v in params.items()}, doseq=False)
        url = urlunparse((u.scheme, u.netloc, u.path, u.params, new_q, u.fragment))

        logger.info(f"Loading category page {page_idx + 1} (skip={skip}, take={take}): {url}")
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(1500)

        # Product cards typically link to /product/<id>
        hrefs: List[str] = await page.evaluate("""() => {
            const anchors = Array.from(document.querySelectorAll('a[href*="/product/"]'));
            return anchors.map(a => a.getAttribute('href'));
        }""")

        hrefs = [h for h in hrefs if h]
        if not hrefs:
            logger.info(f"No product links found at skip={skip}. Stopping pagination.")
            break

        before = len(product_urls)
        for h in hrefs:
            absu = normalize_abs_url(h)
            if absu:
                product_urls.add(absu)

        logger.info(f"Category page {page_idx + 1}: +{len(product_urls) - before} products (total {len(product_urls)})")

        # Stop condition: if we didn't discover anything new, likely end of list
        if len(product_urls) == before:
            logger.info("No new products discovered on this page. Stopping pagination.")
            break

    return list(product_urls)

# ---------------------------------------------------
# PRODUCT EXTRACTION
# ---------------------------------------------------

async def extract_product_data(page: Page, url: str) -> Optional[Dict]:
    """
    Extract from rendered product page. Prefer JSON-LD if present,
    otherwise use DOM fallbacks and regex for price.
    """
    try:
        url = ensure_store_param(url, DEFAULT_STORE)
        await page.goto(url, wait_until="domcontentloaded", timeout=90000)
        await page.wait_for_timeout(2000)

        html = await page.content()

        # ---- JSON-LD FIRST ----
        json_product: Dict = {}
        scripts = await page.locator("script[type='application/ld+json']").all_inner_texts()
        for script in scripts:
            try:
                data = json.loads(script)
                nodes = data.get("@graph", [data]) if isinstance(data, dict) else data
                if isinstance(nodes, list):
                    for node in nodes:
                        if isinstance(node, dict) and node.get("@type") == "Product":
                            json_product = node
                            break
                elif isinstance(nodes, dict) and nodes.get("@type") == "Product":
                    json_product = nodes
                if json_product:
                    break
            except Exception:
                continue

        # ---- DOM FALLBACKS ----
        dom_name = None
        dom_price_text = None
        dom_img = None
        dom_categories: List[str] = []

        # Title/name: try common patterns; fallback to document title
        for sel in ["h1", "[data-testid='product-title']", ".product-title", "header h1"]:
            el = await page.query_selector(sel)
            if el:
                txt = (await el.inner_text()).strip()
                if txt:
                    dom_name = txt
                    break
        if not dom_name:
            dom_name = (await page.title()).strip()

        # Price: try multiple selectors and then regex from full text
        price_selectors = [
            "[data-testid='product-price']",
            ".product-price",
            "span:has-text('FJD$')",
            "div:has-text('FJD$')",
        ]
        for sel in price_selectors:
            el = await page.query_selector(sel)
            if el:
                t = (await el.inner_text()).strip()
                if "FJD" in t or "$" in t:
                    dom_price_text = t
                    break
        if not dom_price_text:
            body_text = (await page.inner_text("body"))[:5000]
            m = re.search(r"FJD\\$\\s*\\d+(?:\\.\\d+)?", body_text)
            if not m:
                m = re.search(r"\\$\\s*\\d+(?:\\.\\d+)?", body_text)
            if m:
                dom_price_text = m.group(0)

        # Image: pick first likely product image
        for sel in ["img[src*='product']", "img[alt]", "img"]:
            el = await page.query_selector(sel)
            if el:
                src = await el.get_attribute("src")
                if src and (src.startswith("http") or src.startswith("/")):
                    dom_img = normalize_abs_url(src)
                    break

        # Categories: breadcrumbs often present
        crumb_links = await page.query_selector_all("a[href*='/category/']")
        for c in crumb_links[:10]:
            t = (await c.inner_text()).strip()
            if t and t.lower() not in ("home",):
                dom_categories.append(t)
        dom_categories = list(dict.fromkeys(dom_categories))  # de-dupe preserve order

        # ---- NORMALIZE ----
        final_name = (json_product.get("name") or dom_name or "").strip()
        if not final_name:
            return None

        offers = json_product.get("offers", {})
        if isinstance(offers, list) and offers:
            offers = offers[0]
        final_price_raw = None
        final_currency = "FJD"

        if isinstance(offers, dict) and offers.get("price") is not None:
            final_price_raw = str(offers.get("price"))
            final_currency = offers.get("priceCurrency", "FJD") or "FJD"
        else:
            final_price_raw = dom_price_text

        price_numeric = normalize_price(str(final_price_raw) if final_price_raw is not None else None)

        final_brand = "New World"
        b = json_product.get("brand")
        if isinstance(b, dict) and b.get("name"):
            final_brand = b["name"]
        elif isinstance(b, str) and b.strip():
            final_brand = b.strip()

        images: List[str] = []
        img = json_product.get("image")
        if isinstance(img, list):
            images = [i for i in img if isinstance(i, str)]
        elif isinstance(img, str):
            images = [img]
        elif dom_img:
            images = [dom_img]

        # ensure abs urls for images
        images = [normalize_abs_url(i) or i for i in images]

        # Ensure product_url includes store param for consistent pricing
        product_url = ensure_store_param(url, DEFAULT_STORE)

        return {
            "source": "newworld",
            "product_url": product_url,
            "name": final_name,
            "brand": final_brand,
            "price_display": str(final_price_raw) if final_price_raw is not None else None,
            "price_numeric": price_numeric,
            "currency": final_currency,
            "categories": dom_categories,
            "images": images,
            "raw_html": html,
            "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
            "dedupe_key": generate_dedupe_key("newworld", product_url),
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
                logger.info(f"Saved: {data['name'][:50]} ({data['price_numeric']})")
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
            viewport={"width": 1280, "height": 720},
        )

        # 1) Discover categories
        page = await context.new_page()
        categories = await discover_categories(page)
        await page.close()

        # Fallback: if DOM discovery fails, attempt to seed a few categories from observed pattern
        if not categories:
            logger.warning("No categories discovered from DOM. Using fallback seed category (1154).")
            categories = [f"{BASE_URL}/category/1154?discount=false&mixAndMatch=false&search=&skip=0&take={DEFAULT_TAKE}"]

        # 2) Crawl categories for product URLs
        master_urls: Set[str] = set()
        for cat in categories:
            cat_page = await context.new_page()
            try:
                urls = await crawl_category_pagination(cat_page, cat)
                master_urls.update(urls)
            finally:
                await cat_page.close()

        logger.info(f"Total unique products found (before store param): {len(master_urls)}")
        logger.info(f"Starting extraction for {len(master_urls)} product URLs using store={DEFAULT_STORE}")

        # 3) Extract products concurrently and save to Supabase
        sem = asyncio.Semaphore(CONCURRENCY)
        tasks = [worker(sem, context, u) for u in master_urls]
        await asyncio.gather(*tasks)

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
