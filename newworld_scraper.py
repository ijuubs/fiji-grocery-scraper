import asyncio
import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Set, Tuple
from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

import httpx
from playwright.async_api import async_playwright, BrowserContext, Page, Response
from tenacity import retry, stop_after_attempt, wait_exponential

print("BOOT: newworld API-harvester scraper started", flush=True)

# ---------------------------
# CONFIG
# ---------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

BASE_URL = "https://www.newworld.com.fj"
HOME_URL = f"{BASE_URL}/"

HEADLESS = True
CONCURRENCY = int(os.getenv("CONCURRENCY", "3"))
DEFAULT_STORE = os.getenv("NEWWORLD_STORE", "newworld-suva-damodar-city-id-S0017")

# Capture window for initial API discovery (seconds)
DISCOVERY_SECONDS = int(os.getenv("NEWWORLD_DISCOVERY_SECONDS", "12"))

# Pagination knobs (used when we can find list endpoints that accept skip/take)
TAKE = int(os.getenv("NEWWORLD_TAKE", "40"))
MAX_PAGES_PER_CATEGORY = int(os.getenv("NEWWORLD_MAX_PAGES_PER_CATEGORY", "250"))

# ---------------------------
# LOGGING
# ---------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("newworld")

# ---------------------------
# SUPABASE
# ---------------------------
HEADERS = {
    "apikey": SUPABASE_KEY or "",
    "Authorization": f"Bearer {SUPABASE_KEY or ''}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

def normalize_price(value: Optional[Any]) -> Optional[float]:
    if value is None:
        return None
    s = str(value)
    s = re.sub(r"[^\d.]", "", s)
    try:
        return float(s)
    except Exception:
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

# ---------------------------
# URL HELPERS
# ---------------------------
def with_store(url: str, store: str) -> str:
    parts = list(urlparse(url))
    qs = parse_qs(parts[4])
    qs["store"] = [store]
    parts[4] = urlencode(qs, doseq=True)
    return urlunparse(parts)

def strip_fragment(url: str) -> str:
    parts = list(urlparse(url))
    parts[5] = ""
    return urlunparse(parts)

def set_skip_take(url: str, skip: int, take: int) -> str:
    """
    Many New World endpoints accept skip/take. We don't assume path;
    we just inject/override query params when present.
    """
    parts = list(urlparse(url))
    qs = parse_qs(parts[4])
    qs["skip"] = [str(skip)]
    qs["take"] = [str(take)]
    parts[4] = urlencode(qs, doseq=True)
    return urlunparse(parts)

# ---------------------------
# JSON MINING (category/products) – generic heuristics
# ---------------------------
def _walk(obj: Any):
    if isinstance(obj, dict):
        yield obj
        for v in obj.values():
            yield from _walk(v)
    elif isinstance(obj, list):
        for it in obj:
            yield from _walk(it)

def extract_category_candidates(obj: Any) -> Set[Tuple[int, str]]:
    """
    Try to find category objects in JSON:
    - id/categoryId
    - name/categoryName/title
    """
    out: Set[Tuple[int, str]] = set()
    for d in _walk(obj):
        if not isinstance(d, dict):
            continue
        # Common key variants
        cid = d.get("categoryId", d.get("id", d.get("CategoryId")))
        name = d.get("categoryName", d.get("name", d.get("title", d.get("CategoryName"))))
        if isinstance(cid, int) and isinstance(name, str) and 1 <= cid <= 100000 and len(name) >= 2:
            # Filter obvious noise
            if any(bad in name.lower() for bad in ["http", "{", "}", "null"]):
                continue
            out.add((cid, name.strip()))
    return out

def extract_product_candidates(obj: Any) -> List[Dict[str, Any]]:
    """
    Find product-like dicts. We look for:
    - productId/id + name/description + price-like fields
    """
    products: List[Dict[str, Any]] = []
    for d in _walk(obj):
        if not isinstance(d, dict):
            continue
        pid = d.get("productId", d.get("id", d.get("ProductId")))
        name = d.get("name", d.get("productName", d.get("title")))
        # price fields vary; grab any plausible
        price = (
            d.get("price")
            or d.get("unitPrice")
            or d.get("currentPrice")
            or d.get("salePrice")
            or d.get("Price")
        )
        if isinstance(pid, int) and isinstance(name, str) and len(name.strip()) > 1:
            products.append({"productId": pid, "name": name.strip(), "price": price, "_raw": d})
    return products

def guess_image_urls(d: Dict[str, Any]) -> List[str]:
    imgs: List[str] = []
    for k in ["image", "imageUrl", "img", "thumbnail", "thumbnailUrl", "primaryImage"]:
        v = d.get(k)
        if isinstance(v, str) and v.startswith("http"):
            imgs.append(v)
    # Sometimes arrays
    v = d.get("images")
    if isinstance(v, list):
        for it in v:
            if isinstance(it, str) and it.startswith("http"):
                imgs.append(it)
            if isinstance(it, dict):
                u = it.get("url") or it.get("imageUrl")
                if isinstance(u, str) and u.startswith("http"):
                    imgs.append(u)
    # De-dupe preserve order
    seen = set()
    out = []
    for u in imgs:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out[:6]

def product_url_from_id(pid: int, store: str) -> str:
    # Product pages exist at /product/<id> and /product/<id>/
    return with_store(f"{BASE_URL}/product/{pid}/", store)

# ---------------------------
# NETWORK CAPTURE
# ---------------------------
class ApiCapture:
    def __init__(self) -> None:
        self.json_urls_seen: Set[str] = set()
        self.category_candidates: Set[Tuple[int, str]] = set()
        self.product_candidates: Dict[int, Dict[str, Any]] = {}  # pid -> product info
        self.list_endpoints: Set[str] = set()  # endpoints with skip/take that return product lists

    async def on_response(self, resp: Response) -> None:
        try:
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct:
                return

            url = strip_fragment(resp.url)
            if url in self.json_urls_seen:
                return
            self.json_urls_seen.add(url)

            # Parse JSON
            data = await resp.json()

            # Mine categories/products from any JSON we see
            cats = extract_category_candidates(data)
            if cats:
                before = len(self.category_candidates)
                self.category_candidates |= cats
                if len(self.category_candidates) > before:
                    logger.info(f"Captured categories: +{len(self.category_candidates)-before} (total {len(self.category_candidates)})")

            prods = extract_product_candidates(data)
            if prods:
                added = 0
                for p in prods:
                    pid = p["productId"]
                    if pid not in self.product_candidates:
                        self.product_candidates[pid] = p
                        added += 1
                if added:
                    logger.info(f"Captured products: +{added} (total {len(self.product_candidates)})")

            # Detect list endpoints (if URL already uses skip/take and yielded products)
            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            if "skip" in qs and "take" in qs and prods:
                self.list_endpoints.add(url)
                logger.info(f"Detected list endpoint: {url}")

        except Exception:
            return

async def seed_store(context: BrowserContext, store: str) -> None:
    # Correct Playwright Python init script usage (single script string)
    await context.add_init_script(script=f"""
(() => {{
  const store = {json.dumps(store)};
  try {{
    localStorage.setItem('store', store);
    localStorage.setItem('selectedStore', store);
    localStorage.setItem('selected_store', store);
    localStorage.setItem('currentStore', store);
  }} catch (e) {{}}
}})();
""")

async def discovery_phase(context: BrowserContext, capture: ApiCapture) -> None:
    page = await context.new_page()
    page.on("response", lambda r: asyncio.create_task(capture.on_response(r)))

    logger.info(f"Discovery: loading homepage with store={DEFAULT_STORE}")
    await page.goto(with_store(HOME_URL, DEFAULT_STORE), wait_until="domcontentloaded", timeout=60000)

    # Click around lightly to trigger API calls (SPA menus)
    # If selectors don't exist, no problem — we still capture whatever loads.
    try:
        await page.wait_for_timeout(2000)
        # Try opening menu/category links if present
        for _ in range(3):
            await page.mouse.wheel(0, 1200)
            await page.wait_for_timeout(800)
    except Exception:
        pass

    logger.info(f"Discovery: waiting {DISCOVERY_SECONDS}s to capture API traffic...")
    await page.wait_for_timeout(DISCOVERY_SECONDS * 1000)
    await page.close()

async def fetch_products_via_list_endpoint(endpoint_sample: str, store: str) -> Set[int]:
    """
    Use a detected skip/take JSON endpoint to paginate directly with httpx.
    We don't assume response shape; we just mine product candidates.
    """
    logger.info("API mode: paginating via detected list endpoint (httpx)")
    p = urlparse(endpoint_sample)
    base = urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

    # Preserve query params other than skip/take; enforce store if the endpoint uses it
    qs = parse_qs(p.query)
    qs["store"] = [store]  # safe even if ignored
    # Remove skip/take so we can control
    qs.pop("skip", None)
    qs.pop("take", None)

    discovered: Set[int] = set()

    async with httpx.AsyncClient(timeout=30) as client:
        for page_idx in range(MAX_PAGES_PER_CATEGORY):
            skip = page_idx * TAKE
            q2 = dict(qs)
            q2["skip"] = [str(skip)]
            q2["take"] = [str(TAKE)]
            url = base + "?" + urlencode(q2, doseq=True)

            r = await client.get(url, headers={"accept": "application/json"})
            if r.status_code != 200:
                logger.warning(f"List endpoint HTTP {r.status_code} at skip={skip}")
                break

            data = r.json()
            prods = extract_product_candidates(data)
            if not prods:
                logger.info(f"No products at skip={skip}; stopping pagination")
                break

            for p in prods:
                discovered.add(p["productId"])

            logger.info(f"List endpoint: skip={skip} -> +{len(prods)} (total ids {len(discovered)})")

            if len(prods) < max(10, TAKE // 3):
                logger.info("Heuristic end: small batch; stopping")
                break

    return discovered

# ---------------------------
# SUPABASE WRITE (raw_products)
# ---------------------------
async def save_product_from_json(product: Dict[str, Any], store: str) -> None:
    pid = product["productId"]
    raw = product.get("_raw") or {}
    url = product_url_from_id(pid, store)

    payload = {
        "source": "newworld",
        "product_url": url,
        "name": product.get("name"),
        "brand": raw.get("brand") or "New World",
        "price_display": str(product.get("price")) if product.get("price") is not None else None,
        "price_numeric": normalize_price(product.get("price")),
        "currency": raw.get("currency") or "FJD",
        "categories": [],  # API may contain category names/ids; keep empty for now (can enrich later)
        "images": guess_image_urls(raw),
        "raw_html": json.dumps(raw, ensure_ascii=False),  # store raw JSON in raw_html field to keep schema same
        "scrape_timestamp": datetime.now(timezone.utc).isoformat(),
        "dedupe_key": generate_dedupe_key("newworld", url),
    }
    await supabase_upsert("raw_products", payload)

async def main() -> None:
    logger.info("Starting New World API-harvester main()")

    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Missing SUPABASE_URL or SUPABASE_KEY")

    capture = ApiCapture()

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

        await seed_store(context, DEFAULT_STORE)
        await discovery_phase(context, capture)

        logger.info(f"Discovery summary: json_urls={len(capture.json_urls_seen)} "
                    f"categories={len(capture.category_candidates)} "
                    f"products={len(capture.product_candidates)} "
                    f"list_endpoints={len(capture.list_endpoints)}")

        # If we found a list endpoint, paginate via httpx (true API mode)
        discovered_ids: Set[int] = set()
        if capture.list_endpoints:
            sample = next(iter(capture.list_endpoints))
            discovered_ids = await fetch_products_via_list_endpoint(sample, DEFAULT_STORE)

        # Merge any products already captured during discovery
        all_products: Dict[int, Dict[str, Any]] = dict(capture.product_candidates)

        # If we discovered ids but not full objects, keep minimal objects (we still save)
        for pid in discovered_ids:
            if pid not in all_products:
                all_products[pid] = {"productId": pid, "name": f"Product {pid}", "price": None, "_raw": {"productId": pid}}

        logger.info(f"Total product objects to save: {len(all_products)}")

        # Save concurrently
        sem = asyncio.Semaphore(CONCURRENCY)

        async def bounded_save(prod: Dict[str, Any]) -> None:
            async with sem:
                try:
                    await save_product_from_json(prod, DEFAULT_STORE)
                    logger.info(f"Saved productId={prod['productId']}")
                except Exception as e:
                    logger.error(f"Save failed productId={prod.get('productId')}: {e}")

        await asyncio.gather(*[bounded_save(prod) for prod in all_products.values()])

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
