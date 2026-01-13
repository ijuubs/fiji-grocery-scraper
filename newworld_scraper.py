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

DISCOVERY_SECONDS = int(os.getenv("NEWWORLD_DISCOVERY_SECONDS", "12"))
TAKE = int(os.getenv("NEWWORLD_TAKE", "40"))
MAX_PAGES_PER_CATEGORY = int(os.getenv("NEWWORLD_MAX_PAGES_PER_CATEGORY", "250"))

# Seed categories (IMPORTANT for triggering product API calls)
# Set in GitHub Action env: NEWWORLD_CATEGORY_IDS: "1154,1234,...."
DEFAULT_SEED_CATEGORY_IDS = [1154]

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
    s = re.sub(r"[^\d.]", "", str(value))
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
    parts = list(urlparse(url))
    qs = parse_qs(parts[4])
    qs["skip"] = [str(skip)]
    qs["take"] = [str(take)]
    parts[4] = urlencode(qs, doseq=True)
    return urlunparse(parts)

def seed_category_urls(store: str) -> List[str]:
    env_ids = os.getenv("NEWWORLD_CATEGORY_IDS", "").strip()
    ids: List[int] = []
    if env_ids:
        for part in env_ids.split(","):
            part = part.strip()
            if part.isdigit():
                ids.append(int(part))
    if not ids:
        ids = DEFAULT_SEED_CATEGORY_IDS

    urls = []
    for cid in ids:
        # Use the visible category route; frontend should trigger XHR calls from here.
        urls.append(with_store(f"{BASE_URL}/category/{cid}", store))
        # Also try a query-based category URL that your earlier script used (often triggers list API):
        urls.append(with_store(f"{BASE_URL}/category/{cid}?skip=0&take={TAKE}&discount=false&mixAndMatch=false&search=", store))
    return urls

# ---------------------------
# JSON MINING (generic heuristics)
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
    out: Set[Tuple[int, str]] = set()
    for d in _walk(obj):
        if not isinstance(d, dict):
            continue
        cid = d.get("categoryId", d.get("id", d.get("CategoryId")))
        name = d.get("categoryName", d.get("name", d.get("title", d.get("CategoryName"))))
        if isinstance(cid, int) and isinstance(name, str) and 1 <= cid <= 100000 and len(name) >= 2:
            if any(bad in name.lower() for bad in ["http", "{", "}", "null"]):
                continue
            out.add((cid, name.strip()))
    return out

def extract_product_candidates(obj: Any) -> List[Dict[str, Any]]:
    products: List[Dict[str, Any]] = []
    for d in _walk(obj):
        if not isinstance(d, dict):
            continue
        pid = d.get("productId", d.get("id", d.get("ProductId")))
        name = d.get("name", d.get("productName", d.get("title")))
        price = d.get("price") or d.get("unitPrice") or d.get("currentPrice") or d.get("salePrice") or d.get("Price")
        if isinstance(pid, int) and isinstance(name, str) and len(name.strip()) > 1:
            products.append({"productId": pid, "name": name.strip(), "price": price, "_raw": d})
    return products

def guess_image_urls(d: Dict[str, Any]) -> List[str]:
    imgs: List[str] = []
    for k in ["image", "imageUrl", "img", "thumbnail", "thumbnailUrl", "primaryImage"]:
        v = d.get(k)
        if isinstance(v, str) and v.startswith("http"):
            imgs.append(v)
    v = d.get("images")
    if isinstance(v, list):
        for it in v:
            if isinstance(it, str) and it.startswith("http"):
                imgs.append(it)
            if isinstance(it, dict):
                u = it.get("url") or it.get("imageUrl")
                if isinstance(u, str) and u.startswith("http"):
                    imgs.append(u)
    seen = set()
    out = []
    for u in imgs:
        if u not in seen:
            out.append(u)
            seen.add(u)
    return out[:6]

def product_url_from_id(pid: int, store: str) -> str:
    return with_store(f"{BASE_URL}/product/{pid}/", store)

# ---------------------------
# NETWORK CAPTURE
# ---------------------------
class ApiCapture:
    def __init__(self) -> None:
        self.json_urls_seen: List[str] = []
        self.json_url_set: Set[str] = set()
        self.category_candidates: Set[Tuple[int, str]] = set()
        self.product_candidates: Dict[int, Dict[str, Any]] = {}
        self.list_endpoints: Set[str] = set()

    async def on_response(self, resp: Response) -> None:
        try:
            url = strip_fragment(resp.url)

            # Try parsing JSON even if content-type is weird
            ct = (resp.headers.get("content-type") or "").lower()
            if "json" not in ct and "application/" in ct:
                # Still allow parsing if URL looks like API-ish
                if not any(x in url.lower() for x in ["api", "product", "category", "search", "graphql"]):
                    return

            if url in self.json_url_set:
                return
            self.json_url_set.add(url)
            self.json_urls_seen.append(url)

            # Prefer resp.json(); fallback to text->json if needed
            try:
                data = await resp.json()
            except Exception:
                txt = await resp.text()
                if not txt or len(txt) > 2_000_000:
                    return
                data = json.loads(txt)

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

            parsed = urlparse(url)
            qs = parse_qs(parsed.query)
            if "skip" in qs and "take" in qs and prods:
                self.list_endpoints.add(url)
                logger.info(f"Detected list endpoint: {url}")

        except Exception:
            return

async def seed_store(context: BrowserContext, store: str) -> None:
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
    await page.wait_for_timeout(1500)

    # Visit seed category URLs to FORCE the SPA to call product/category APIs
    seeds = seed_category_urls(DEFAULT_STORE)
    logger.info(f"Discovery: visiting {len(seeds)} seed category URLs to trigger XHR...")
    for u in seeds:
        try:
            logger.info(f"Discovery seed visit: {u}")
            await page.goto(u, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(2500)
            # light scroll to trigger lazy load
            await page.mouse.wheel(0, 1600)
            await page.wait_for_timeout(1000)
        except Exception as e:
            logger.warning(f"Seed visit failed: {e}")

    logger.info(f"Discovery: waiting {DISCOVERY_SECONDS}s to capture API traffic...")
    await page.wait_for_timeout(DISCOVERY_SECONDS * 1000)
    await page.close()

async def fetch_products_via_list_endpoint(endpoint_sample: str, store: str) -> Set[int]:
    logger.info("API mode: paginating via detected list endpoint (httpx)")
    p = urlparse(endpoint_sample)
    base = urlunparse((p.scheme, p.netloc, p.path, "", "", ""))

    qs = parse_qs(p.query)
    qs["store"] = [store]
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

            for p3 in prods:
                discovered.add(p3["productId"])

            logger.info(f"List endpoint: skip={skip} -> +{len(prods)} (total ids {len(discovered)})")

            if len(prods) < max(10, TAKE // 3):
                logger.info("Heuristic end: small batch; stopping")
                break

    return discovered

# ---------------------------
# SAVE
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
        "categories": [],
        "images": guess_image_urls(raw),
        # Keep schema unchanged; store raw JSON here for now:
        "raw_html": json.dumps(raw, ensure_ascii=False),
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
        browser = await p.chromium.launch(headless=True)
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

        logger.info(
            f"Discovery summary: json_urls={len(capture.json_url_set)} "
            f"categories={len(capture.category_candidates)} "
            f"products={len(capture.product_candidates)} "
            f"list_endpoints={len(capture.list_endpoints)}"
        )

        # If still empty, print captured JSON URLs so we can pinpoint the real API
        if len(capture.product_candidates) == 0 and len(capture.json_urls_seen) > 0:
            logger.warning("No products captured. Captured JSON URLs:")
            for u in capture.json_urls_seen[:30]:
                logger.warning(f"  JSON: {u}")

        discovered_ids: Set[int] = set()
        if capture.list_endpoints:
            sample = next(iter(capture.list_endpoints))
            discovered_ids = await fetch_products_via_list_endpoint(sample, DEFAULT_STORE)

        all_products: Dict[int, Dict[str, Any]] = dict(capture.product_candidates)
        for pid in discovered_ids:
            if pid not in all_products:
                all_products[pid] = {"productId": pid, "name": f"Product {pid}", "price": None, "_raw": {"productId": pid}}

        logger.info(f"Total product objects to save: {len(all_products)}")

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
