import asyncio
import aiohttp
import asyncpg
import csv
import gzip
import json
import logging
import os
import random
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# ── LOGGING: stdout → bat-fil tar seg av >> scraper_log.txt ──
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler()]
)

# Per-butikk loggere — egne filer for live debugging
def make_store_logger(name):
    log = logging.getLogger(name)
    log.setLevel(logging.INFO)
    h = logging.FileHandler(f"scraped/log_{name}.txt", encoding="utf-8", mode="w")
    h.setFormatter(logging.Formatter("%(asctime)s | %(message)s"))
    log.addHandler(h)
    log.propagate = False
    return log

OUTPUT_FILE = "scraped/prices.csv"

GLOBAL_WORKERS = 30
LIMIT_PER_HOST = 5
PROXY_FILE = ""

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 Chrome/122.0.0.0 Safari/537.36",
]

STORES = {
    "kicks": {"id": 2, "sitemaps": ["https://www.kicks.no/sitemap"]},
    "bangerhead": {"id": 5, "sitemaps": ["https://www.bangerhead.no/sitemap.xml"]}
}

STORE_ID_TO_NAME = {v["id"]: k for k, v in STORES.items()}

_SKIP = re.compile(
    r"/(kategori|category|merke|brand|tilbud|offers|sok|search|"
    r"blogg|blog|article|inspiration|guide|kundeservice|club|"
    r"om-kicks|om-bangerhead|finn-butik|kampanje|gavekort|"
    r"personvern|sitemap|\?|#)",
    re.IGNORECASE
)

def is_product_url(url):
    if _SKIP.search(url):
        return False
    try:
        path = url.split("/", 3)[3].rstrip("/")
    except IndexError:
        return False
    return "/" not in path and "-" in path


def load_proxies():
    proxies = []
    try:
        with open(PROXY_FILE) as f:
            for line in f:
                ip, port, user, pw = line.strip().split(":")
                proxies.append(f"http://{user}:{pw}@{ip}:{port}")
    except Exception:
        pass
    logging.info("Loaded %s proxies", len(proxies))
    return proxies

PROXIES = load_proxies()

def get_proxy():
    return random.choice(PROXIES) if PROXIES else None


async def fetch(session, url, use_proxy=False):
    try:
        async with session.get(
            url,
            headers={"User-Agent": random.choice(USER_AGENTS)},
            proxy=get_proxy() if use_proxy else None,
            timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.read()
    except Exception:
        return None
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data.decode("utf-8", "ignore")


async def crawl_sitemap(session, url):
    xml = await fetch(session, url)
    if not xml:
        return []
    try:
        root = ET.fromstring(xml)
    except Exception:
        return []
    return [e.text for e in root.findall(".//{*}loc") if e.text]


async def gather_all_urls(session):
    all_urls = []
    seen = set()

    async def crawl_recursive(url, store_id, depth=0):
        if depth > 5:
            return
        urls = await crawl_sitemap(session, url)
        for u in urls:
            if u in seen:
                continue
            seen.add(u)
            if "sitemap" in u.lower() or u.endswith(".xml") or u.endswith(".xml.gz"):
                await crawl_recursive(u, store_id, depth + 1)
            elif is_product_url(u):
                all_urls.append((u, store_id))

    for store, info in STORES.items():
        before = len(all_urls)
        logging.info("=== Crawling sitemap: %s ===", store)
        for sitemap_url in info["sitemaps"]:
            await crawl_recursive(sitemap_url, info["id"])
        logging.info("%s: %s produkt-URLer etter filtrering", store, len(all_urls) - before)

    logging.info("TOTAL produkt-URLer: %s", len(all_urls))
    return all_urls


async def load_products():
    conn = await asyncpg.connect(
        host=os.environ["DB_HOST"],
        port=int(os.environ.get("DB_PORT", 5432)),
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_NAME"]
    )
    rows = await conn.fetch(
        "SELECT id, normalized_name FROM products WHERE normalized_name IS NOT NULL"
    )
    await conn.close()
    logging.info("Loaded %s products", len(rows))
    return {r["normalized_name"]: r["id"] for r in rows}


def extract_price(html):
    soup = BeautifulSoup(html, "lxml")

    # Metode 1: JSON-LD
    for script in soup.find_all("script", type="application/ld+json"):
        try:
            data = json.loads(script.string or "")
            if isinstance(data, list):
                data = data[0]
            if data.get("@type") == "Product":
                name = data.get("name")
                offers = data.get("offers")
                if isinstance(offers, list):
                    offers = offers[0]
                price = offers.get("price") if offers else None
                if name and price:
                    return name.lower().strip(), float(price)
        except Exception:
            pass

    # Metode 2: __NEXT_DATA__
    nd = soup.find("script", id="__NEXT_DATA__")
    if nd:
        try:
            data = json.loads(nd.string or "")
            props = data.get("props", {}).get("pageProps", {})
            product = props.get("product") or props.get("data", {}).get("product")
            if product:
                name = product.get("name") or product.get("title")
                price = (product.get("price") or product.get("currentPrice") or
                         product.get("salesPrice") or
                         (product.get("priceV2") or {}).get("amount"))
                if name and price:
                    return name.lower().strip(), float(str(price).replace(",", "."))
        except Exception:
            pass

    # Metode 3: meta-tag
    meta = soup.find("meta", {"property": "product:price:amount"})
    if meta and meta.get("content"):
        try:
            price = float(meta["content"].replace(",", "."))
            title = soup.find("title")
            name = title.string.lower().strip() if title else None
            if name and price:
                return name, price
        except Exception:
            pass

    title_tag = soup.find("h1") or soup.find("title")
    html_name = title_tag.get_text(strip=True).lower() if title_tag else None

    # Metode 4: Bangerhead — meta itemprop="price"
    try:
        meta_price = soup.find("meta", {"itemprop": "price"})
        if meta_price and meta_price.get("content"):
            price_val = float(meta_price["content"].replace(",", "."))
            if html_name and 10 < price_val < 50000:
                return html_name, price_val
    except Exception:
        pass

    # Metode 5: Bangerhead fallback — PrisREA / PrisORD
    try:
        for cls in ("PrisREA", "PrisORD"):
            el = soup.find(attrs={"class": cls})
            if el:
                m = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)", el.get_text())
                if m:
                    price_val = float(m.group(1).replace(" ", "").replace(",", "."))
                    if html_name and 10 < price_val < 50000:
                        return html_name, price_val
    except Exception:
        pass

    # Metode 6: Kicks — Price__StyledText / ScreenReaderOnly
    try:
        for span in soup.find_all("span"):
            classes = " ".join(span.get("class") or [])
            if "Price__StyledText" in classes or "ScreenReaderOnly" in classes:
                m = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)\s*kr", span.get_text())
                if m:
                    price_val = float(m.group(1).replace(" ", "").replace(",", "."))
                    if html_name and 10 < price_val < 50000:
                        return html_name, price_val
    except Exception:
        pass

    # Metode 7: Generisk tekst-fallback
    try:
        for t in soup.find_all(string=lambda s: s and "kr" in s and any(c.isdigit() for c in s)):
            m = re.search(r"(\d[\d\s]*(?:[.,]\d+)?)\s*kr", t)
            if m:
                v = float(m.group(1).replace(" ", "").replace(",", "."))
                if 10 < v < 50000 and html_name:
                    return html_name, v
    except Exception:
        pass

    return None, None


def match_product(name, slug, product_map):
    slug_name = re.sub(r"-+", " ", slug).lower().strip()
    words = slug_name.split()

    for i in range(len(words), max(2, len(words) - 6), -1):
        candidate = " ".join(words[:i])
        pid = product_map.get(candidate)
        if pid:
            return pid, f"slug_{i}w"

    if name and len(name) > 15:
        pid = product_map.get(name)
        if pid:
            return pid, "json_name"

    return None, None


store_stats = {}
stats = {"scraped": 0, "matched": 0, "errors": 0}


async def worker(queue, session, product_map, writer, lock, store_loggers):
    while True:
        url, store_id = await queue.get()
        store_name = STORE_ID_TO_NAME.get(store_id, str(store_id))
        slog = store_loggers[store_name]

        await asyncio.sleep(random.uniform(0.5, 1.5))

        try:
            async with session.get(
                url,
                headers={"User-Agent": random.choice(USER_AGENTS)},
                proxy=get_proxy(),
                timeout=aiohttp.ClientTimeout(total=15)
            ) as resp:
                if resp.status != 200:
                    stats["errors"] += 1
                    store_stats[store_id]["errors"] += 1
                    slog.info("HTTP %s | %s", resp.status, url[:80])
                    queue.task_done()
                    continue
                html = await resp.text()
        except Exception as e:
            logging.warning("Error | %s | %s", store_name, str(e)[:60])
            stats["errors"] += 1
            store_stats[store_id]["errors"] += 1
            queue.task_done()
            continue

        name, price = extract_price(html)
        slug = url.rstrip("/").split("/")[-1]
        pid, method = match_product(name, slug, product_map)

        stats["scraped"] += 1
        store_stats[store_id]["scraped"] += 1

        if pid and price:
            now = datetime.now(timezone.utc).isoformat()
            async with lock:
                writer.writerow([pid, store_id, price, url, now])
            stats["matched"] += 1
            store_stats[store_id]["matched"] += 1
            slog.info("MATCH [%s] pid=%s pris=%.0f | %s", method, pid, price, url[-60:])
        else:
            slog.info("MISS  | json=[%s] | slug=[%s] | pris=%s", name, slug[:50], price)

        if stats["scraped"] % 200 == 0:
            lines = [f"── Progress | totalt scraped={stats['scraped']} matched={stats['matched']} left={queue.qsize()} ──"]
            for sid, st in store_stats.items():
                sn = STORE_ID_TO_NAME.get(sid, str(sid))
                rate = f"{st['matched']/st['scraped']*100:.1f}%" if st["scraped"] else "–"
                lines.append(f"   {sn:12} scraped={st['scraped']:5} matched={st['matched']:4} ({rate}) errors={st['errors']}")
            logging.info("\n".join(lines))

        queue.task_done()


async def main():
    product_map = await load_products()

    store_loggers = {}
    for store, info in STORES.items():
        store_stats[info["id"]] = {"scraped": 0, "matched": 0, "errors": 0}
        store_loggers[store] = make_store_logger(store)
        logging.info("Per-butikk logg: scraped/log_%s.txt", store)

    connector = aiohttp.TCPConnector(limit=1000, limit_per_host=LIMIT_PER_HOST, ttl_dns_cache=300)

    async with aiohttp.ClientSession(connector=connector) as session:
        urls = await gather_all_urls(session)
        queue = asyncio.Queue()
        for u in urls:
            queue.put_nowait(u)

        lock = asyncio.Lock()
        with open(OUTPUT_FILE, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["product_id", "store_id", "price", "url", "last_checked"])
            workers = [
                asyncio.create_task(worker(queue, session, product_map, writer, lock, store_loggers))
                for _ in range(GLOBAL_WORKERS)
            ]
            logging.info("Started %s workers — queue: %s URLs", GLOBAL_WORKERS, queue.qsize())
            await queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

    logging.info("=" * 50)
    logging.info("FERDIG!")
    for sid, st in store_stats.items():
        sn = STORE_ID_TO_NAME.get(sid, str(sid))
        rate = f"{st['matched']/st['scraped']*100:.1f}%" if st["scraped"] else "–"
        logging.info("%s: scraped=%s matched=%s (%s) errors=%s", sn, st["scraped"], st["matched"], rate, st["errors"])


if __name__ == "__main__":
    asyncio.run(main())
