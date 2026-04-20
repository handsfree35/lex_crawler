# NordicBeuty Price Scraper

![Lex Crawler](lex.jpg)

Async Python price scraper built for [NordicBeuty](https://nordicbeuty.no) — a Norwegian skincare comparison platform.

## What it does

- Crawls product sitemaps from Norwegian skincare retailers
- Extracts product names and prices using 7 fallback methods (JSON-LD, Next.js `__NEXT_DATA__`, meta tags, CSS selectors, and generic text parsing)
- Matches scraped products against a PostgreSQL product catalog using progressive slug matching
- Writes matched prices to CSV for pipeline ingestion

## Tech stack

- `asyncio` + `aiohttp` — async HTTP with configurable concurrency (30 workers, 5 per host)
- `asyncpg` — async PostgreSQL
- `BeautifulSoup` + `lxml` — HTML parsing
- Per-store logging for live debugging

## Setup

```bash
pip install aiohttp asyncpg beautifulsoup4 lxml
mkdir scraped
```

Create a `.env` file (see `.env.example`) and export variables before running:

```bash
export DB_HOST=localhost
export DB_PORT=5432
export DB_USER=your_user
export DB_PASSWORD=your_password
export DB_NAME=your_database

python price_scraper_optimized.py
```

## Output

`scraped/prices.csv` — columns: `product_id, store_id, price, url, last_checked`

Per-store debug logs: `scraped/log_<store>.txt`

## Architecture

```
Sitemap crawl → URL filter → Async fetch (30 workers)
    → Price extraction (7 methods) → Slug matching → PostgreSQL → CSV
```

The slug matcher uses progressive truncation to handle product variants
(colours, sizes) that share a base product in the catalog.

## Part of NordicBeuty

This scraper feeds a multi-stage SQL pipeline:

```
prices.csv → offer_prices → offers → launch_pool → api_launch_products
```

Live platform: [nordicbeuty.no](https://nordicbeuty.no)
