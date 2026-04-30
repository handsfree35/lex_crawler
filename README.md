# Lex Crawler — Async E-commerce Price Scraper

![Lex Crawler](lex.jpg)

Async Python crawler for collecting product and price data from Norwegian skincare retailers.

Built for [NordicBeuty](https://nordicbeuty.no), a skincare discovery and price comparison platform for the Norwegian market.

---

## Problem

E-commerce product data is messy.

Retailers use different page structures, price formats, metadata patterns and JavaScript-rendered content. A simple scraper that relies on one CSS selector will break quickly when stores change their markup.

The goal of Lex Crawler is to collect product and price data from multiple retailers, normalize the output and feed it into the NordicBeuty product database.

---

## What It Does

- Crawls product sitemaps from Norwegian skincare retailers
- Filters and discovers relevant product URLs
- Fetches pages asynchronously with configurable concurrency
- Extracts product names and prices using multiple fallback methods
- Parses JSON-LD, Next.js `__NEXT_DATA__`, meta tags, CSS selectors and generic text
- Matches scraped products against a PostgreSQL product catalog
- Uses progressive slug matching to handle variants and naming differences
- Writes matched prices to CSV for pipeline ingestion
- Creates per-store debug logs for easier troubleshooting

---

## Results

- Designed for scheduled product and price ingestion
- Supports multi-store scraping workflows
- Uses 7 fallback methods for price extraction
- Matches products against a PostgreSQL-backed catalog
- Feeds the NordicBeuty offer/pricing pipeline

If running with full production data, the crawler is designed to support large-scale URL processing, retailer-specific debugging and recurring price updates.

---

## Tech Stack

- `asyncio` + `aiohttp` — async HTTP fetching with configurable concurrency
- `asyncpg` — async PostgreSQL access
- `BeautifulSoup` + `lxml` — HTML parsing
- CSV export for ingestion
- Per-store logging for debugging
- PostgreSQL-backed product matching

---

## Architecture

    Retailer sitemap
        ↓
    URL discovery and filtering
        ↓
    Async HTTP fetch
        ↓
    HTML / JSON-LD / Next.js metadata parsing
        ↓
    Price extraction using fallback methods
        ↓
    Progressive slug matching
        ↓
    PostgreSQL catalog lookup
        ↓
    CSV export
        ↓
    NordicBeuty ingestion pipeline

---

## Extraction Strategy

Lex Crawler does not rely on a single selector.

It uses several fallback methods because each retailer exposes product data differently:

1. JSON-LD structured data
2. Next.js `__NEXT_DATA__`
3. Meta tags
4. CSS selectors
5. Generic text parsing
6. URL and slug-based heuristics
7. Store-specific fallback logic

This makes the crawler more resilient when individual retailers change markup.

---

## Product Matching

The crawler matches scraped products against an existing PostgreSQL catalog using progressive slug matching.

This helps handle:

- Product variants
- Different naming formats
- Sizes and colours
- Retailer-specific product titles
- Shared base products across multiple stores

---

## Setup

Install dependencies:

    pip install aiohttp asyncpg beautifulsoup4 lxml
    mkdir scraped

Create a `.env` file based on `.env.example` and export the database variables:

    export DB_HOST=localhost
    export DB_PORT=5432
    export DB_USER=your_user
    export DB_PASSWORD=your_password
    export DB_NAME=your_database

Run the crawler:

    python price_scraper_optimized.py

---

## Output

Price observations are written to:

    scraped/prices.csv

Columns:

    product_id, store_id, price, url, last_checked

Per-store debug logs:

    scraped/log_<store>.txt

---

## NordicBeuty Pipeline

Lex Crawler feeds a multi-stage SQL ingestion pipeline:

    prices.csv
        ↓
    offer_prices
        ↓
    offers
        ↓
    launch_pool
        ↓
    api_launch_products

Live platform:

[nordicbeuty.no](https://nordicbeuty.no)

---

## Engineering Notes

The crawler is built around fault tolerance and real-world data messiness.

Retailer pages vary heavily, so the extraction layer uses multiple fallback methods instead of assuming consistent HTML. Concurrency is configurable to avoid overly aggressive scraping, and per-store logs make it easier to debug when a retailer changes structure or blocks requests.

---

## Status

Active project used as part of the NordicBeuty data pipeline.
