#!/usr/bin/env python3
"""
Dubizzle Dubai Property Scraper
Uses Algolia API directly to bypass bot protection.
"""

import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone

# Algolia config (public search-only key from dubizzle frontend)
ALGOLIA_APP_ID = "WD0PTZ13ZS"
ALGOLIA_API_KEY = "cef139620248f1bc328a00fddc7107a6"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/*/queries"

# Category configs: (slug_filter, index_name, label)
CATEGORIES = [
    (
        '("categories_v2.slug_paths":"property-for-sale") AND ("categories_v2.slug_paths":"property-for-sale/residential") AND ("city.id"=2)',
        "by_verification_feature_asc_property-for-sale-residential.com",
        "Residential",
    ),
    (
        '("categories_v2.slug_paths":"property-for-sale") AND ("categories_v2.slug_paths":"property-for-sale/commercial") AND ("city.id"=2)',
        "by_verification_feature_asc_property-for-sale-commercial.com",
        "Commercial",
    ),
    (
        '("categories_v2.slug_paths":"property-for-sale") AND ("categories_v2.slug_paths":"property-for-sale/land") AND ("city.id"=2)',
        "by_verification_feature_asc_property-for-sale-land.com",
        "Land",
    ),
    (
        '("categories_v2.slug_paths":"property-for-sale") AND ("categories_v2.slug_paths":"property-for-sale/multiple-units") AND ("city.id"=2)',
        "by_verification_feature_asc_property-for-sale-multiple-units.com",
        "Multiple Units",
    ),
]

MAX_PAGES = 5
HITS_PER_PAGE = 35
DB_FILE = "listings_db_uae.json"
FEED_FILE = "drops_feed_uae.json"
WAR_START = "2026-03-01"
DROP_THRESHOLD = 0.05
BASE_URL = "https://dubai.dubizzle.com"


def algolia_search(index_name, filters, page=0):
    """Query Algolia search API directly."""
    body = json.dumps({
        "requests": [
            {
                "indexName": index_name,
                "params": f"filters={urllib.parse.quote(filters)}&hitsPerPage={HITS_PER_PAGE}&page={page}",
            }
        ]
    }).encode("utf-8")

    headers = {
        "x-algolia-api-key": ALGOLIA_API_KEY,
        "x-algolia-application-id": ALGOLIA_APP_ID,
        "Content-Type": "application/json",
    }

    for attempt in range(3):
        try:
            req = urllib.request.Request(ALGOLIA_URL, data=body, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                result = data.get("results", [{}])[0]
                hits = result.get("hits", [])
                total_pages = result.get("nbPages", 0)
                return hits, total_pages
        except Exception as e:
            print(f"  Attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(2)
    return [], 0


import urllib.parse


def parse_hit(hit):
    """Parse a single property listing hit into a standardized dict."""
    try:
        name = hit.get("name", {})
        title = name.get("en", name) if isinstance(name, dict) else str(name)

        price = hit.get("price")
        if price is None:
            return None
        try:
            price = int(float(price))
        except (ValueError, TypeError):
            return None
        if price <= 0:
            return None

        # Size and rooms
        size = hit.get("size", "")
        bedrooms = hit.get("bedrooms", "")
        bathrooms = hit.get("bathrooms", "")

        # Location
        city = hit.get("city", {})
        city_name = city.get("name", {}).get("en", "Dubai") if isinstance(city, dict) else "Dubai"
        neighborhoods = hit.get("neighborhoods", {})
        area_names = neighborhoods.get("name", {}).get("en", []) if isinstance(neighborhoods, dict) else []
        area = area_names[0] if isinstance(area_names, list) and area_names else ""

        # URL
        abs_url = hit.get("absolute_url", {})
        url = abs_url.get("en", "") if isinstance(abs_url, dict) else str(abs_url)
        if url and not url.startswith("http"):
            url = BASE_URL + url

        # Category
        cats = hit.get("categories", {})
        cat_names = cats.get("name", {}).get("en", []) if isinstance(cats, dict) else []
        category = cat_names[0] if isinstance(cat_names, list) and cat_names else "Property"

        # Date
        added = hit.get("added")
        if added and isinstance(added, (int, float)) and added > 1000000000:
            date_str = datetime.fromtimestamp(added, tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        listing_id = str(hit.get("id", hit.get("objectID", "")))
        if not listing_id:
            listing_id = url.split("/")[-2] if url else str(hash(title))

        return {
            "id": listing_id,
            "title": title,
            "price": price,
            "size": str(size),
            "bedrooms": str(bedrooms),
            "bathrooms": str(bathrooms),
            "area": area,
            "city": city_name,
            "category": category,
            "url": url,
            "date": date_str,
        }
    except Exception as e:
        print(f"  Parse error: {e}")
        return None


def scrape_all():
    """Scrape all property categories via Algolia API."""
    all_listings = []
    for filters, index_name, label in CATEGORIES:
        print(f"\nScraping category: {label}")
        page = 0
        while page < MAX_PAGES:
            print(f"  Page {page}: querying Algolia index {index_name}")
            hits, total_pages = algolia_search(index_name, filters, page)
            print(f"  Found {len(hits)} hits, totalPages={total_pages}")
            if not hits:
                break
            for hit in hits:
                listing = parse_hit(hit)
                if listing:
                    all_listings.append(listing)
            page += 1
            if page >= total_pages:
                break
            time.sleep(0.5)
    print(f"\nTotal property listings scraped: {len(all_listings)}")
    return all_listings


def load_db():
    if os.path.exists(DB_FILE):
        try:
            with open(DB_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass
    return {}


def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=2)


def load_feed():
    if os.path.exists(FEED_FILE):
        try:
            with open(FEED_FILE, "r") as f:
                return json.load(f)
        except (json.JSONDecodeError, ValueError):
            pass
    return {"meta": {"war_start": WAR_START, "threshold": DROP_THRESHOLD}, "tracked": 0, "drops": []}


def save_feed(feed):
    with open(FEED_FILE, "w") as f:
        json.dump(feed, f, indent=2)


def main():
    print("=" * 60)
    print("Dubizzle Dubai Property Scraper")
    print(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    print("=" * 60)
    listings = scrape_all()
    if not listings:
        print("No listings found. Exiting.")
        return

    db = load_db()
    feed = load_feed()
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    new_count = 0
    drop_count = 0

    for listing in listings:
        lid = listing["id"]
        price = listing["price"]

        if lid in db:
            prev = db[lid]
            prev_price = prev.get("price", price)
            if price < prev_price:
                drop_pct = (prev_price - price) / prev_price
                if drop_pct >= DROP_THRESHOLD:
                    drop_entry = {
                        "id": lid,
                        "title": listing["title"],
                        "old_price": prev_price,
                        "new_price": price,
                        "drop_pct": round(drop_pct * 100, 1),
                        "url": listing["url"],
                        "area": listing["area"],
                        "category": listing["category"],
                        "size": listing.get("size", ""),
                        "bedrooms": listing.get("bedrooms", ""),
                        "date": today,
                        "first_seen": prev.get("first_seen", today),
                    }
                    feed["drops"].append(drop_entry)
                    drop_count += 1
            db[lid]["price"] = price
            db[lid]["last_seen"] = today
            db[lid]["title"] = listing["title"]
        else:
            db[lid] = {
                "title": listing["title"],
                "price": price,
                "size": listing.get("size", ""),
                "bedrooms": listing.get("bedrooms", ""),
                "bathrooms": listing.get("bathrooms", ""),
                "area": listing["area"],
                "city": listing["city"],
                "category": listing["category"],
                "url": listing["url"],
                "first_seen": today,
                "last_seen": today,
            }
            new_count += 1

    feed["tracked"] = len(db)
    feed["last_updated"] = today
    feed["drops"] = feed["drops"][-500:]

    save_db(db)
    save_feed(feed)

    print(f"\nResults:")
    print(f"  New listings: {new_count}")
    print(f"  Price drops: {drop_count}")
    print(f"  Total tracked: {len(db)}")


if __name__ == "__main__":
    main()
