"""
OLX Lebanon Number Plates Scraper
===================================
Scrapes number plate listings from OLX Lebanon.
Tracks price history and detects price drops.

Usage:
    pip install requests
    python olx_scraper_plates.py

Output:
  - listings_db_plates.json  → full database of all plate listings + price history
  - drops_feed_plates.json   → current active price drops (for the dashboard)
"""

import requests
import json
import os
import time
import random
from datetime import datetime

WAR_START = "2026-03-01"
BASE_URL = "https://www.olx.com.lb"
CATEGORY_URLS = [
    "/vehicles/number-plates/",
]
MAX_PAGES_PER_CATEGORY = 25
DB_FILE = "listings_db_plates.json"
DROPS_FILE = "drops_feed_plates.json"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/124.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

MIN_DELAY = 2
MAX_DELAY = 4

def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, ensure_ascii=False, indent=2)

def save_drops(drops):
    with open(DROPS_FILE, "w", encoding="utf-8") as f:
        json.dump(drops, f, ensure_ascii=False, indent=2)

def fetch_page(url):
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    try:
        resp = requests.get(url, headers=HEADERS, timeout=20)
        resp.raise_for_status()
        return resp.text
    except requests.RequestException as e:
        print(f"  ✗ Failed to fetch {url}: {e}")
        return None

def extract_hits(html):
    marker = "window.state = "
    idx = html.find(marker)
    if idx == -1:
        return [], 0
    start = html.index("{", idx)
    decoder = json.JSONDecoder()
    try:
        state, _ = decoder.raw_decode(html, start)
    except json.JSONDecodeError as e:
        print(f"  ✗ JSON decode error: {e}")
        return [], 0
    algolia = state.get("algolia", {})
    content = algolia.get("content")
    if not content:
        return [], 0
    return content.get("hits", []), content.get("nbPages", 0)

def get_formatted_field(hit, attribute):
    for f in hit.get("formattedExtraFields", []):
        if f.get("attribute") == attribute:
            return f.get("formattedValue", "")
    return ""

def parse_date(date_str):
    """Parse ISO format date with T and Z, return YYYY-MM-DD or None."""
    if not date_str:
        return None
    try:
        # Handle ISO format with T and Z (e.g., "2026-03-05T10:30:45Z")
        if "T" in date_str:
            date_str = date_str.split("T")[0]
        return date_str
    except Exception:
        return None

def parse_hit(hit):
    extra = hit.get("extraFields") or {}
    price = extra.get("price")
    if not price or price < 10:
        return None

    ext_id = hit.get("externalID", "")
    slug = hit.get("slug", "")
    title = hit.get("title", "Unknown")

    # Plate-specific fields
    plate_code = get_formatted_field(hit, "plate_code") or ""
    plate_type = get_formatted_field(hit, "plate_type") or "Standard"

    locations = hit.get("location", [])
    loc_parts = []
    district = ""
    for loc in locations:
        level = loc.get("level", -1)
        name = loc.get("name", "")
        if level == 1:
            district = name
            loc_parts.append(name)
        elif level == 2:
            loc_parts.insert(0, name)
    location_str = ", ".join(loc_parts) if loc_parts else "Lebanon"

    url = f"{BASE_URL}/ad/{slug}-ID{ext_id}.html" if slug else ""

    # Extract posted_date from various possible fields
    posted_date_str = (
        parse_date(hit.get("createdAt")) or
        parse_date(hit.get("created_at")) or
        parse_date(hit.get("publishedAt"))
    )

    return {
        "id": ext_id,
        "title": title,
        "url": url,
        "plate_code": plate_code,
        "plate_type": plate_type,
        "location": location_str,
        "district": district,
        "price_usd": price,
        "posted_date": posted_date_str,
    }

def scrape_category(cat_path):
    listings = []
    url = BASE_URL + cat_path
    print(f"  Fetching page 1: {url}")
    html = fetch_page(url)
    if not html:
        return listings

    hits, nb_pages = extract_hits(html)
    if not hits:
        print("  → No hits found on page 1, stopping.")
        return listings

    max_page = min(nb_pages, MAX_PAGES_PER_CATEGORY)
    print(f"  → Page 1: {len(hits)} hits, {nb_pages} total pages (scraping up to {max_page})")

    for hit in hits:
        parsed = parse_hit(hit)
        if parsed:
            listings.append(parsed)

    for page in range(2, max_page + 1):
        page_url = f"{url}?page={page}"
        print(f"  Fetching page {page}: {page_url}")
        html = fetch_page(page_url)
        if not html:
            break
        hits, _ = extract_hits(html)
        if not hits:
            print(f"  → No hits on page {page}, stopping.")
            break
        for hit in hits:
            parsed = parse_hit(hit)
            if parsed:
                listings.append(parsed)
        print(f"  → {len(listings)} valid listings so far")

    return listings

def update_database(db, new_listings):
    today = datetime.now().strftime("%Y-%m-%d")
    new_count = 0
    updated = 0
    drops = 0

    for listing in new_listings:
        lid = listing["id"]
        if lid in db:
            existing = db[lid]
            old_price = existing["current_price"]
            new_price = listing["price_usd"]
            if new_price and old_price and new_price != old_price:
                existing["price_history"].append({"price": new_price, "date": today})
                existing["current_price"] = new_price
                existing["last_updated"] = today
                if new_price < old_price:
                    existing["drop_usd"] = existing["original_price"] - new_price
                    existing["drop_pct"] = round(
                        (existing["original_price"] - new_price) / existing["original_price"] * 100, 1
                    )
                    existing["last_drop_date"] = today
                    drops += 1
                updated += 1
            existing["title"] = listing["title"]
            existing["url"] = listing["url"]
            existing["last_seen"] = today
            # Backfill posted_date if not already present
            if "posted_date" not in existing:
                existing["posted_date"] = listing.get("posted_date") or existing["first_seen"]
        else:
            db[lid] = {
                "id": lid,
                "title": listing["title"],
                "url": listing["url"],
                "plate_code": listing.get("plate_code", ""),
                "plate_type": listing.get("plate_type", "Standard"),
                "location": listing["location"],
                "district": listing.get("district", ""),
                "original_price": listing["price_usd"],
                "current_price": listing["price_usd"],
                "price_history": [{"price": listing["price_usd"], "date": today}],
                "first_seen": today,
                "last_seen": today,
                "last_updated": today,
                "posted_date": listing.get("posted_date") or today,
                "drop_usd": 0,
                "drop_pct": 0,
                "last_drop_date": None,
            }
            new_count += 1

    return new_count, updated, drops

def generate_drops_feed(db):
    drops = []
    new_listings = []
    for lid, listing in db.items():
        if listing["drop_usd"] > 0:
            drops.append({
                "id": listing["id"],
                "title": listing["title"],
                "url": listing["url"],
                "plate_code": listing.get("plate_code", ""),
                "plate_type": listing.get("plate_type", "Standard"),
                "location": listing["location"],
                "original_price": listing["original_price"],
                "current_price": listing["current_price"],
                "drop_usd": listing["drop_usd"],
                "drop_pct": listing["drop_pct"],
                "last_drop_date": listing["last_drop_date"],
                "first_seen": listing["first_seen"],
                "posted_date": listing.get("posted_date"),
                "price_history": listing["price_history"],
            })
        post_date = listing.get("posted_date") or listing.get("first_seen", WAR_START)
        if post_date >= WAR_START:
            new_listings.append({
                "id": listing["id"],
                "title": listing["title"],
                "url": listing["url"],
                "plate_code": listing.get("plate_code", ""),
                "plate_type": listing.get("plate_type", "Standard"),
                "location": listing["location"],
                "original_price": listing["original_price"],
                "current_price": listing["current_price"],
                "first_seen": listing["first_seen"],
                "posted_date": listing.get("posted_date"),
                "price_history": listing["price_history"],
            })
    drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    new_listings.sort(key=lambda x: x["posted_date"] or x["first_seen"], reverse=True)

    return {
        "generated_at": datetime.now().isoformat(),
        "total_tracked": len(db),
        "total_drops": len(drops),
        "total_new": len(new_listings),
        "avg_drop_pct": round(sum(d["drop_pct"] for d in drops) / len(drops), 1) if drops else 0,
        "biggest_drop_usd": max((d["drop_usd"] for d in drops), default=0),
        "drops": drops,
        "new_listings": new_listings,
    }

def main():
    print("=" * 60)
    print("  OLX Lebanon Number Plates Scraper")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    db = load_db()
    print(f"\n📦 Loaded database: {len(db)} existing listings\n")

    all_listings = []
    for cat_url in CATEGORY_URLS:
        print(f"\n🔍 Scraping: {cat_url}")
        listings = scrape_category(cat_url)
        all_listings.extend(listings)
        print(f"  ✓ Got {len(listings)} listings from this category")

    print(f"\n📊 Total scraped: {len(all_listings)} listings")
    seen = set()
    unique = []
    for item in all_listings:
        if item["id"] not in seen:
            seen.add(item["id"])
            unique.append(item)
    print(f"📊 Unique listings: {len(unique)}")

    new_count, updated, drops = update_database(db, unique)
    print(f"\n✅ Results:")
    print(f"  New listings: {new_count}")
    print(f"  Price changes: {updated}")
    print(f"  Price drops: {drops}")

    save_db(db)
    print(f"\n💾 Saved database: {len(db)} total listings → {DB_FILE}")

    feed = generate_drops_feed(db)
    save_drops(feed)
    print(f"📡 Generated drops feed: {feed['total_drops']} drops → {DROPS_FILE}")

    today = datetime.now()
    stale = 0
    for lid, listing in db.items():
        last_seen = datetime.strptime(listing["last_seen"], "%Y-%m-%d")
        if (today - last_seen).days > 7:
            listing["stale"] = True
            stale += 1
    if stale:
        print(f"⚠️ {stale} listings not seen in 7+ days (possibly sold/removed)")
        save_db(db)

    print(f"\n{'=' * 60}")
    print("  Done! Dashboard data ready.")
    print(f"{'=' * 60}\n")

if __name__ == "__main__":
    main()
