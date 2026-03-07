"""
Dubizzle UAE Property Scraper
==============================
Scrapes property listings from Dubizzle UAE.
Tracks price history and detects price drops.

Usage:
    pip install requests
    python dubizzle_scraper.py

Output:
  - listings_db_uae.json       → full database of all property listings + price history
  - drops_feed_uae.json        → current active price drops (for the dashboard)
"""

import requests
import json
import os
import time
import random
from datetime import datetime

WAR_START = "2026-03-01"
BASE_URL = "https://www.dubizzle.com.ae"
CATEGORY_URLS = [
    "/properties/apartments-duplex/for-sale/",
    "/properties/villa-house/for-sale/",
    "/properties/penthouse/for-sale/",
    "/properties/townhouse/for-sale/",
    "/properties/land/for-sale/",
]
MAX_PAGES_PER_CATEGORY = 25
DB_FILE = "listings_db_uae.json"
DROPS_FILE = "drops_feed_uae.json"

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
    """Extract listing data from Dubizzle page.
    Dubizzle uses a similar window.__remixContext or window.state pattern.
    Try multiple extraction patterns."""

    # Pattern 1: window.state (same as OLX - shared platform)
    marker = "window.state = "
    idx = html.find(marker)
    if idx != -1:
        start = html.index("{", idx)
        decoder = json.JSONDecoder()
        try:
            state, _ = decoder.raw_decode(html, start)
            algolia = state.get("algolia", {})
            content = algolia.get("content")
            if content:
                return content.get("hits", []), content.get("nbPages", 0)
        except (json.JSONDecodeError, ValueError):
            pass

    # Pattern 2: __NEXT_DATA__ (Next.js pattern)
    marker2 = '__NEXT_DATA__" type="application/json">'
    idx2 = html.find(marker2)
    if idx2 != -1:
        start2 = idx2 + len(marker2)
        end2 = html.find("</script>", start2)
        if end2 != -1:
            try:
                next_data = json.loads(html[start2:end2])
                props = next_data.get("props", {}).get("pageProps", {})
                search = props.get("searchResult", props.get("listings", {}))
                if isinstance(search, dict):
                    hits = search.get("hits", search.get("results", search.get("listings", [])))
                    pages = search.get("nbPages", search.get("totalPages", 0))
                    return hits, pages
            except (json.JSONDecodeError, ValueError):
                pass

    # Pattern 3: Look for JSON-LD structured data
    marker3 = "window.__PRELOADED_STATE__ = "
    idx3 = html.find(marker3)
    if idx3 != -1:
        start3 = html.index("{", idx3)
        decoder = json.JSONDecoder()
        try:
            state, _ = decoder.raw_decode(html, start3)
            listings = state.get("listings", state.get("search", {}))
            if isinstance(listings, dict):
                hits = listings.get("items", listings.get("hits", []))
                pages = listings.get("totalPages", listings.get("nbPages", 0))
                return hits, pages
        except (json.JSONDecodeError, ValueError):
            pass

    return [], 0

def get_formatted_field(hit, attribute):
    """Try multiple patterns to get field value."""
    # Pattern 1: formattedExtraFields (OLX-style)
    for f in hit.get("formattedExtraFields", []):
        if f.get("attribute") == attribute:
            return f.get("formattedValue", "")
    # Pattern 2: Direct field
    return hit.get(attribute, "")

def get_extra_field(hit, key, default=None):
    """Get field from extraFields or direct."""
    extra = hit.get("extraFields") or {}
    val = extra.get(key)
    if val is not None:
        return val
    return hit.get(key, default)

def parse_hit(hit):
    price = get_extra_field(hit, "price")
    if not price or price < 10000:
        return None

    ext_id = str(hit.get("externalID", hit.get("id", "")))
    slug = hit.get("slug", "")
    title = hit.get("title", "Unknown")

    prop_type = get_formatted_field(hit, "category") or get_formatted_field(hit, "type") or ""
    if not prop_type:
        title_lower = title.lower()
        if "villa" in title_lower or "house" in title_lower:
            prop_type = "Villa"
        elif "penthouse" in title_lower:
            prop_type = "Penthouse"
        elif "townhouse" in title_lower:
            prop_type = "Townhouse"
        elif "land" in title_lower or "plot" in title_lower:
            prop_type = "Land"
        else:
            prop_type = "Apartment"

    sqm = get_extra_field(hit, "area") or get_extra_field(hit, "size")
    bedrooms = get_extra_field(hit, "rooms") or get_extra_field(hit, "bedrooms")

    locations = hit.get("location", [])
    loc_parts = []
    district = ""
    if isinstance(locations, list):
        for loc in locations:
            level = loc.get("level", -1)
            name = loc.get("name", "")
            if level == 1:
                district = name
                loc_parts.append(name)
            elif level == 2:
                loc_parts.insert(0, name)
    elif isinstance(locations, str):
        loc_parts = [locations]
        district = locations
    location_str = ", ".join(loc_parts) if loc_parts else "UAE"

    url = f"{BASE_URL}/ad/{slug}-ID{ext_id}.html" if slug else ""
    if not url and ext_id:
        url = f"{BASE_URL}/ad/ID{ext_id}.html"

    return {
        "id": ext_id,
        "title": title,
        "url": url,
        "type": prop_type,
        "sqm": sqm,
        "bedrooms": bedrooms,
        "location": location_str,
        "district": district,
        "price_usd": price,
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
        else:
            db[lid] = {
                "id": lid,
                "title": listing["title"],
                "url": listing["url"],
                "type": listing.get("type", "Apartment"),
                "sqm": listing.get("sqm"),
                "bedrooms": listing.get("bedrooms"),
                "location": listing["location"],
                "district": listing.get("district", ""),
                "original_price": listing["price_usd"],
                "current_price": listing["price_usd"],
                "price_history": [{"price": listing["price_usd"], "date": today}],
                "first_seen": today,
                "last_seen": today,
                "last_updated": today,
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
                "type": listing.get("type", "Apartment"),
                "sqm": listing.get("sqm"),
                "bedrooms": listing.get("bedrooms"),
                "location": listing["location"],
                "original_price": listing["original_price"],
                "current_price": listing["current_price"],
                "drop_usd": listing["drop_usd"],
                "drop_pct": listing["drop_pct"],
                "last_drop_date": listing["last_drop_date"],
                "first_seen": listing["first_seen"],
                "price_history": listing["price_history"],
            })
        if listing.get("first_seen", WAR_START) > WAR_START:
            new_listings.append({
                "id": listing["id"],
                "title": listing["title"],
                "url": listing["url"],
                "type": listing.get("type", "Apartment"),
                "sqm": listing.get("sqm"),
                "bedrooms": listing.get("bedrooms"),
                "location": listing["location"],
                "original_price": listing["original_price"],
                "current_price": listing["current_price"],
                "first_seen": listing["first_seen"],
                "price_history": listing["price_history"],
            })
    drops.sort(key=lambda x: x["drop_pct"], reverse=True)
    new_listings.sort(key=lambda x: x["first_seen"], reverse=True)

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
    print("  Dubizzle UAE Property Scraper")
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
