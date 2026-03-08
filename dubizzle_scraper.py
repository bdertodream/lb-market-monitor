"""
Dubizzle Dubai Property Scraper
================================
Scrapes property listings from Dubizzle Dubai.
Tracks price history and detects price drops.

Usage:
    pip install requests
    python dubizzle_scraper.py

Output:
    - listings_db_uae.json → full database of all property listings + price history
    - drops_feed_uae.json → current active price drops (for the dashboard)
"""

import requests
import json
import os
import time
import random
from datetime import datetime

WAR_START = "2026-03-01"
BASE_URL = "https://dubai.dubizzle.com"

CATEGORY_URLS = [
    "/en/property-for-sale/residential/",
    "/en/property-for-sale/commercial/",
    "/en/property-for-sale/land/",
    "/en/property-for-sale/multiple-units/",
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
        print(f"  \u2717 Failed to fetch {url}: {e}")
        return None


def extract_hits(html):
    """Extract listing data from Dubizzle Dubai page.
    Dubizzle uses Next.js with Redux SSR actions in __NEXT_DATA__."""

    marker = '<script id="__NEXT_DATA__" type="application/json">'
    idx = html.find(marker)
    if idx != -1:
        start = idx + len(marker)
        end = html.find("</script>", start)
        if end != -1:
            try:
                next_data = json.loads(html[start:end])
                actions = (
                    next_data.get("props", {})
                    .get("pageProps", {})
                    .get("reduxWrapperActionsGIPP", [])
                )
                for action in actions:
                    if action.get("type") == "listings/fetchListingDataForQuery/fulfilled":
                        payload = action.get("payload", {})
                        hits = payload.get("hits", [])
                        pagination = payload.get("pagination", {})
                        total_pages = pagination.get("totalPages", 0)
                        return hits, total_pages
            except (json.JSONDecodeError, ValueError) as e:
                print(f"  \u2717 JSON parse error: {e}")

    # Fallback: try window.state pattern (legacy)
    marker2 = "window.state = "
    idx2 = html.find(marker2)
    if idx2 != -1:
        start2 = html.index("{", idx2)
        decoder = json.JSONDecoder()
        try:
            state, _ = decoder.raw_decode(html, start2)
            algolia = state.get("algolia", {})
            content = algolia.get("content")
            if content:
                return content.get("hits", []), content.get("nbPages", 0)
        except (json.JSONDecodeError, ValueError):
            pass

    return [], 0


def parse_hit(hit):
    price = hit.get("price")
    if not price or price < 10000:
        return None

    ext_id = str(hit.get("id", hit.get("external_id", "")))

    # Name is multilingual dict
    name = hit.get("name", {})
    title = name.get("en", name.get("ar", "Unknown")) if isinstance(name, dict) else str(name)

    # URL
    abs_url = hit.get("absolute_url", {})
    url = abs_url.get("en", "") if isinstance(abs_url, dict) else str(abs_url)

    # Posted date from unix timestamp
    added = hit.get("added")
    posted_date_str = None
    if added and isinstance(added, (int, float)):
        try:
            posted_date_str = datetime.fromtimestamp(added).strftime("%Y-%m-%d")
        except (OSError, ValueError):
            pass

    # Property type from categories
    categories = hit.get("categories", {})
    cat_names = categories.get("name", {}).get("en", []) if isinstance(categories, dict) else []
    prop_type = cat_names[0] if cat_names else "Property"

    sqm = hit.get("size")
    bedrooms = hit.get("bedrooms")

    # Location
    city = hit.get("city", {})
    city_name = city.get("name", {}).get("en", "Dubai") if isinstance(city, dict) else "Dubai"
    neighborhoods = hit.get("neighborhoods", {})
    nbh_names = neighborhoods.get("name", {}).get("en", []) if isinstance(neighborhoods, dict) else []
    district = nbh_names[0] if nbh_names else ""
    location_str = f"{district}, {city_name}" if district else city_name

    return {
        "id": ext_id,
        "title": title,
        "url": url,
        "type": prop_type,
        "sqm": sqm,
        "bedrooms": bedrooms,
        "location": location_str,
        "district": district,
        "price_usd": price,  # Actually AED but same field name for consistency
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
        print("  \u2192 No hits found on page 1, stopping.")
        return listings

    max_page = min(nb_pages, MAX_PAGES_PER_CATEGORY)
    print(f"  \u2192 Page 1: {len(hits)} hits, {nb_pages} total pages (scraping up to {max_page})")

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
            print(f"  \u2192 No hits on page {page}, stopping.")
            break

        for hit in hits:
            parsed = parse_hit(hit)
            if parsed:
                listings.append(parsed)
        print(f"  \u2192 {len(listings)} valid listings so far")

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
                        (existing["original_price"] - new_price)
                        / existing["original_price"]
                        * 100,
                        1,
                    )
                    existing["last_drop_date"] = today
                    drops += 1
                updated += 1
            existing["title"] = listing["title"]
            existing["url"] = listing["url"]
            existing["last_seen"] = today
            if "posted_date" not in existing:
                existing["posted_date"] = listing.get("posted_date") or existing["first_seen"]
        else:
            db[lid] = {
                "id": lid,
                "title": listing["title"],
                "url": listing["url"],
                "type": listing.get("type", "Property"),
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
                "type": listing.get("type", "Property"),
                "sqm": listing.get("sqm"),
                "bedrooms": listing.get("bedrooms"),
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
                "type": listing.get("type", "Property"),
                "sqm": listing.get("sqm"),
                "bedrooms": listing.get("bedrooms"),
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
    print("  Dubizzle Dubai Property Scraper")
    print(f"  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    db = load_db()
    print(f"\n\U0001f4e6 Loaded database: {len(db)} existing listings\n")

    all_listings = []
    for cat_url in CATEGORY_URLS:
        print(f"\n\U0001f50d Scraping: {cat_url}")
        listings = scrape_category(cat_url)
        all_listings.extend(listings)
        print(f"  \u2713 Got {len(listings)} listings from this category")

    print(f"\n\U0001f4ca Total scraped: {len(all_listings)} listings")

    seen = set()
    unique = []
    for item in all_listings:
        if item["id"] not in seen:
            seen.add(item["id"])
            unique.append(item)
    print(f"\U0001f4ca Unique listings: {len(unique)}")

    new_count, updated, drops = update_database(db, unique)
    print(f"\n\u2705 Results:")
    print(f"   New listings: {new_count}")
    print(f"   Price changes: {updated}")
    print(f"   Price drops: {drops}")

    save_db(db)
    print(f"\n\U0001f4be Saved database: {len(db)} total listings \u2192 {DB_FILE}")

    feed = generate_drops_feed(db)
    save_drops(feed)
    print(f"\U0001f4e1 Generated drops feed: {feed['total_drops']} drops \u2192 {DROPS_FILE}")

    today = datetime.now()
    stale = 0
    for lid, listing in db.items():
        last_seen = datetime.strptime(listing["last_seen"], "%Y-%m-%d")
        if (today - last_seen).days > 7:
            listing["stale"] = True
            stale += 1
    if stale:
        print(f"\u26a0\ufe0f  {stale} listings not seen in 7+ days (possibly sold/removed)")
        save_db(db)

    print(f"\n{'=' * 60}")
    print("  Done! Dashboard data ready.")
    print(f"{'=' * 60}\n")


if __name__ == "__main__":
    main()
