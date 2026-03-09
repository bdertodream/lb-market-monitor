#!/usr/bin/env python3
"""
Dubizzle Dubai Car Scraper
Uses Algolia API directly to bypass bot protection.
"""

import json
import os
import time
import urllib.request
import urllib.error
import urllib.parse
from datetime import datetime, timezone

# Algolia config (public search-only key from dubizzle frontend)
ALGOLIA_APP_ID = "WD0PTZ13ZS"
ALGOLIA_API_KEY = "cef139620248f1bc328a00fddc7107a6"
ALGOLIA_URL = f"https://{ALGOLIA_APP_ID}-dsn.algolia.net/1/indexes/*/queries"

# Category configs: (filter, index_name, label)
CATEGORIES = [
    (
        '("category_v2.slug_paths":"motors/used-cars") AND ("site.id":"2")',
        "motors.com",
        "Used Cars",
    ),
    (
        '("category_v2.slug_paths":"motors/new-cars") AND ("site.id":"2")',
        "motors.com",
        "New Cars",
    ),
]

MAX_PAGES = 5
HITS_PER_PAGE = 35
DB_FILE = "listings_db_uae_cars.json"
FEED_FILE = "drops_feed_uae_cars.json"
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


def parse_hit(hit):
    """Parse a single car listing hit into a standardized dict."""
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

        # Car-specific details
        details = hit.get("details", {})
        year = ""
        kilometers = ""
        body_type = ""
        fuel_type = ""
        if isinstance(details, dict):
            for k, v in details.items():
                vv = v.get("en", v) if isinstance(v, dict) else str(v)
                if isinstance(vv, list):
                    vv = vv[0] if vv else ""
                kl = k.lower()
                if "year" in kl:
                    year = str(vv)
                elif "kilometer" in kl:
                    kilometers = str(vv)
                elif "body" in kl:
                    body_type = str(vv)
                elif "fuel" in kl:
                    fuel_type = str(vv)

        # Category / make / model
        cats = hit.get("category", {})
        cat_names = cats.get("en", []) if isinstance(cats, dict) else []
        if isinstance(cat_names, list) and len(cat_names) >= 3:
            make = cat_names[1]
            model = cat_names[2]
        elif isinstance(cat_names, list) and len(cat_names) >= 2:
            make = cat_names[1]
            model = ""
        else:
            make = ""
            model = ""

        neighbourhood = hit.get("neighbourhood", {})
        area = neighbourhood.get("en", "") if isinstance(neighbourhood, dict) else str(neighbourhood)

        abs_url = hit.get("absolute_url", {})
        url = abs_url.get("en", "") if isinstance(abs_url, dict) else str(abs_url)
        if url and not url.startswith("http"):
            url = BASE_URL + url

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
            "make": make,
            "model": model,
            "year": year,
            "kilometers": kilometers,
            "body_type": body_type,
            "fuel_type": fuel_type,
            "area": area,
            "url": url,
            "date": date_str,
        }
    except Exception as e:
        print(f"  Parse error: {e}")
        return None


def scrape_all():
    """Scrape all car categories via Algolia."""
    all_listings = []
    for filters, index_name, label in CATEGORIES:
        print(f"\nScraping {label}...")
        for page in range(MAX_PAGES):
            hits, total_pages = algolia_search(index_name, filters, page)
            if not hits:
                print(f"  Page {page}: no hits, stopping")
                break
            print(f"  Page {page}: {len(hits)} hits (of {total_pages} pages)")
            for hit in hits:
                parsed = parse_hit(hit)
                if parsed:
                    parsed["category"] = label
                    all_listings.append(parsed)
            if page + 1 >= total_pages:
                break
            time.sleep(0.5)
    return all_listings


# ── Database helpers ──────────────────────────────────────────────
def load_db():
    if os.path.exists(DB_FILE):
        with open(DB_FILE) as f:
            return json.load(f)
    return {}

def save_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=2)

def load_feed():
    if os.path.exists(FEED_FILE):
        with open(FEED_FILE) as f:
            return json.load(f)
    return {"total_tracked": 0, "drops": [], "new_listings": []}

def save_feed(feed):
    with open(FEED_FILE, "w") as f:
        json.dump(feed, f, indent=2)


# ── Main ──────────────────────────────────────────────────────────
def main():
    print("=== Dubizzle Dubai Car Scraper (Algolia API) ===")
    listings = scrape_all()
    print(f"\nTotal parsed: {len(listings)}")

    db = load_db()
    old_feed = load_feed()
    existing_drops = old_feed.get("drops", old_feed) if isinstance(old_feed, dict) else old_feed
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    new_count = 0
    drop_count = 0
    drops = list(existing_drops) if isinstance(existing_drops, list) else []

    for item in listings:
        lid = item["id"]
        if lid in db:
            old_price = db[lid]["price"]
            if item["price"] < old_price:
                pct = (old_price - item["price"]) / old_price
                db[lid]["prices"].append({"price": item["price"], "date": now})
                db[lid]["price"] = item["price"]
                db[lid]["title"] = item["title"]
                db[lid]["url"] = item["url"]
                if pct >= DROP_THRESHOLD:
                    drop_count += 1
                    drops.append({
                        "id": lid,
                        "title": item["title"],
                        "old_price": old_price,
                        "new_price": item["price"],
                        "drop_pct": round(pct * 100, 1),
                        "url": item["url"],
                        "category": item.get("category", ""),
                        "make": item.get("make", ""),
                        "model": item.get("model", ""),
                        "year": item.get("year", ""),
                        "date": now,
                    })
            elif item["price"] > old_price:
                db[lid]["prices"].append({"price": item["price"], "date": now})
                db[lid]["price"] = item["price"]
        else:
            new_count += 1
            db[lid] = {
                "id": lid,
                "title": item["title"],
                "price": item["price"],
                "make": item.get("make", ""),
                "model": item.get("model", ""),
                "year": item.get("year", ""),
                "kilometers": item.get("kilometers", ""),
                "body_type": item.get("body_type", ""),
                "fuel_type": item.get("fuel_type", ""),
                "area": item.get("area", ""),
                "category": item.get("category", ""),
                "url": item["url"],
                "first_seen": now,
                "prices": [{"price": item["price"], "date": now}],
                "date": item.get("date", ""),
            }

    # Build new_listings from db (listings first seen after war start)
    new_listings = []
    for lid, entry in db.items():
        if entry.get("first_seen", "") >= WAR_START:
            new_listings.append({
                "id": lid,
                "title": entry["title"],
                "url": entry["url"],
                "category": entry.get("category", ""),
                "make": entry.get("make", ""),
                "model": entry.get("model", ""),
                "year": entry.get("year", ""),
                "price": entry["price"],
                "first_seen": entry["first_seen"],
            })
    new_listings.sort(key=lambda x: x.get("first_seen", ""), reverse=True)

    feed = {
        "total_tracked": len(db),
        "total_drops": len(drops),
        "total_new": len(new_listings),
        "last_updated": now,
        "drops": drops[-500:],
        "new_listings": new_listings[:500],
    }

    save_db(db)
    save_feed(feed)
    print(f"New: {new_count} | Drops: {drop_count} | DB size: {len(db)}")

if __name__ == "__main__":
    main()
