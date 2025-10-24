# src/fetch - fetch data from listings

import time, csv, random
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import re
import os

def delayed_fetch(url):
    time.sleep(2 + random.random()*2) # 2-4 sec delay
    h = {"User-Agent":"Mozilla/5.0"}
    return requests.get(url, headers=h, timeout=15)

def parse_listing(html, url):
    soup = BeautifulSoup(html, 'html.parser')

    # Title (exact selector from your screenshot)
    title_el = soup.select_one('h1[data-testid="object-title"]') or soup.select_one('h1[class="break-words mb-24"]')

    raw = title_el.decode_contents() if title_el else ""
    print("codepoints:", [hex(ord(c)) for c in raw[:40]])
    title = title_el.string
    print(f"TITLE MF: {title}")

    # Price: prefer <p class="h2"> (your screenshot) else fallback to regex search for "kr"
    price_el = soup.select_one('p.h2') or soup.find(text=re.compile(r'\d{2,}\s*(?:kr|kr\.|,-)', re.IGNORECASE))
    price_text = None
    if price_el:
        price_text = price_el.get_text(" ", strip=True) if hasattr(price_el, 'get_text') else str(price_el)

    if not price_text:
        m = re.search(r'(\d{1,3}(?:[.\s]\d{3})*|\d{3,6})\s*(?:kr|kr\.|,-)', html, flags=re.IGNORECASE)
        price_text = m.group(0) if m else None

    def extract_int(s):
        if not s:
            return None
        s = s.replace(u'\xa0', ' ')
        m = re.search(r'(\d{1,3}(?:[.\s]\d{3})*|\d{3,6})', s)
        if not m:
            return None
        num = re.sub(r'[.\s]', '', m.group(1))
        try:
            return int(num)
        except:
            return None

    price_num = extract_int(price_text)

    # Condition: "Mere information" section contains <b>Brugt - men i god stand</b> in your screenshot
    condition_text = "Not specified"
    info_section = soup.find('section', attrs={'aria-label': 'Mere information'}) or soup.find('section', attrs={'aria-label': re.compile(r'Mere', re.I)})
    if info_section:
        b = info_section.find('b')
        if b:
            condition_text = b.get_text(strip=True)
        else:
            p = info_section.find('p')
            if p:
                condition_text = p.get_text(" ", strip=True)

    # Description: use data-testid description if present
    desc_el = soup.select_one('section[data-testid="description"]') or soup.select_one('section.about-section') or soup.select_one('meta[name="description"]')
    if desc_el:
        desc = desc_el.get_text(" ", strip=True) if hasattr(desc_el, 'get_text') else desc_el.get('content', '')
    else:
        desc = ""

    # Location: map-link or fallback selectors
    loc_el = soup.select_one('a[data-testid="map-link"]') or soup.select_one('.location') or soup.select_one('div.vip-location')
    location = loc_el.get_text(" ", strip=True) if loc_el else ""

    # Small debug output to help you confirm parsing; remove later
    print("URL:", url)
    print("Title:", title)
    print("Price text:", price_text)
    print("Parsed price (int):", price_num)
    print("Condition:", condition_text)
    print("-" * 40)

    return {
        "url": url,
        "title": title,
        "price_dkk": price_num,
        "desc": desc,
        "location": location,
        "date": "",
        "condition_text": condition_text
    }

if __name__ == "__main__":
    urls = [
        "https://www.dba.dk/recommerce/forsale/item/9416406",
        "https://www.dba.dk/recommerce/forsale/item/3458498",
        "https://www.dba.dk/recommerce/forsale/item/14769358"
    ]
    out = Path("data/raw_auto.csv")
    # create data directory if missing
    out.parent.mkdir(parents=True, exist_ok=True)

    print("CSV path:", out.resolve())

    with out.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["url", "title", "price_dkk", "desc", "location", "date", "condition_text"])
        w.writeheader()
        for u in urls:
            try:
                r = delayed_fetch(u)
            except Exception as e:
                print(f"Fetch failed for {u}: {e}")
                continue
            if r.ok:
                item = parse_listing(r.text, u)
                if not item:
                    print(f"No item parsed for {u}")
                    continue

                # If price parsed, write normally and log.
                if item.get("price_dkk") is not None:
                    w.writerow(item)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except Exception:
                        pass
                    print(f"WROTE row for {u} (price {item.get('price_dkk')})")
                else:
                    # Debug: also write a row with empty price so you can inspect failures
                    debug_item = {**item, "price_dkk": ""}
                    w.writerow(debug_item)
                    f.flush()
                    try:
                        os.fsync(f.fileno())
                    except Exception:
                        pass
                    print(f"WROTE debug row for {u} (missing price) — check the CSV to inspect HTML/fields")
            else:
                print(f"HTTP error {r.status_code} for {u}")