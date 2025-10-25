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
    title = title_el.string

    # annonce-id

    #annonce_id_el = soup.select_one('section[data-testid="object-info"]')
    #if annonce_id_el:
        
    #annonce_id_el_p = annonce_id_el.select_one('p').string
    #print(annonce_id_el_p)
    

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

    # Condition: Initialize variable and handle the parsing more safely
    condition_text_el = None
    info_section = soup.select_one('span[class="flex gap-8 border rounded-full py-8 px-16"]')
    
    if info_section:
        condition_text_el = info_section.select_one("b")
        condition_text = condition_text_el.string if condition_text_el else "Not specified"
    else:
        condition_text = "Not specified"



    # Description: use data-testid description if present
    desc_el = soup.select_one('div.whitespace-pre-wrap') or soup.select_one('section[data-testid="description"]')
    if desc_el:
        p_tags = desc_el.find_all('p')
        
        if p_tags:
            # Use direct string property instead of get_text()
            paragraphs = []
            for p in p_tags:
                # Use .string or decode_contents() instead of get_text()
                text = p.string or p.decode_contents()
                if text:
                    # Clean up any HTML entities and strip whitespace
                    text = text.strip()
                    if text:
                        paragraphs.append(text)
            
            desc = '\n'.join(paragraphs)
        else:
            desc = desc_el.decode_contents().strip()
    else:
        desc = "Ingen desc (auto)"

    # Date


    # Location: map-link or fallback selectors
    loc_el = soup.select_one('span[data-testid="object-address"]')
    location = loc_el.string

    # Get annonce-id and date from object-info section
    annonce_id = None
    date = ""
    info_section = soup.select_one('section[data-testid="object-info"]')
    if info_section:
        # Get all p tags in the section
        info_texts = [p.get_text(strip=True) for p in info_section.find_all('p')]
        
        # Find ID (usually last line with just numbers)
        for text in info_texts:
            if text.isdigit():
                annonce_id = text
                break
        
        # Find date (line starting with "Sidst redigeret")
        for text in info_texts:
            if "redigeret" in text:
                date = text.replace("Sidst redigeret", "").strip()
                break

    # Fallback for ID if not found
    if not annonce_id:
        url_match = re.search(r'/item/(\d+)', url)
        annonce_id = url_match.group(1) if url_match else "unknown"

    # Small debug output to help you confirm parsing; remove later
    print("URL:", url)
    print("Title:", title)
    print("Price text:", price_text)
    print("Parsed price (int):", price_num)
    print("Condition:", condition_text)
    print("-" * 40)

    return {
        "post_id": annonce_id,
        "url": url,
        "title": title,
        "price_dkk": price_num,
        "desc": desc,
        "location": location,
        "date": date,
        "condition_text": condition_text
    }

if __name__ == "__main__":
    urls = [
        "https://www.dba.dk/recommerce/forsale/item/9416406",
        "https://www.dba.dk/recommerce/forsale/item/3458498",
        "https://www.dba.dk/recommerce/forsale/item/14769358"
    ]
    out = Path("data/raw_auto.csv")
    out.parent.mkdir(parents=True, exist_ok=True)

    with out.open("w", newline="", encoding="utf-8") as f:
        # Use quoting=csv.QUOTE_ALL to properly escape fields containing commas
        w = csv.DictWriter(f, 
            fieldnames=["post_id", "url", "title", "price_dkk", "desc", "location", "date", "condition_text"],
            quoting=csv.QUOTE_ALL,  # Quote all fields
            quotechar='"',          # Use double quotes
            escapechar='\\'         # Use backslash to escape quotes within fields
        )
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