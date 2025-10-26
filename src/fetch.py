# src/fetch - fetch data from listings

import time, csv, random
from pathlib import Path
import requests
from bs4 import BeautifulSoup
import re
import os
from urllib.parse import urljoin, urlparse, parse_qsl, urlencode, urlunparse

def delayed_fetch(url):
    time.sleep(0.5 + random.random()*0.5) # 2-4 sec delay
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

def collect_listing_urls(search_url, max_pages=10, pause=0.4):
    """
    Build pages by setting the `page` query param (page=1..max_pages)
    and extract listing links from each page. Returns deduplicated full URLs.
    """
    seen = set()
    results = []

    parsed = urlparse(search_url)
    def extract_from_html(base_url, html):
        soup = BeautifulSoup(html, "html.parser")
        found = 0
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if re.search(r'/item/\d+', href):
                full = urljoin(base_url, href)
                if full not in seen:
                    seen.add(full)
                    results.append(full)
                    found += 1
        return found

    for page in range(1, max_pages + 1):
        qs = dict(parse_qsl(parsed.query, keep_blank_values=True))
        qs['page'] = str(page)
        new_q = urlencode(qs, doseq=True)
        page_url = urlunparse(parsed._replace(query=new_q))

        print(f"Collecting page {page}: {page_url}")
        try:
            r = delayed_fetch(page_url)
        except Exception as e:
            print("Fetch failed:", e)
            break
        if not r.ok:
            print("HTTP error", r.status_code, "for", page_url)
            break

        found = extract_from_html(page_url, r.text)
        if found == 0:
            print("No listings found on page", page, "- stopping early.")
            break

        time.sleep(pause)

    return results

if __name__ == "__main__":
    search_url = "https://www.dba.dk/recommerce/forsale/search?page=1&product_category=2.93.3216.506"  # change to your search URL
    max_pages = 10          # how many result pages to crawl
    out = Path(__file__).resolve().parent.parent / "data" / "raw_auto.csv"
    out.parent.mkdir(parents=True, exist_ok=True)

    urls_file = out.parent / "listing_urls.txt"

    # 1) collect or load listing URLs
    if urls_file.exists():
        listing_urls = [line.strip() for line in urls_file.read_text(encoding="utf-8").splitlines() if line.strip()]
        print(f"Loaded {len(listing_urls)} URLs from {urls_file}")
    else:
        listing_urls = collect_listing_urls(search_url, max_pages=max_pages, pause=0.4)
        urls_file.write_text("\n".join(listing_urls), encoding="utf-8")
        print(f"Collected {len(listing_urls)} URLs and saved to {urls_file}")

    # 2) open CSV (append mode), write header only if new
    write_header = not out.exists()
    with out.open("a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f,
            fieldnames=["post_id", "url", "title", "price_dkk", "desc", "location", "date", "condition_text"],
            quoting=csv.QUOTE_ALL, quotechar='"', escapechar='\\'
        )
        if write_header:
            w.writeheader()

        # load seen post_ids to avoid duplicates
        seen = set()
        if not write_header:
            import csv as _csv
            with out.open("r", encoding="utf-8") as rf:
                reader = _csv.DictReader(rf)
                for row in reader:
                    if row.get("post_id"):
                        seen.add(row["post_id"])

        # 3) iterate listings and parse
        for idx, u in enumerate(listing_urls, start=1):
            print(f"[{idx}/{len(listing_urls)}] Fetching {u}")
            try:
                r = delayed_fetch(u)
            except Exception as e:
                print("Fetch failed:", e)
                continue
            if not r.ok:
                print("HTTP error", r.status_code, "for", u)
                continue

            item = parse_listing(r.text, u)
            pid = item.get("post_id")
            if pid in seen:
                print("SKIP already seen", pid)
                continue

            w.writerow(item)
            f.flush()
            try:
                os.fsync(f.fileno())
            except Exception:
                pass
            seen.add(pid)
            print("WROTE", pid)