"""
Web scraping module for AnyWho phone lookups.
Enhanced with modern anti-detection measures.
"""

import time
import random
from typing import Dict, Any, List, Optional, Tuple
import requests

from src.processors.data_processor import normalize_name, normalize_address
from src.parsers.anywho_parser import parse_profile_cards
from src.algorithms.scoring import select_top_two_groups_phones
from src.config.settings import ANYWHO_PEOPLE, DEFAULT_USER_AGENT, DEFAULT_HEADERS, DEFAULT_REQUEST_TIMEOUT

# Modern user agents (updated for 2024)
MODERN_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
]

def get_random_user_agent() -> str:
    """Get a random modern user agent"""
    return random.choice(MODERN_USER_AGENTS)

def build_enhanced_session(ua: Optional[str] = None, initialize_session: bool = True) -> requests.Session:
    """Create session with realistic browser headers and Cloudflare bypass measures"""
    s = requests.Session()

    # Use random modern user agent if not specified
    user_agent = ua or get_random_user_agent()

    # Generate realistic Sec-Ch-Ua values based on user agent
    chrome_version = "131"
    if "Chrome/130" in user_agent:
        chrome_version = "130"
    elif "Chrome/129" in user_agent:
        chrome_version = "129"

    # Enhanced headers with Cloudflare bypass measures
    headers = {
        "User-Agent": user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Fetch-User": "?1",
        f'Sec-Ch-Ua': f'"Google Chrome";v="{chrome_version}", "Chromium";v="{chrome_version}", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Cache-Control": "max-age=0",
        "DNT": "1",

        # Cloudflare bypass headers
        "CF-IPCountry": "US",
        "CF-Visitor": '{"scheme":"https"}',

        # Start with Google referer (will be updated)
        "Referer": "https://www.google.com/",
    }

    s.headers.update(headers)

    # Initialize session by visiting homepage first (like real browsers)
    if initialize_session:
        try:
            print("  Initializing session by visiting AnyWho homepage...")
            # Visit homepage with search engine referer
            s.headers.update({"Referer": "https://www.google.com/search?q=phone+lookup"})
            home_response = s.get("https://www.anywho.com/", timeout=DEFAULT_REQUEST_TIMEOUT)
            home_response.raise_for_status()

            # Small delay to look more human
            time.sleep(random.uniform(1.0, 2.0))

            print("  Session initialized successfully")
        except Exception as e:
            print(f"  Warning: Session initialization failed: {e}")

    return s

def build_session(ua: Optional[str] = None) -> requests.Session:
    """Create session with realistic browser headers (legacy function for compatibility)"""
    return build_enhanced_session(ua)

def add_random_delay(base_delay: float = 1.0) -> float:
    """Add random delay to prevent detection"""
    delay = base_delay + random.uniform(0.5, 2.0)
    time.sleep(delay)
    return delay

def build_search_urls(name_dict: Dict[str, Any], base_url: str) -> List[Dict[str, str]]:
    """Build search URLs for each name variant"""
    urls = []

    for variant in name_dict["search_variants"]:
        # Build base URL
        search_terms = variant["search_name"].replace(' ', '+')
        url = f"{base_url}/{search_terms}/florida/miami"

        # Add middle name parameter if available
        if variant["middle_name"]:
            url += f"?middle_name={variant['middle_name']}"

        urls.append({
            "url": url,
            "variant_type": variant["variant_type"],
            "search_name": variant["search_name"]
        })

    return urls

def enrich_name(session: requests.Session, name: str, addr: str, sleep_sec: float = 1.0) -> Tuple[Optional[str], Optional[str], Optional[str], Optional[str], List[Dict[str, Any]]]:
    """Enhanced enrichment function with multiple search variants and anti-detection measures"""
    if not name.strip():
        return None, None, None, None, []

    # Enhanced name normalization
    target_name = normalize_name(name)
    target_addr = normalize_address(addr)

    if not target_name['search_variants']:
        return None, None, None, None, []

    all_candidates = []
    successful_variant = None

    # Try each name variant in sequence (basic, then middle_initial)
    for variant in target_name['search_variants']:
        urls = build_search_urls(target_name, ANYWHO_PEOPLE)

        for url_info in urls:
            if url_info['variant_type'] != variant['variant_type']:
                continue

            try:
                print(f"  Searching: {url_info['url']} ({url_info['variant_type']})")

                # Add random delay before request
                actual_delay = add_random_delay(sleep_sec)

                # Update referer to the AnyWho homepage (now that we've visited it)
                session.headers.update({"Referer": "https://www.anywho.com/"})

                # Add some randomization to headers for each request
                session.headers.update({
                    "Sec-Fetch-Site": random.choice(["same-origin", "same-site"]),
                    "Cache-Control": random.choice(["max-age=0", "no-cache"]),
                })

                response = session.get(url_info['url'], timeout=DEFAULT_REQUEST_TIMEOUT)
                response.raise_for_status()

                # Try multiple parsing strategies to find all candidates
                candidates = parse_profile_cards(response.text, variant)

                for candidate in candidates:
                    candidate['search_variant'] = url_info['variant_type']
                    candidate['search_url'] = url_info['url']

                if candidates:
                    all_candidates.extend(candidates)
                    successful_variant = url_info['variant_type']
                    print(f"    Found {len(candidates)} candidates with {url_info['variant_type']} variant")

                # Additional delay between variant attempts
                if sleep_sec > 0:
                    time.sleep(sleep_sec * 0.5)

            except Exception as e:
                print(f"  Error with {url_info['variant_type']}: {e}")

                # Enhanced 403 error handling with exponential backoff
                if "403" in str(e):
                    print(f"  Got 403 error, trying with fresh session and longer delay...")

                    # Exponential backoff: wait longer before retry
                    backoff_delay = sleep_sec * 3 + random.uniform(2.0, 5.0)
                    print(f"  Backing off for {backoff_delay:.1f} seconds...")
                    time.sleep(backoff_delay)

                    # Try with completely fresh session
                    fresh_session = build_enhanced_session()
                    try:
                        fresh_session.headers.update({"Referer": "https://www.anywho.com/"})
                        response = fresh_session.get(url_info['url'], timeout=DEFAULT_REQUEST_TIMEOUT)
                        response.raise_for_status()

                        candidates = parse_profile_cards(response.text, variant)
                        if candidates:
                            all_candidates.extend(candidates)
                            successful_variant = url_info['variant_type']
                            print(f"    Found {len(candidates)} candidates with fresh session")
                    except Exception as e2:
                        print(f"  Fresh session also failed: {e2}")

                        # If still failing, try one more time with different approach
                        if "403" in str(e2):
                            print("  Attempting final retry with different user agent and longer delay...")
                            time.sleep(sleep_sec * 5)  # Even longer delay
                            final_session = build_enhanced_session()
                            try:
                                final_session.headers.update({"Referer": "https://www.google.com/search?q=anywho+phone+lookup"})
                                response = final_session.get(url_info['url'], timeout=DEFAULT_REQUEST_TIMEOUT)
                                response.raise_for_status()

                                candidates = parse_profile_cards(response.text, variant)
                                if candidates:
                                    all_candidates.extend(candidates)
                                    successful_variant = url_info['variant_type']
                                    print(f"    Found {len(candidates)} candidates with final retry")
                            except Exception as e3:
                                print(f"  Final retry also failed: {e3}")
                continue

    if not all_candidates:
        print(f"  No candidates found for {name}")
        return None, None, None, None, []

    print(f"  Found {len(all_candidates)} candidates total")

    # Select phones from top two person-groups
    phone1, phone2, phone3, phone4 = select_top_two_groups_phones(all_candidates, target_name, target_addr)

    # Build final candidate list for visibility (top picks only)
    final_candidates: List[Dict[str, Any]] = []
    for cand in all_candidates:
        if cand.get('phone') in {phone1, phone2, phone3, phone4}:
            final_candidates.append(cand)

    # Return primary two phones (backwards compatible), but stash others in candidates
    # Downstream save logic will read Phone3/Phone4 from cache or context
    # To maintain function signature, we return only first two; extra phones go in candidate metadata
    for cand in final_candidates:
        if cand.get('phone') == phone1:
            cand['rank'] = 1
        elif cand.get('phone') == phone2:
            cand['rank'] = 2
        elif cand.get('phone') == phone3:
            cand['rank'] = 3
        elif cand.get('phone') == phone4:
            cand['rank'] = 4

    return phone1, phone2, phone3, phone4, final_candidates
