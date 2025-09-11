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

def build_enhanced_session(ua: Optional[str] = None) -> requests.Session:
    """Create session with realistic browser headers and anti-detection measures"""
    s = requests.Session()

    # Use random modern user agent if not specified
    user_agent = ua or get_random_user_agent()

    # Enhanced headers that mimic real browser behavior
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
        "Sec-Ch-Ua": '"Google Chrome";v="131", "Chromium";v="131", "Not_A Brand";v="24"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
        "Cache-Control": "max-age=0",
        "DNT": "1",
        "Referer": "https://www.google.com/",
    }

    s.headers.update(headers)
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

                # Update referer to make request look more natural
                session.headers.update({"Referer": "https://www.anywho.com/"})

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
                # If we get a 403, try with a fresh session and different user agent
                if "403" in str(e):
                    print(f"  Got 403 error, trying with fresh session...")
                    session = build_enhanced_session()
                    try:
                        session.headers.update({"Referer": "https://www.anywho.com/"})
                        response = session.get(url_info['url'], timeout=DEFAULT_REQUEST_TIMEOUT)
                        response.raise_for_status()

                        candidates = parse_profile_cards(response.text, variant)
                        if candidates:
                            all_candidates.extend(candidates)
                            successful_variant = url_info['variant_type']
                            print(f"    Found {len(candidates)} candidates with fresh session")
                    except Exception as e2:
                        print(f"  Fresh session also failed: {e2}")
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
