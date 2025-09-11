"""
HTML parsing module for AnyWho search results.
"""

import re
from typing import Dict, Any, List, Optional, Set
from bs4 import BeautifulSoup

from src.processors.data_processor import normalize_phone, extract_phones_from_text
from src.processors.data_processor import is_likely_name, is_likely_address
from src.config.settings import PHONE_PATTERNS

def parse_anywho_results(html: str) -> List[Dict[str, Any]]:
    """Enhanced parsing of AnyWho search results with comprehensive CSS selectors"""
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    candidates = []

    # Strategy 1: Comprehensive CSS selectors for AnyWho profiles
    result_selectors = [
        # Common AnyWho patterns
        'div[class*="result"]',
        'div[class*="listing"]',
        'div[class*="person"]',
        'div[class*="contact"]',
        'div[class*="profile"]',
        'li[class*="result"]',
        'article[class*="person"]',
        'section[class*="result"]',

        # Specific AnyWho classes that might exist
        '.result-item',
        '.person-result',
        '.listing-result',
        '.contact-result',
        '.profile-card',
        '.person-card',
        '.contact-card',

        # Data attributes
        '[data-person]',
        '[data-result]',
        '[data-contact]',
        '[data-profile]',

        # Generic containers that might hold person data
        'div:has(h1,h2,h3,h4,strong,b)',  # Any div with headings
        'div:has(phone,a[href*="tel"])',  # Divs with phone links

        # Look for any div that contains both a name-like element and phone
        'div:has([class*="name"]),div:has(h1,h2,h3,h4,strong,b)',

        # Very broad fallback - any div with substantial text content
        'div:not([class]):not([id])',  # Plain divs
    ]

    for selector in result_selectors:
        try:
            results = soup.select(selector)
            for result in results:
                # Skip very small or empty elements
                text_content = result.get_text(' ', strip=True)
                if len(text_content) < 20:
                    continue

                candidate = parse_result_block(result)
                if candidate and candidate.get('phone'):
                    candidates.append(candidate)
        except Exception:
            continue

    # Strategy 2: If still no candidates, try phone element walk-up
    if not candidates:
        phone_elements = soup.find_all(text=re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'))

        for phone_elem in phone_elements:
            container = phone_elem.parent
            for _ in range(5):
                if container and container.name in ['div', 'li', 'article', 'section']:
                    text_content = container.get_text(' ', strip=True)
                    if len(text_content) > 30:
                        candidate = parse_result_block(container)
                        if candidate and candidate.get('phone'):
                            candidates.append(candidate)
                        break
                container = container.parent if container else None

    # Strategy 3: Text-based fallback
    if not candidates:
        candidates = parse_text_based_results(soup)

    # Remove duplicates by phone number
    unique_candidates = []
    seen_phones = set()
    for candidate in candidates:
        phone = candidate.get('phone', '')
        if phone and phone not in seen_phones:
            unique_candidates.append(candidate)
            seen_phones.add(phone)

    return unique_candidates

def find_profile_cards(soup: BeautifulSoup) -> List[Any]:
    """Locate all profile cards that contain a PHONE NUMBER(S) section.

    Uses multiple strategies to find all profile cards on the page, not just the first one.
    We prioritize containers that clearly resemble profile cards by searching for sections
    that include headings such as PHONE NUMBER(S):, LIVES IN:, AKA:, etc.
    """
    cards: List[Any] = []
    seen: Set[int] = set()

    # Strategy 1: Find elements containing the phone numbers header
    header_elems = soup.find_all(string=re.compile(r"PHONE\s+NUMBER\(S\):", re.IGNORECASE))

    for header in header_elems:
        container = getattr(header, 'parent', None)
        if container:
            for level in range(4):
                if container and container.parent and container.parent.name not in ('html', 'body'):
                    container = container.parent
                else:
                    break

            if container is not None and id(container) not in seen:
                container_text = container.get_text(' ', strip=True)
                if len(container_text) > 50 and len(container_text) < 10000:
                    seen.add(id(container))
                    cards.append(container)

    # Strategy 2: Look for other profile indicators if no cards found yet
    if not cards:
        lives_elems = soup.find_all(string=re.compile(r"LIVES\s+IN:", re.IGNORECASE))

        for lives_header in lives_elems:
            container = getattr(lives_header, 'parent', None)
            if container:
                for level in range(4):
                    if container and container.parent and container.parent.name not in ('html', 'body'):
                        container = container.parent
                    else:
                        break

                if container and id(container) not in seen:
                    container_text = container.get_text(' ', strip=True)
                    if len(container_text) > 50:
                        seen.add(id(container))
                        cards.append(container)

    # Strategy 3: Look for name headings that might indicate profile cards
    if len(cards) < 2:
        name_headings = soup.find_all(['h1', 'h2', 'h3', 'h4'])
        for heading in name_headings:
            heading_text = heading.get_text(' ', strip=True)
            if (re.search(r'Age\s+\d+', heading_text, re.IGNORECASE) or
                len(heading_text.split()) >= 2):

                container = heading.parent
                for level in range(3):
                    if container and container.parent:
                        container = container.parent
                    else:
                        break

                if container and id(container) not in seen:
                    container_text = container.get_text(' ', strip=True)
                    if (len(container_text) > 50 and
                        re.search(r'\d{3}.*\d{3}.*\d{4}', container_text)):
                        seen.add(id(container))
                        cards.append(container)

    return cards

def _extract_section_text(card_text: str, start_label: str, labels: List[str]) -> str:
    """Extract text between a start label and the next label from the list.

    The search is done in a case-insensitive way on the full card text.
    """
    text_upper = card_text.upper()
    start_upper = start_label.upper()
    start_idx = text_upper.find(start_upper)
    if start_idx == -1:
        return ""

    # Find the next section label after start
    next_indices = []
    for label in labels:
        idx = text_upper.find(label.upper(), start_idx + len(start_upper))
        if idx != -1:
            next_indices.append(idx)

    end_idx = min(next_indices) if next_indices else len(card_text)
    return card_text[start_idx + len(start_upper):end_idx].strip()

def extract_phone_section(card_element) -> List[str]:
    """Extract phone numbers exclusively from the PHONE NUMBER(S): section."""
    card_text = card_element.get_text('\n', strip=True)
    # Labels observed on cards; used to bound sections
    labels = [
        'PHONE NUMBER(S):', 'LIVES IN:', 'USED TO LIVE IN:', 'EMAILS:',
        'MAY BE RELATED TO:', 'SOCIAL PROFILES:', 'AKA:', 'RESULTS', 'SUMMARY'
    ]

    phone_block = _extract_section_text(card_text, 'PHONE NUMBER(S):', labels)
    if not phone_block:
        return []

    # Extract phone numbers within the bounded phone section only
    phones: Set[str] = set()
    for pattern in PHONE_PATTERNS:
        matches = pattern.findall(phone_block)
        for match in matches:
            digits = ''.join(match) if isinstance(match, tuple) else match
            norm = normalize_phone(digits)
            if norm:
                phones.add(norm)

    return list(phones)

def extract_name_age(card_element) -> Dict[str, Any]:
    """Extract name and age from the top of the card if present."""
    # Prefer heading tags
    for tag in ['h1', 'h2', 'h3']:
        heading = card_element.find(tag)
        if heading:
            text = heading.get_text(' ', strip=True)
            age_match = re.search(r'Age\s+(\d+)', text, re.IGNORECASE)
            return {
                'name': re.sub(r'\,?\s*Age\s+\d+', '', text, flags=re.IGNORECASE).strip(),
                'age': age_match.group(1) if age_match else ''
            }

    # Fallback: first strong/bold line
    strong = card_element.find(['strong', 'b'])
    if strong:
        text = strong.get_text(' ', strip=True)
        age_match = re.search(r'Age\s+(\d+)', text, re.IGNORECASE)
        return {
            'name': re.sub(r'\,?\s*Age\s+\d+', '', text, flags=re.IGNORECASE).strip(),
            'age': age_match.group(1) if age_match else ''
        }

    # Final fallback: first line
    first_line = card_element.get_text('\n', strip=True).split('\n', 1)[0]
    age_match = re.search(r'Age\s+(\d+)', first_line, re.IGNORECASE)
    return {
        'name': re.sub(r'\,?\s*Age\s+\d+', '', first_line, flags=re.IGNORECASE).strip(),
        'age': age_match.group(1) if age_match else ''
    }

def extract_address_section(card_element) -> List[str]:
    """Extract ALL addresses from both LIVES IN and USED TO LIVE IN sections."""
    addresses = []

    card_text = card_element.get_text('\n', strip=True)
    labels = [
        'PHONE NUMBER(S):', 'LIVES IN:', 'USED TO LIVE IN:', 'EMAILS:',
        'MAY BE RELATED TO:', 'SOCIAL PROFILES:', 'AKA:'
    ]

    # Extract current address
    current_block = _extract_section_text(card_text, 'LIVES IN:', labels)
    if current_block:
        current_addr = current_block.split('\n')[0].strip()
        if current_addr:
            addresses.append(current_addr)

    # Extract previous addresses
    prev_block = _extract_section_text(card_text, 'USED TO LIVE IN:', labels)
    if prev_block:
        # Split on bullets (•) and clean up
        prev_addresses = re.split(r'\s*•\s*|\n', prev_block)
        for addr in prev_addresses:
            addr = addr.strip()
            if addr and len(addr) > 5:  # Filter out very short fragments
                addresses.append(addr)

    return addresses

def extract_aka_section(card_element) -> List[str]:
    """Extract AKA names from the card if present."""
    card_text = card_element.get_text('\n', strip=True)
    labels = [
        'PHONE NUMBER(S):', 'LIVES IN:', 'USED TO LIVE IN:', 'EMAILS:',
        'MAY BE RELATED TO:', 'SOCIAL PROFILES:', 'AKA:'
    ]
    aka_block = _extract_section_text(card_text, 'AKA:', labels)
    if not aka_block:
        return []
    # Split on bullets or separators
    parts = re.split(r'\s+•\s+|\s{2,}|,\s*', aka_block)
    return [p.strip() for p in parts if len(p.strip()) >= 3][:10]

def validate_profile_card(card_element) -> bool:
    """Basic validation to ensure this is a person profile card."""
    text = card_element.get_text(' ', strip=True)
    return (
        re.search(r'PHONE\s+NUMBER\(S\):', text, re.IGNORECASE) is not None and
        (re.search(r'LIVES\s+IN:', text, re.IGNORECASE) is not None or
         re.search(r'USED\s+TO\s+LIVE\s+IN:', text, re.IGNORECASE) is not None)
    )

def extract_profile_data(card, name_variant: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Extract structured candidate data from a profile card."""
    if not validate_profile_card(card):
        return None

    phones = extract_phone_section(card)
    if not phones:
        return None

    name_age = extract_name_age(card)
    addresses = extract_address_section(card)
    aka_names = extract_aka_section(card)

    # Filter out JavaScript/React code masquerading as names
    name = name_age.get('name', '')
    if len(name) > 100 or 'function' in name.lower() or 'react' in name.lower():
        return None

    return {
        'name': name,
        'addresses': addresses,  # Now stores list of all addresses
        'address': addresses[0] if addresses else '',  # Backward compatibility
        'phone': phones[0],
        'all_phones': phones,
        'aka': aka_names,
        'raw_text': card.get_text(' ', strip=True)[:500],
        'search_variant': name_variant.get('variant_type', 'basic')
    }

def parse_profile_cards(html: str, name_variant: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Parse the response HTML focusing on profile cards and phone section."""
    if not html:
        return []

    soup = BeautifulSoup(html, 'html.parser')
    candidates: List[Dict[str, Any]] = []

    # Method 1: Find profile cards with PHONE NUMBER(S) headers
    cards = find_profile_cards(soup)
    for card in cards:
        data = extract_profile_data(card, name_variant)
        if data and data.get('phone'):
            candidates.append(data)

    # Method 2: If no candidates found, try broader CSS selector approach
    if not candidates:
        candidates = parse_anywho_results(html)

    # Method 3: Enhanced fallback - look for any elements that might contain person data
    if not candidates:
        candidates = parse_text_based_results(soup)

    # Method 4: Ultimate fallback - scan for any phone numbers and try to associate with names
    if not candidates:
        candidates = parse_ultimate_fallback(html)

    return candidates

def parse_result_block(block) -> Optional[Dict[str, Any]]:
    """Parse a single result block to extract name, address, and phone"""
    try:
        block_text = block.get_text(' ', strip=True)

        # Extract phones from this block
        phones = extract_phones_from_text(block_text, PHONE_PATTERNS)
        if not phones:
            return None

        # Extract name - look for various patterns
        name = extract_name_from_block(block, block_text)

        # Extract address - look for address patterns
        address = extract_address_from_block(block, block_text)

        return {
            'name': name,
            'addresses': [address] if address else [],  # Store as list for consistency
            'address': address,  # Backward compatibility
            'phone': phones[0],  # Take first phone
            'all_phones': phones,
            'raw_text': block_text[:500]  # Keep for debugging
        }
    except Exception:
        return None

def extract_name_from_block(block, block_text: str) -> str:
    """Extract name using multiple strategies"""
    # Strategy 1: Look for heading tags
    for tag in ['h1', 'h2', 'h3', 'h4', 'strong', 'b']:
        heading = block.find(tag)
        if heading:
            name_text = heading.get_text(strip=True)
            if name_text and len(name_text.split()) >= 2:
                return name_text
    
    # Strategy 2: Look for name-specific CSS classes
    name_selectors = [
        '[class*="name"]',
        '[class*="person"]',
        '[class*="contact-name"]',
        '[data-name]'
    ]
    
    for selector in name_selectors:
        name_elem = block.select_one(selector)
        if name_elem:
            name_text = name_elem.get_text(strip=True)
            if name_text and len(name_text.split()) >= 2:
                return name_text
    
    # Strategy 3: Extract from first line that looks like a name
    lines = block_text.split('\n')
    for line in lines[:3]:  # Check first 3 lines
        line = line.strip()
        if line and is_likely_name(line):
            return line
    
    return ""

def extract_address_from_block(block, block_text: str) -> str:
    """Extract address using multiple strategies"""
    # Strategy 1: Look for address tags
    address_elem = block.find('address')
    if address_elem:
        return address_elem.get_text(' ', strip=True)
    
    # Strategy 2: Look for address-specific CSS classes
    addr_selectors = [
        '[class*="address"]',
        '[class*="location"]',
        '[class*="street"]',
        '[data-address]'
    ]
    
    for selector in addr_selectors:
        addr_elem = block.select_one(selector)
        if addr_elem:
            addr_text = addr_elem.get_text(' ', strip=True)
            if addr_text and is_likely_address(addr_text):
                return addr_text
    
    # Strategy 3: Find address patterns in text
    lines = block_text.split('\n')
    for line in lines:
        line = line.strip()
        if line and is_likely_address(line):
            return line
    
    return ""

def parse_text_based_results(soup) -> List[Dict[str, Any]]:
    """Fallback: parse results from general text patterns"""
    candidates = []

    # Find all text nodes with phone numbers
    all_text = soup.get_text('\n', strip=True)
    phones = extract_phones_from_text(all_text, PHONE_PATTERNS)

    if not phones:
        return candidates

    # Try to find context around each phone number
    lines = all_text.split('\n')
    for i, line in enumerate(lines):
        line_phones = extract_phones_from_text(line, PHONE_PATTERNS)
        if line_phones:
            # Look for name in nearby lines
            context_lines = lines[max(0, i-2):i+3]
            context_text = ' '.join(context_lines)

            name = ""
            address = ""

            for ctx_line in context_lines:
                ctx_line = ctx_line.strip()
                if not name and is_likely_name(ctx_line):
                    name = ctx_line
                elif not address and is_likely_address(ctx_line):
                    address = ctx_line

            candidates.append({
                'name': name,
                'address': address,
                'phone': line_phones[0],
                'all_phones': line_phones,
                'raw_text': context_text[:500]
            })

    return candidates

def parse_ultimate_fallback(html: str) -> List[Dict[str, Any]]:
    """Ultimate fallback: scan entire HTML for any phone/name combinations"""
    candidates = []

    if not html:
        return candidates

    soup = BeautifulSoup(html, 'html.parser')

    # Extract all phone numbers from the entire page
    all_text = soup.get_text(' ', strip=True)
    all_phones = extract_phones_from_text(all_text, PHONE_PATTERNS)

    if not all_phones:
        return candidates

    # Look for name-like text near each phone number
    # Split by potential separators and look for patterns
    text_blocks = re.split(r'[•\n]{2,}|\s{4,}', all_text)

    for block in text_blocks:
        block = block.strip()
        if not block:
            continue

        block_phones = extract_phones_from_text(block, PHONE_PATTERNS)
        if not block_phones:
            continue

        # Extract potential names from this block
        lines = block.split('\n')
        name_candidates = []

        for line in lines:
            line = line.strip()
            # Look for lines that might be names (contain letters, reasonable length)
            if (len(line) > 3 and len(line) < 50 and
                re.search(r'[A-Za-z]{2,}', line) and
                not re.search(r'\d{4,}', line)):  # Avoid lines with long numbers
                name_candidates.append(line)

        # Take the most likely name (first one that's not too long)
        best_name = ""
        for name_cand in name_candidates[:3]:  # Check first 3 candidates
            if len(name_cand.split()) >= 2 and len(name_cand) < 30:
                best_name = name_cand
                break

        # If no good name found, use first name candidate
        if not best_name and name_candidates:
            best_name = name_candidates[0]

        # Look for address in the block
        address = ""
        for line in lines:
            if is_likely_address(line):
                address = line
                break

        if best_name or address:  # At least some identifying info
            candidates.append({
                'name': best_name,
                'address': address,
                'phone': block_phones[0],
                'all_phones': block_phones,
                'raw_text': block[:500]
            })

    return candidates
