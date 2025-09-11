"""
Scoring algorithms for candidate matching and ranking.
"""

import re
from typing import Dict, Any, List, Set
from rapidfuzz import fuzz

def score_candidate(target_name: Dict[str, Any], target_addr: Dict[str, Any], candidate: Dict[str, Any]) -> float:
    """Enhanced scoring algorithm with multiple criteria and empty address handling"""
    if not candidate.get('phone'):
        return 0

    total_score = 0
    max_score = 0

    # Check if target address is empty and adjust weights accordingly
    has_target_address = bool(target_addr.get('raw') and target_addr['raw'].strip())
    candidate_addresses = candidate.get('addresses', [candidate.get('address', '')])

    if has_target_address:
        # Normal scoring with full address weight
        # Name scoring (35% weight)
        name_score = score_name_match(target_name, candidate.get('name', ''))
        total_score += name_score * 0.35
        max_score += 100 * 0.35

        # Address scoring (45% weight) - Compare against ALL candidate addresses
        addr_score = score_address_match(target_addr, candidate_addresses)
        total_score += addr_score * 0.45
        max_score += 100 * 0.45

        # Location context scoring (15% weight)
        location_score = score_location_context(candidate.get('raw_text', ''))
        total_score += location_score * 0.15
        max_score += 100 * 0.15
    else:
        # Empty target address - shift weights to favor other signals
        # Name scoring (50% weight) - Increased
        name_score = score_name_match(target_name, candidate.get('name', ''))
        total_score += name_score * 0.50
        max_score += 100 * 0.50

        # Location context scoring (30% weight) - Increased for geographic relevance
        location_score = score_location_context(candidate.get('raw_text', ''))
        total_score += location_score * 0.30
        max_score += 100 * 0.30

        # Address scoring (10% weight) - Reduced, but still check for Miami/FL context
        addr_score = score_address_match(target_addr, candidate_addresses)
        total_score += addr_score * 0.10
        max_score += 100 * 0.10

    # Data quality scoring (10% weight for empty address case, 5% for normal case)
    quality_weight = 0.10 if not has_target_address else 0.05
    quality_score = score_data_quality(candidate)
    total_score += quality_score * quality_weight
    max_score += 100 * quality_weight

    return (total_score / max_score) * 100 if max_score > 0 else 0

def score_name_match(target_name: Dict[str, Any], candidate_name: str) -> float:
    """Score name similarity with multiple strategies"""
    if not candidate_name:
        return 0

    from src.processors.data_processor import normalize_name
    cand_name = normalize_name(candidate_name)

    # Exact match on both first and last
    if (target_name['first'] and target_name['last'] and
        target_name['first'] == cand_name['first'] and
        target_name['last'] == cand_name['last']):
        return 100

    # Exact match on last name, first name starts with same letters
    if (target_name['last'] and target_name['last'] == cand_name['last'] and
        target_name['first'] and cand_name['first'] and
        cand_name['first'].startswith(target_name['first'][:3])):
        return 95

    # Middle name matching bonus - significantly increased for better matching
    middle_bonus = 0
    if (target_name.get('has_middle') and cand_name.get('has_middle') and
        target_name.get('middle_initial') and cand_name.get('middle_initial') and
        target_name['middle_initial'] == cand_name['middle_initial']):
        middle_bonus = 30  # Increased from 10 to 30 for higher priority

    # Fuzzy matching on full names
    full_similarity = fuzz.token_set_ratio(target_name['full'], cand_name['full'])

    # Use search_name from variants if available, otherwise construct it
    target_search = target_name.get('search_variants', [{}])[0].get('search_name', f"{target_name['first']} {target_name['last']}")
    cand_search = cand_name.get('search_variants', [{}])[0].get('search_name', f"{cand_name['first']} {cand_name['last']}")

    search_similarity = fuzz.token_set_ratio(target_search, cand_search)

    final_score = max(full_similarity, search_similarity) + middle_bonus
    return final_score

def score_address_match(target_addr: Dict[str, Any], candidate_addresses: List[str]) -> float:
    """Enhanced address similarity scoring with multiple addresses and partial matching"""
    if not candidate_addresses:
        return 0

    from src.processors.data_processor import normalize_address
    max_score = 0

    # Compare target against ALL candidate addresses
    for cand_addr_text in candidate_addresses:
        if not cand_addr_text:
            continue

        cand_addr = normalize_address(cand_addr_text)
        score = calculate_address_similarity(target_addr, cand_addr)
        max_score = max(max_score, score)

    return min(max_score, 100)

def calculate_address_similarity(target_addr: Dict[str, Any], cand_addr: Dict[str, Any]) -> float:
    """Calculate similarity score between two normalized addresses"""
    score = 0

    # Street number exact match - highest priority
    if target_addr['street_num'] and target_addr['street_num'] == cand_addr['street_num']:
        score += 50

    # Street name exact match - high priority
    if (target_addr['street_name'] and cand_addr['street_name'] and
        target_addr['street_name'] == cand_addr['street_name']):
        score += 30

    # Partial street name match
    elif (target_addr['street_name'] and cand_addr['street_name']):
        target_street = target_addr['street_name'].lower()
        cand_street = cand_addr['street_name'].lower()
        if (target_street in cand_street or cand_street in target_street):
            score += 20

    # Enhanced token overlap with partial matching
    common_tokens = target_addr['tokens'].intersection(cand_addr['tokens'])
    if target_addr['tokens']:
        token_ratio = len(common_tokens) / len(target_addr['tokens'])
        score += token_ratio * 30

    # Add partial matching for single words/numbers
    score += calculate_partial_matches(target_addr, cand_addr)

    return score

def calculate_partial_matches(target_addr: Dict[str, Any], cand_addr: Dict[str, Any]) -> float:
    """Check for single word/number matches that indicate potential relationship"""
    score = 0

    # Street number partial match (any digits match)
    if target_addr['street_num'] and cand_addr['street_num']:
        target_nums = set(re.findall(r'\d+', target_addr['street_num']))
        cand_nums = set(re.findall(r'\d+', cand_addr['street_num']))
        if target_nums.intersection(cand_nums):
            score += 25

    # Single word matches in address tokens
    target_words = {word.lower() for word in target_addr['tokens'] if len(word) > 2}
    cand_words = {word.lower() for word in cand_addr['tokens'] if len(word) > 2}
    common_words = target_words.intersection(cand_words)

    if common_words:
        # Boost score for meaningful word matches (not just common words like "ST", "AVE")
        meaningful_matches = [word for word in common_words if word not in {'st', 'ave', 'blvd', 'dr', 'rd', 'ct', 'ln', 'pl', 'way', 'n', 's', 'e', 'w'}]
        score += min(len(meaningful_matches) * 10, 25)

    return score

def score_location_context(raw_text: str) -> float:
    """Score based on Miami/Florida context"""
    if not raw_text:
        return 50  # neutral
    
    text_upper = raw_text.upper()
    score = 50  # base score
    
    # Positive indicators
    if 'MIAMI' in text_upper:
        score += 30
    elif 'FL' in text_upper or 'FLORIDA' in text_upper:
        score += 20
    
    # Look for zip codes in Miami area
    miami_zips = ['33101', '33102', '33109', '33125', '33126', '33127', '33128', '33129', '33130',
                  '33131', '33132', '33133', '33134', '33135', '33136', '33137', '33138', '33139',
                  '33140', '33141', '33142', '33143', '33144', '33145', '33146', '33147', '33150',
                  '33151', '33152', '33153', '33154', '33155', '33156', '33157', '33158', '33160',
                  '33161', '33162', '33163', '33164', '33165', '33166', '33167', '33168', '33169',
                  '33170', '33172', '33173', '33174', '33175', '33176', '33177', '33178', '33179',
                  '33180', '33181', '33182', '33183', '33184', '33185', '33186', '33187', '33188',
                  '33189', '33190', '33193', '33194', '33196', '33197', '33199']
    
    for zip_code in miami_zips:
        if zip_code in text_upper:
            score += 25
            break
    
    return min(score, 100)

def score_data_quality(candidate: Dict[str, Any]) -> float:
    """Score based on data completeness and quality"""
    score = 0
    
    # Has name
    if candidate.get('name'):
        score += 40
    
    # Has address
    if candidate.get('address'):
        score += 30
    
    # Phone number format quality
    phone = candidate.get('phone', '')
    if phone:
        if re.match(r'\(\d{3}\) \d{3}-\d{4}', phone):
            score += 30  # Well formatted
        else:
            score += 20  # Has phone but not well formatted
    
    return score

def _build_person_key(candidate: Dict[str, Any]) -> str:
    """Create a grouping key representing a person using normalized name.

    Uses intelligent grouping to recognize similar names as the same person.
    Enhanced to separate groups when middle initials differ for better matching.
    """
    from src.processors.data_processor import normalize_name
    
    name_text = candidate.get('name', '') or ''
    norm = normalize_name(name_text)

    if norm.get('first') or norm.get('last'):
        # Create a more flexible key that groups similar names together
        first = norm.get('first', '').upper().strip()
        last = norm.get('last', '').upper().strip()

        # Remove middle initial from first name if present (e.g., "RAFAEL P" -> "RAFAEL")
        first_parts = first.split()
        if len(first_parts) > 1 and len(first_parts[-1]) == 1:
            first = ' '.join(first_parts[:-1])

        # Create base key without middle initial
        base_key = f"{first} {last}".strip()

        # If no middle name detected, return base key
        if not norm.get('has_middle'):
            return base_key

        # Enhanced: If middle initial exists, create separate groups for different middle initials
        # This prevents "Warren Jackson Spencer" and "Warren Lane Spencer" from being grouped together
        if norm.get('middle_initial') and len(norm['middle_initial']) == 1:
            key = f"{base_key} {norm['middle_initial']}"
            return key

        # If middle name exists but no single-letter initial, still group by base name
        return base_key

    # Fallback to phone if name missing
    phone = candidate.get('phone', '') or ''
    return phone

def _collect_group_phones(cands: List[Dict[str, Any]], max_phones: int = 2, exclude: Set[str] = None) -> List[str]:
    """Collect up to max_phones unique normalized phones from a candidate group."""
    from src.processors.data_processor import normalize_phone
    
    if exclude is None:
        exclude = set()
    phones: List[str] = []
    seen: Set[str] = set(exclude)
    for cand in cands:
        # Prefer all_phones if available, fallback to single phone
        numbers = cand.get('all_phones') or ([cand.get('phone')] if cand.get('phone') else [])
        for num in numbers:
            norm = normalize_phone(num)
            if norm and norm not in seen:
                phones.append(norm)
                seen.add(norm)
                if len(phones) >= max_phones:
                    return phones
    return phones

def select_top_two_groups_phones(
    candidates: List[Dict[str, Any]],
    target_name: Dict[str, Any],
    target_addr: Dict[str, Any]
) -> tuple[str, str, str, str]:
    """From a flat candidate list, choose up to two best person-groups.

    Returns up to four phones: (phone1, phone2, phone3, phone4).
    - phone1/phone2 come from best matching person (up to two numbers)
    - phone3/phone4 come from second best matching person
    """
    if not candidates:
        return "", "", "", ""

    # Score candidates and group by person key
    groups: Dict[str, Dict[str, Any]] = {}
    for cand in candidates:
        score = score_candidate(target_name, target_addr, cand)
        key = _build_person_key(cand)
        if key not in groups:
            groups[key] = {"best_score": score, "members": [cand]}
        else:
            groups[key]["best_score"] = max(groups[key]["best_score"], score)
            groups[key]["members"].append(cand)

    # Enhanced sorting: prioritize groups with middle initial matches when scores are close
    def sort_with_middle_priority(group):
        base_score = group["best_score"]

        # Check if any candidate in this group has middle initial match with target
        has_middle_match = False
        target_middle = target_name.get('middle_initial')
        if target_middle:
            for cand in group["members"]:
                from src.processors.data_processor import normalize_name
                cand_norm = normalize_name(cand.get('name', ''))
                if (cand_norm.get('has_middle') and
                    cand_norm.get('middle_initial') == target_middle):
                    has_middle_match = True
                    break

        # Boost score by 15 points if middle initial matches (helps when scores are close)
        return base_score + (15 if has_middle_match else 0)

    sorted_groups = sorted(groups.values(), key=sort_with_middle_priority, reverse=True)
    if not sorted_groups:
        return "", "", "", ""

    # Collect phones from first best group
    first_group = sorted_groups[0]
    first_phones = _collect_group_phones(first_group["members"], max_phones=2)
    phone1 = first_phones[0] if len(first_phones) > 0 else ""
    phone2 = first_phones[1] if len(first_phones) > 1 else ""

    # Collect phones from second best group
    phone3 = phone4 = ""
    if len(sorted_groups) > 1:
        exclude = set([p for p in [phone1, phone2] if p])
        second_group = sorted_groups[1]
        second_phones = _collect_group_phones(second_group["members"], max_phones=2, exclude=exclude)

        if second_phones:
            phone3 = second_phones[0] if len(second_phones) > 0 else ""
            exclude.update(second_phones[:1])
            remaining = [p for p in second_phones[1:] if p not in exclude]
            phone4 = remaining[0] if remaining else ""
        # No fallback - only use first and second best groups

    return phone1, phone2, phone3, phone4
