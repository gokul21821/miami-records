"""
Data processing module for name, address, and phone normalization.
"""

import re
from typing import Dict, Any, List, Optional, Set

def normalize_phone(phone_str: str) -> Optional[str]:
    """Extract and normalize phone number to (XXX) XXX-XXXX format"""
    if not phone_str:
        return None
    
    # Remove all non-digit characters for initial processing
    digits = re.sub(r'\D', '', phone_str)
    
    # Must be exactly 10 digits for US phone numbers
    if len(digits) != 10:
        return None
    
    # Format as (XXX) XXX-XXXX
    return f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"

def extract_phones_from_text(text: str, phone_patterns: List[re.Pattern]) -> List[str]:
    """Extract all valid phone numbers from text using multiple patterns"""
    phones = set()
    
    for pattern in phone_patterns:
        matches = pattern.findall(text)
        for match in matches:
            if isinstance(match, tuple):
                # Join the groups
                phone_digits = ''.join(match)
            else:
                phone_digits = match
            
            normalized = normalize_phone(phone_digits)
            if normalized:
                phones.add(normalized)
    
    return list(phones)

def normalize_name(name: str) -> Dict[str, Any]:
    """Enhanced name normalization with middle name detection"""
    if not name:
        return {
            "first": "", "middle": "", "last": "",
            "full": "", "has_middle": False,
            "middle_initial": "", "search_variants": []
        }

    cleaned = re.sub(r'[^\w\s]', ' ', name.upper().strip())
    parts = [p for p in cleaned.split() if len(p) > 1]

    result = {
        "first": "", "middle": "", "last": "",
        "full": name.strip(), "has_middle": False,
        "middle_initial": "", "search_variants": []
    }

    if len(parts) == 1:
        # Single name
        result["first"] = parts[0]
    elif len(parts) == 2:
        # Standard LAST FIRST format
        result["last"], result["first"] = parts[0], parts[1]
    elif len(parts) == 3:
        # Handle LAST FIRST MIDDLE or FIRST MIDDLE LAST
        result = detect_name_format(parts)
    elif len(parts) > 3:
        # Handle compound names like ESTRADA CASTRO MARTHA
        result = handle_compound_names(parts)

    # Generate search variants (only basic and middle initial, skip full middle)
    result["search_variants"] = generate_name_variants(result)

    return result

def detect_name_format(parts: List[str]) -> Dict[str, Any]:
    """Detect if name is LAST FIRST MIDDLE or FIRST MIDDLE LAST"""
    result = {
        "first": "", "middle": "", "last": "",
        "full": "", "has_middle": False, "middle_initial": ""
    }

    # Enhanced heuristics for detection:
    # 1. If second part is 1 character, likely middle initial in LAST FIRST MIDDLE format (e.g., "SPENCER WARREN J")
    # 2. If first part is common first name, likely FIRST MIDDLE LAST format
    # 3. Default to LAST FIRST MIDDLE if ambiguous

    if len(parts[1]) == 1:
        # Likely LAST FIRST MIDDLE format (e.g., "SPENCER WARREN J")
        result["last"] = parts[0]
        result["first"] = parts[1]  # This would be "WARREN" in the example
        result["middle"] = parts[2]  # This would be "J" in the example
        result["has_middle"] = True
        result["middle_initial"] = parts[2]  # Middle initial is the third part
    else:
        # Check if first part looks like a first name
        common_first_names = {'JOHN', 'JAMES', 'MICHAEL', 'MARY', 'PATRICIA', 'LINDA', 'BARBARA',
                             'ELIZABETH', 'JENNIFER', 'MARIA', 'SUSAN', 'MARGARET', 'DOROTHY',
                             'LISA', 'NANCY', 'KAREN', 'BETTY', 'HELEN', 'SANDRA', 'DONNA',
                             'ROBERT', 'WILLIAM', 'DAVID', 'RICHARD', 'CHARLES', 'JOSEPH',
                             'THOMAS', 'CHRISTOPHER', 'DANIEL', 'MATTHEW', 'ANTHONY', 'MARK',
                             'DONALD', 'STEVEN', 'PAUL', 'ANDREW', 'JOSHUA', 'KENNETH', 'KEVIN',
                             'BRIAN', 'GEORGE', 'TIMOTHY', 'RONALD', 'JASON', 'EDWARD', 'JACOB'}

        if parts[0] in common_first_names:
            # Likely FIRST MIDDLE LAST format
            result["first"] = parts[0]
            result["middle"] = parts[1]
            result["last"] = parts[2]
            result["has_middle"] = True
            # Set middle initial if middle name is single letter
            if len(parts[1]) == 1:
                result["middle_initial"] = parts[1]
        else:
            # Default to LAST FIRST MIDDLE format
            result["last"] = parts[0]
            result["first"] = parts[1]
            result["middle"] = parts[2]
            result["has_middle"] = True
            # Set middle initial if middle name is single letter
            if len(parts[2]) == 1:
                result["middle_initial"] = parts[2]

    return result

def handle_compound_names(parts: List[str]) -> Dict[str, Any]:
    """Handle names like ESTRADA CASTRO MARTHA"""
    result = {
        "first": "", "middle": "", "last": "",
        "full": "", "has_middle": False, "middle_initial": ""
    }

    # For compound names, treat first N-1 parts as compound last name
    # and last part as first name
    if len(parts) >= 3:
        result["last"] = ' '.join(parts[:-1])  # All but last part as last name
        result["first"] = parts[-1]  # Last part as first name

    return result

def generate_name_variants(name_dict: Dict[str, Any]) -> List[Dict[str, str]]:
    """Generate name search variants (basic and middle initial only)"""
    variants = []

    base = {
        "first": name_dict["first"],
        "last": name_dict["last"]
    }

    # Variant 1: Basic first last
    variants.append({
        "search_name": f"{base['first']} {base['last']}",
        "middle_name": "",
        "variant_type": "basic"
    })

    # Variant 2: With middle initial (only if middle name exists)
    if name_dict["has_middle"] and name_dict["middle_initial"]:
        variants.append({
            "search_name": f"{base['first']} {base['last']}",
            "middle_name": name_dict["middle_initial"].lower(),
            "variant_type": "middle_initial"
        })
    elif name_dict["has_middle"] and name_dict["middle"] and len(name_dict["middle"]) == 1:
        # If middle is a single character, use it as initial
        variants.append({
            "search_name": f"{base['first']} {base['last']}",
            "middle_name": name_dict["middle"].lower(),
            "variant_type": "middle_initial"
        })

    return variants

def normalize_address(addr: str) -> Dict[str, Any]:
    """Parse and normalize address components"""
    if not addr:
        return {"raw": "", "tokens": set(), "street_num": "", "street_name": ""}
    
    cleaned = re.sub(r'[^\w\s#]', ' ', addr.upper().strip())
    tokens = set(p for p in cleaned.split() if len(p) > 1)
    
    # Extract street number (first sequence of digits)
    street_num_match = re.search(r'\b(\d+)\b', cleaned)
    street_num = street_num_match.group(1) if street_num_match else ""
    
    # Extract street name (remove common directionals and types)
    street_tokens = tokens - {'N', 'S', 'E', 'W', 'ST', 'AVE', 'BLVD', 'DR', 'RD', 'CT', 'LN', 'PL', 'WAY'}
    if street_num:
        street_tokens.discard(street_num)
    
    return {
        "raw": addr.strip(),
        "tokens": tokens,
        "street_num": street_num,
        "street_name": ' '.join(sorted(street_tokens)) if street_tokens else ""
    }

def is_likely_name(text: str) -> bool:
    """Determine if text looks like a person's name"""
    if not text or len(text) > 50:
        return False
    
    words = text.split()
    if len(words) < 2 or len(words) > 4:
        return False
    
    # Check if it contains mostly alphabetic characters
    alpha_ratio = sum(c.isalpha() or c.isspace() for c in text) / len(text)
    if alpha_ratio < 0.8:
        return False
    
    # Avoid common non-name patterns
    avoid_patterns = [
        r'\d+',  # Contains numbers
        r'(phone|call|contact|email|@)',  # Contact-related words
        r'(street|ave|blvd|rd|dr|ct|ln)',  # Address words
    ]
    
    text_lower = text.lower()
    for pattern in avoid_patterns:
        if re.search(pattern, text_lower):
            return False
    
    return True

def is_likely_address(text: str) -> bool:
    """Determine if text looks like an address"""
    if not text or len(text) > 100:
        return False
    
    text_upper = text.upper()
    
    # Must contain at least one digit (street number)
    if not re.search(r'\d', text):
        return False
    
    # Should contain common address components
    address_indicators = [
        r'\b\d{1,5}\s+[A-Z]',  # Street number followed by letter
        r'\b(ST|AVE|AVENUE|BLVD|BOULEVARD|DR|DRIVE|RD|ROAD|CT|COURT|LN|LANE|PL|PLACE|WAY)\b',
        r'\b(STREET|DRIVE|ROAD|COURT|LANE|PLACE)\b',
        r'\bFL\b|\bFLORIDA\b|\bMIAMI\b',  # Location indicators
    ]
    
    for pattern in address_indicators:
        if re.search(pattern, text_upper):
            return True
    
    return False
