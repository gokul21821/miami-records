"""
Configuration settings and constants for the Miami real estate project.
"""

import re
import os
from typing import List, Pattern
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Base URLs
ANYWHO_BASE = "https://www.anywho.com"
ANYWHO_PEOPLE = f"{ANYWHO_BASE}/people"

# Enhanced phone number regex patterns
PHONE_PATTERNS: List[Pattern] = [
    re.compile(r'\((\d{3})\)\s*(\d{3})-(\d{4})'),  # (305) 555-1234
    re.compile(r'(\d{3})-(\d{3})-(\d{4})'),        # 305-555-1234
    re.compile(r'(\d{3})\.(\d{3})\.(\d{4})'),      # 305.555.1234
    re.compile(r'(\d{3})\s+(\d{3})\s+(\d{4})'),    # 305 555 1234
    re.compile(r'(\d{10})'),                       # 3055551234
]

# Default paths
DEFAULT_CACHE_PATH = "data/cache/anywho_cache_enhanced.json"

# Request settings
DEFAULT_SLEEP_SEC = 1.0
DEFAULT_REQUEST_TIMEOUT = 30

# Default user agent (updated for 2024)
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"
)

# Enhanced default request headers with modern browser features
DEFAULT_HEADERS = {
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
}

# Miami zip codes for location context scoring
MIAMI_ZIP_CODES = [
    '33101', '33102', '33109', '33125', '33126', '33127', '33128', '33129', '33130',
    '33131', '33132', '33133', '33134', '33135', '33136', '33137', '33138', '33139',
    '33140', '33141', '33142', '33143', '33144', '33145', '33146', '33147', '33150',
    '33151', '33152', '33153', '33154', '33155', '33156', '33157', '33158', '33160',
    '33161', '33162', '33163', '33164', '33165', '33166', '33167', '33168', '33169',
    '33170', '33172', '33173', '33174', '33175', '33176', '33177', '33178', '33179',
    '33180', '33181', '33182', '33183', '33184', '33185', '33186', '33187', '33188',
    '33189', '33190', '33193', '33194', '33196', '33197', '33199'
]

# Common first names for name format detection
COMMON_FIRST_NAMES = {
    'JOHN', 'JAMES', 'MICHAEL', 'MARY', 'PATRICIA', 'LINDA', 'BARBARA',
    'ELIZABETH', 'JENNIFER', 'MARIA', 'SUSAN', 'MARGARET', 'DOROTHY',
    'LISA', 'NANCY', 'KAREN', 'BETTY', 'HELEN', 'SANDRA', 'DONNA',
    'ROBERT', 'WILLIAM', 'DAVID', 'RICHARD', 'CHARLES', 'JOSEPH',
    'THOMAS', 'CHRISTOPHER', 'DANIEL', 'MATTHEW', 'ANTHONY', 'MARK',
    'DONALD', 'STEVEN', 'PAUL', 'ANDREW', 'JOSHUA', 'KENNETH', 'KEVIN',
    'BRIAN', 'GEORGE', 'TIMOTHY', 'RONALD', 'JASON', 'EDWARD', 'JACOB'
}

# Address parsing settings
COMMON_STREET_WORDS = {'N', 'S', 'E', 'W', 'ST', 'AVE', 'BLVD', 'DR', 'RD', 'CT', 'LN', 'PL', 'WAY'}
