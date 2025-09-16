"""
Document type constants and utilities for Miami-Dade Official Records.

This module centralizes all document type related constants and utilities
used across the application for consistent handling of MOR and LIE records.
"""

from typing import Dict, List, Optional


# Document Type Constants
DOCUMENT_TYPES = {
    'MOR': {
        'label': 'MORTGAGE - MOR',
        'folder': 'MORTGAGE_MOR',
        'description': 'Mortgage Records',
        'party_type': 'borrower',
        'counter_party_type': 'lender'
    },
    'LIE': {
        'label': 'LIEN - LIE',
        'folder': 'LIEN_LIE',
        'description': 'Lien Records',
        'party_type': 'debtor',
        'counter_party_type': 'creditor'
    }
}

# GUI Display Options
GUI_DOC_TYPE_OPTIONS = [
    "MORTGAGE - MOR",
    "LIEN - LIE"
]

# Default document type
DEFAULT_DOC_TYPE = 'MOR'


def get_doc_type_info(doc_type: str) -> Dict:
    """
    Get complete information for a document type.

    Args:
        doc_type: 'MOR' or 'LIE'

    Returns:
        Dict with label, folder, description, etc.
    """
    return DOCUMENT_TYPES.get(doc_type.upper(), DOCUMENT_TYPES[DEFAULT_DOC_TYPE])


def get_folder_name(doc_type: str) -> str:
    """
    Get folder name for document type.

    Args:
        doc_type: Document type code ('MOR', 'LIE') or label ('MORTGAGE - MOR', 'LIEN - LIE')

    Returns:
        Folder name string
    """
    # Handle both code and label inputs
    doc_type_upper = doc_type.upper()

    # Check if it's a label
    for code, info in DOCUMENT_TYPES.items():
        if info['label'] == doc_type_upper:
            return info['folder']

    # Check if it's a code
    if doc_type_upper in DOCUMENT_TYPES:
        return DOCUMENT_TYPES[doc_type_upper]['folder']

    # Default fallback
    return DOCUMENT_TYPES[DEFAULT_DOC_TYPE]['folder']


def get_label_from_code(doc_type_code: str) -> str:
    """
    Get label from document type code.

    Args:
        doc_type_code: 'MOR' or 'LIE'

    Returns:
        Label string like 'MORTGAGE - MOR'
    """
    return get_doc_type_info(doc_type_code)['label']


def get_code_from_label(doc_type_label: str) -> str:
    """
    Get code from document type label.

    Args:
        doc_type_label: Label like 'MORTGAGE - MOR' or 'LIEN - LIE'

    Returns:
        Code string like 'MOR' or 'LIE'
    """
    label_upper = doc_type_label.upper()
    for code, info in DOCUMENT_TYPES.items():
        if info['label'] == label_upper:
            return code
    return DEFAULT_DOC_TYPE


def get_party_types(doc_type: str) -> tuple[str, str]:
    """
    Get party type names for document type.

    Args:
        doc_type: Document type code or label

    Returns:
        Tuple of (primary_party_type, counter_party_type)
    """
    info = get_doc_type_info(doc_type)
    return info['party_type'], info['counter_party_type']


def validate_doc_type(doc_type: str) -> bool:
    """
    Validate if document type is supported.

    Args:
        doc_type: Document type to validate

    Returns:
        True if valid, False otherwise
    """
    doc_type_upper = doc_type.upper()
    return (doc_type_upper in DOCUMENT_TYPES or
            any(info['label'] == doc_type_upper for info in DOCUMENT_TYPES.values()))


def get_all_doc_types() -> List[str]:
    """
    Get list of all supported document type codes.

    Returns:
        List of document type codes
    """
    return list(DOCUMENT_TYPES.keys())


def get_all_labels() -> List[str]:
    """
    Get list of all supported document type labels.

    Returns:
        List of document type labels
    """
    return [info['label'] for info in DOCUMENT_TYPES.values()]
