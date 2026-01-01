"""
# Copyright (C) 2025 Qleric
# Licensed under AGPL-3.0 - see LICENSE file
"""

import atexit
import gc
import io
import json
import logging
import os
import re
import time
import tracemalloc
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from functools import wraps
from typing import Any, Dict, List, Optional, Tuple, Union
from dotenv import load_dotenv

import fitz  # PyMuPDF
import msgspec
import msgspec.yaml
import spacy
from anthropic import Anthropic, APIError, RateLimitError
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

load_dotenv()

# -------------------------------------------------------------------------
# Default Prompts (Fallbacks)
# -------------------------------------------------------------------------

DEFAULT_EXTRACTION_PROMPT = """You are an expert contract analyst. Extract the requested fields into JSON format.

EXTRACTION TIER: {tier_label}

FIELDS TO EXTRACT:
{fields_list}

Instructions:
1. Extract the 'value' and 'verbatim_source' (exact substring) for each field.
2. If not found, return null.
3. Return ONLY JSON.
"""

DEFAULT_RECHECK_PROMPT = """You are an expert contract analyst doing a SECOND PASS review.
The initial extraction MISSED these fields. Look for synonyms or hidden clauses.

Return JSON format with 'value', 'verbatim_source', and 'page_number'.
"""


# -------------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------------

def get_anthropic_api_key() -> str:
    """Get the Anthropic API key."""
    key = os.getenv('ANTHROPIC_API_KEY', '').strip()
    if not key:
        logger.warning("No ANTHROPIC_API_KEY found in .env")
        return ""
    return key


# ═══════════════════════════════════════════════════════════════════════════════
# MASTER FIELD BANK - 60+ fields across comprehensive categories
# ═══════════════════════════════════════════════════════════════════════════════

MASTER_FIELD_BANK = {
    # ═══════════════════════════════════════════════════════════════════════════
    # CORE DATES & PARTIES (3 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "dates_parties": {
        "effective_date",
        "expiration_date",
        "parties",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # FINANCIAL TERMS (3 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "financial_basic": {
        "total_contract_value",
        "payment_terms",
        "currency",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # TERMINATION & RENEWAL (3 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "termination_basic": {
        "termination_notice_period",
        "renewal_terms",
        "governing_law",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # LIABILITY & RISK - BASIC (4 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "liability_basic": {
        "liability_cap",
        "indemnification_clauses",
        "insurance_requirements",
        "limitation_of_liability",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PERFORMANCE & OBLIGATIONS - BASIC (4 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "performance_basic": {
        "deliverables",
        "sla_terms",
        "performance_metrics",
        "acceptance_criteria",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # INTELLECTUAL PROPERTY - BASIC (3 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "ip_basic": {
        "ip_ownership",
        "license_scope",
        "usage_restrictions",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # COMPLIANCE & DISPUTE - BASIC (5 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "compliance_basic": {
        "confidentiality_period",
        "non_compete_terms",
        "arbitration_clause",
        "audit_rights",
        "data_protection",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # ADMINISTRATIVE - BASIC (4 fields)
    # ═══════════════════════════════════════════════════════════════════════════
    "administrative_basic": {
        "notice_address",
        "amendment_process",
        "assignment_rights",
        "force_majeure",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PAYMENT WORKFLOW - EXTENDED (5 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "payment_workflow": {
        "late_fees",
        "payment_milestones",
        "invoice_frequency",
        "dispute_procedures",
        "escrow_terms",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # COMPLIANCE EXTENDED (5 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "compliance_extended": {
        "gdpr_obligations",
        "ccpa_compliance",
        "security_standards",
        "audit_frequency",
        "certification_requirements",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # PERFORMANCE EXTENDED (5 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "performance_extended": {
        "penalties",
        "cure_periods",
        "escalation_procedures",
        "change_order_process",
        "warranty_terms",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # RISK MANAGEMENT (4 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "risk_management": {
        "risk_allocation",
        "contingency_provisions",
        "material_breach_definition",
        "remedies",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # TERMINATION EXTENDED (4 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "termination_extended": {
        "termination_for_cause",
        "termination_for_convenience",
        "transition_assistance",
        "survival_clauses",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # IP EXTENDED (5 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "ip_extended": {
        "background_ip",
        "foreground_ip",
        "joint_ip",
        "moral_rights_waiver",
        "source_code_escrow",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # COMMERCIAL TERMS (5 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "commercial_terms": {
        "exclusivity",
        "territory_restrictions",
        "volume_commitments",
        "price_adjustments",
        "benchmarking_rights",
    },
    
    # ═══════════════════════════════════════════════════════════════════════════
    # RELATIONSHIP TERMS (4 fields) [NEW]
    # ═══════════════════════════════════════════════════════════════════════════
    "relationship_terms": {
        "subcontracting_rights",
        "key_personnel",
        "governance_structure",
        "reporting_requirements",
    },
}

# Flatten all fields for validation
ALL_FIELDS = set()
for category_fields in MASTER_FIELD_BANK.values():
    ALL_FIELDS.update(category_fields)

# ═══════════════════════════════════════════════════════════════════════════════
# TIERED FIELD DEFINITIONS - Built from Master Bank
# Note: 59-field MASTER_FIELD_BANK is a MENU for mix-match selection
# Users extract MAX 25 fields per extraction (Enterprise tier limit)
# ═══════════════════════════════════════════════════════════════════════════════

# Maximum fields allowed per extraction
MAX_CUSTOM_FIELDS = 25

TIER_FIELDS = {
    # Essential: 9 core fields (1 credit)
    # dates_parties(3) + financial_basic(3) + termination_basic(3) = 9
    "essential": (
        MASTER_FIELD_BANK["dates_parties"] |
        MASTER_FIELD_BANK["financial_basic"] |
        MASTER_FIELD_BANK["termination_basic"]
    ),  # 9 fields

    # Professional: 18 fields (3 credits)
    # Essential(9) + liability_basic(4) + performance_basic(4) + 1 additional = 18
    # We add ip_ownership as the +1 to reach 18
    "professional": (
        MASTER_FIELD_BANK["dates_parties"] |
        MASTER_FIELD_BANK["financial_basic"] |
        MASTER_FIELD_BANK["termination_basic"] |
        MASTER_FIELD_BANK["liability_basic"] |
        MASTER_FIELD_BANK["performance_basic"] |
        {"ip_ownership"}  # +1 to reach 18
    ),  # 9 + 4 + 4 + 1 = 18 fields

    # Enterprise: 25 fields (5 credits) - Full predefined extraction
    # Professional(18) + remaining ip_basic(2) + compliance_basic(5) = 25
    "enterprise": (
        MASTER_FIELD_BANK["dates_parties"] |
        MASTER_FIELD_BANK["financial_basic"] |
        MASTER_FIELD_BANK["termination_basic"] |
        MASTER_FIELD_BANK["liability_basic"] |
        MASTER_FIELD_BANK["performance_basic"] |
        MASTER_FIELD_BANK["ip_basic"] |
        MASTER_FIELD_BANK["compliance_basic"]
    ),  # 9 + 4 + 4 + 3 + 5 = 25 fields
    
    # Full: 59 fields (for mix-match MENU reference only - NOT for direct extraction)
    "full": ALL_FIELDS,
}

# Credit cost per tier
TIER_CREDITS = {
    "essential": 1,
    "professional": 3,
    "enterprise": 5,
}

# Contract type categories for classification
CONTRACT_TYPES = [
    "Service Agreement",
    "Employment Contract",
    "Non-Disclosure Agreement",
    "License Agreement",
    "Sales Agreement",
    "Lease Agreement",
    "Partnership Agreement",
    "Consulting Agreement",
    "Supply Agreement",
    "Vendor Agreement",
    "Master Services Agreement",
    "Statement of Work",
    "General Agreement",  # Fallback
]

# Credit calculation for custom field selection (mix-match)
def calculate_custom_credits(field_count: int) -> int:
    """
    Calculate credits based on number of fields selected.
    Enforces 25-field maximum limit.
    """
    if field_count > MAX_CUSTOM_FIELDS:
        raise ValueError(f"Maximum {MAX_CUSTOM_FIELDS} fields allowed per extraction")
    if field_count <= 9:
        return 1
    elif field_count <= 18:
        return 3
    else:
        return 5

# Field descriptions for LLM prompting
FIELD_DESCRIPTIONS = {
    # ═══ CORE DATES & PARTIES ═══
    "effective_date": "Contract START date / Effective date",
    "expiration_date": "Contract END date / Expiration date",
    "parties": "Names of all parties/entities in the contract",
    
    # ═══ FINANCIAL TERMS ═══
    "total_contract_value": "Total monetary value of the contract (calculate if needed)",
    "payment_terms": "Payment schedule and terms (e.g., Net 30, monthly)",
    "currency": "Currency used (USD, EUR, GBP, etc.)",
    
    # ═══ TERMINATION & RENEWAL ═══
    "termination_notice_period": "Notice period required to terminate",
    "renewal_terms": "Auto-renewal conditions, renewal options",
    "governing_law": "Jurisdiction / Governing law / Choice of law",
    
    # ═══ LIABILITY & RISK ═══
    "liability_cap": "Maximum liability amount or percentage cap",
    "indemnification_clauses": "Who indemnifies whom and for what",
    "insurance_requirements": "Required insurance types and minimum amounts",
    "limitation_of_liability": "Exclusions, carve-outs from liability limits",
    
    # ═══ PERFORMANCE & OBLIGATIONS ═══
    "deliverables": "Key deliverables, milestones, or work products",
    "sla_terms": "Service Level Agreement terms and commitments",
    "performance_metrics": "KPIs, penalties for non-performance",
    "acceptance_criteria": "How deliverables/work is accepted",
    
    # ═══ INTELLECTUAL PROPERTY ═══
    "ip_ownership": "Who owns intellectual property created",
    "license_scope": "License type (exclusive, non-exclusive, perpetual)",
    "usage_restrictions": "Geographic, industry, or use-case limitations",
    
    # ═══ COMPLIANCE & DISPUTE ═══
    "confidentiality_period": "Duration of confidentiality/NDA obligations",
    "non_compete_terms": "Non-compete restrictions and duration",
    "arbitration_clause": "Dispute resolution method (arbitration, mediation, litigation)",
    "audit_rights": "Right to audit records, financials, or compliance",
    "data_protection": "GDPR, CCPA, or other data privacy obligations",
    
    # ═══ ADMINISTRATIVE ═══
    "notice_address": "Address for official notices/communications",
    "amendment_process": "How the contract can be modified",
    "assignment_rights": "Whether contract can be assigned/transferred",
    "force_majeure": "Force majeure clause presence and terms",
    
    # ═══ PAYMENT WORKFLOW (NEW) ═══
    "late_fees": "Late payment penalties, interest rates, or fee structures",
    "payment_milestones": "Milestone-based payment schedule and triggers",
    "invoice_frequency": "How often invoices are submitted (monthly, quarterly, etc.)",
    "dispute_procedures": "Process for disputing invoices or payments",
    "escrow_terms": "Escrow arrangements, holdbacks, or retainage terms",
    
    # ═══ COMPLIANCE EXTENDED (NEW) ═══
    "gdpr_obligations": "Specific GDPR compliance requirements and data handling",
    "ccpa_compliance": "California Consumer Privacy Act requirements",
    "security_standards": "Required security certifications (SOC2, ISO27001, etc.)",
    "audit_frequency": "How often audits can be conducted",
    "certification_requirements": "Required certifications or qualifications",
    
    # ═══ PERFORMANCE EXTENDED (NEW) ═══
    "penalties": "Financial penalties for non-performance or SLA breaches",
    "cure_periods": "Time allowed to remedy breaches before termination",
    "escalation_procedures": "How disputes or issues are escalated",
    "change_order_process": "Procedure for scope changes and modifications",
    "warranty_terms": "Warranty period, coverage, and limitations",
    
    # ═══ RISK MANAGEMENT (NEW) ═══
    "risk_allocation": "How risks are divided between parties",
    "contingency_provisions": "Backup plans or contingency clauses",
    "material_breach_definition": "What constitutes a material breach",
    "remedies": "Available remedies for breach (damages, specific performance)",
    
    # ═══ TERMINATION EXTENDED (NEW) ═══
    "termination_for_cause": "Grounds for termination due to breach or default",
    "termination_for_convenience": "Right to terminate without cause",
    "transition_assistance": "Obligations to help transition to new provider",
    "survival_clauses": "Provisions that survive contract termination",
    
    # ═══ IP EXTENDED (NEW) ═══
    "background_ip": "Pre-existing intellectual property each party brings",
    "foreground_ip": "New IP created during the contract",
    "joint_ip": "Jointly developed intellectual property ownership",
    "moral_rights_waiver": "Waiver of moral rights to creative works",
    "source_code_escrow": "Source code escrow arrangements for software",
    
    # ═══ COMMERCIAL TERMS (NEW) ═══
    "exclusivity": "Exclusive dealing or exclusivity provisions",
    "territory_restrictions": "Geographic limitations on rights or operations",
    "volume_commitments": "Minimum purchase or volume requirements",
    "price_adjustments": "Price escalation clauses or adjustment mechanisms",
    "benchmarking_rights": "Right to benchmark pricing against market",
    
    # ═══ RELATIONSHIP TERMS (NEW) ═══
    "subcontracting_rights": "Whether and how subcontracting is permitted",
    "key_personnel": "Named individuals critical to performance",
    "governance_structure": "Joint steering committees or governance bodies",
    "reporting_requirements": "Required reports, frequency, and format",
}

# ═══════════════════════════════════════════════════════════════════════════════
# FIELD TAXONOMY - Which fields can be citation-verified vs. which are derived
# ═══════════════════════════════════════════════════════════════════════════════
# 
# EXTRACTIVE: Value should be quoted directly from the document
# DERIVED: Value is calculated, inferred, or consolidated from multiple sources
#
# This prevents penalizing correct synthesis fields that can't have single citations

DERIVED_FIELDS = {
    # These fields often require calculation or consolidation
    "total_contract_value",      # May be sum of multiple payments
    "payment_terms",             # Often summarized from multiple clauses
    "parties",                   # Consolidated from multiple mentions
    "renewal_terms",             # May be inferred from multiple sections
    "deliverables",              # Often listed across multiple sections
    "sla_terms",                 # Typically summarized from schedules/exhibits
    "payment_milestones",        # Derived from multiple schedule entries
    "remedies",                  # May combine multiple remedy clauses
}

# All other fields are EXTRACTIVE (should have direct citation)
def is_derived_field(field_name: str) -> bool:
    """Check if a field is derived (synthesis) vs. extractive (direct quote)."""
    return field_name in DERIVED_FIELDS


# ═══════════════════════════════════════════════════════════════════════════════
# TEXT NORMALIZATION FOR FUZZY MATCHING
# ═══════════════════════════════════════════════════════════════════════════════
# 
# Handles: line breaks, hyphenation, OCR artifacts, ligatures, extra whitespace

def normalize_text_for_matching(text: str) -> str:
    """
    Normalize text to handle PDF extraction quirks:
    - Line breaks within sentences
    - Hyphenation at line ends
    - Multiple spaces
    - Common ligatures (fi, fl, ff, ffi, ffl)
    - OCR common errors
    """
    if not text:
        return ""
    
    # Convert ligatures to regular characters
    ligature_map = {
        '\ufb01': 'fi',  # fi ligature
        '\ufb02': 'fl',  # fl ligature
        '\ufb00': 'ff',  # ff ligature
        '\ufb03': 'ffi', # ffi ligature
        '\ufb04': 'ffl', # ffl ligature
    }
    for lig, replacement in ligature_map.items():
        text = text.replace(lig, replacement)
    
    # Remove hyphenation at line breaks (word-\n -> word)
    text = re.sub(r'-\s*\n\s*', '', text)
    
    # Normalize all whitespace (newlines, tabs, multiple spaces) to single space
    text = re.sub(r'\s+', ' ', text)
    
    # Strip leading/trailing whitespace
    text = text.strip()
    
    # Lowercase for comparison (but preserve original for display)
    return text.lower()


def fuzzy_text_exists(needle: str, haystack: str, threshold: float = 0.85) -> bool:
    """
    Check if needle exists in haystack with fuzzy matching.
    Handles OCR errors and PDF text extraction quirks.
    
    Returns True if:
    1. Exact normalized match exists, OR
    2. Similarity ratio >= threshold
    """
    if not needle or not haystack:
        return False
    
    # Normalize both
    norm_needle = normalize_text_for_matching(needle)
    norm_haystack = normalize_text_for_matching(haystack)
    
    # First try: exact substring match on normalized text
    if norm_needle in norm_haystack:
        return True
    
    # Second try: check if words are present (order-independent)
    needle_words = set(norm_needle.split())
    if len(needle_words) >= 3:  # Only for multi-word searches
        haystack_words = set(norm_haystack.split())
        word_overlap = len(needle_words & haystack_words) / len(needle_words)
        if word_overlap >= threshold:
            return True
    
    return False


# For backward compatibility
SUPPORTED_KEYS = TIER_FIELDS["essential"]

INPUT_TOKEN_COST = 0.000003
OUTPUT_TOKEN_COST = 0.000015

# 🔒 FIX: Cost alert threshold
HIGH_COST_ALERT_THRESHOLD = 1.0  # Alert if single request costs over $1

# -------------------------------------------------------------------------
# Logging
# -------------------------------------------------------------------------

logging.getLogger().handlers.clear()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# -------------------------------------------------------------------------
# Decorators
# -------------------------------------------------------------------------

def performance_profiler(func):
    """Decorator to measure execution time and peak memory usage."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        tracemalloc.start()
        start_time = time.perf_counter()
        try:
            result = func(*args, **kwargs)
        finally:
            end_time = time.perf_counter()
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()

            if isinstance(result, dict):
                result.setdefault("performance_metrics", {})
                result["performance_metrics"].update({
                    "execution_time_seconds": f"{end_time - start_time:.4f}",
                    "peak_memory_usage_mb": f"{peak / 10**6:.2f}",
                })

        return result

    return wrapper


# -------------------------------------------------------------------------
# Data Models
# -------------------------------------------------------------------------

class ExtractionSource(Enum):
    REGEX = "Regex"
    SYSTEM_FALLBACK = "System Fallback"
    INFERENCE = "Inference (Claude)"
    NONE = "None"


@dataclass
class ExtractionResult:
    value: str
    source: ExtractionSource
    page_number: Optional[int] = None
    reference_snippet: Optional[str] = None
    bbox: Optional[List[List[float]]] = None
    grounded: bool = False  # True if text evidence found in PDF (not "verified correct")
    field_type: str = "extractive"  # "extractive" or "derived"

    def to_dict(self) -> Dict[str, Any]:
        return {
            "value": self.value,
            "source": self.source.value,
            "page_number": self.page_number,
            "reference_snippet": self.reference_snippet,
            "bbox": self.bbox,
            "grounded": self.grounded,
            "field_type": self.field_type,
        }


@dataclass
class PatternConfig:
    patterns: List[str]
    formatter: Optional[str] = None
    fallback_text: Optional[str] = "Not Found"
    find_all: bool = False


@dataclass
class PageText:
    page_number: int
    text: str


# -------------------------------------------------------------------------
# Main Extractor
# -------------------------------------------------------------------------

class ContractExtractor:
    """Contract information extractor using Anthropic Claude and PyMuPDF for coordinates."""

    def __init__(self):
        self.patterns: Dict[str, PatternConfig] = {}
        atexit.register(self.cleanup)

        self.nlp = spacy.blank("en")
        self.nlp.add_pipe("sentencizer")

        # Initialize Anthropic Client
        api_key = get_anthropic_api_key()
        self.client = Anthropic(api_key=api_key) if api_key else None

        try:
            self.patterns = self._build_comprehensive_patterns()
        except Exception as e:
            logger.error(f"Failed to initialize ContractExtractor: {e}")
            raise

    def cleanup(self):
        logger.info("Performing cleanup...")
        gc.collect()

    def _build_comprehensive_patterns(self) -> Dict[str, PatternConfig]:
        patterns = {}
        patterns_dir = os.path.join(os.path.dirname(__file__), "patterns")

        if not os.path.isdir(patterns_dir):
            return {}

        for filename in os.listdir(patterns_dir):
            if not filename.endswith(".yaml"):
                continue

            key = filename.replace(".yaml", "")
            # Support all fields, not just essential
            if key not in TIER_FIELDS["enterprise"]:
                continue

            try:
                with open(os.path.join(patterns_dir, filename), "rb") as f:
                    config_data = msgspec.yaml.decode(f.read(), type=PatternConfig)
                    patterns[key] = config_data
            except Exception as e:
                logger.error("Failed to load pattern file %s: %s", filename, e)

        return patterns

    def _smart_truncate(self, text: str, max_chars: int = 100000) -> Tuple[str, bool]:
        if len(text) <= max_chars:
            logger.info(f"Contract text length: {len(text)} chars (no truncation needed)")
            return text, False

        first_part_size = int(max_chars * 0.6)
        last_part_size = int(max_chars * 0.4)

        truncated = (
            text[:first_part_size]
            + "\n\n[... middle section omitted due to length ...]\n\n"
            + text[-last_part_size:]
        )

        logger.warning(
            f"Contract text truncated from {len(text)} to {len(truncated)} chars "
            f"(first {first_part_size}, last {last_part_size})"
        )

        return truncated, True

    # ═══════════════════════════════════════════════════════════════════════════════
    # SMART CHUNKING FOR LONG CONTRACTS (>50 pages)
    # ═══════════════════════════════════════════════════════════════════════════════
    
    # Section markers for intelligent boundary detection
    SECTION_MARKERS = [
        r'^ARTICLE\s+[IVX\d]+\.?',           # ARTICLE I, ARTICLE 1
        r'^SECTION\s+\d+\.?',                 # SECTION 1
        r'^\d+\.\s+[A-Z][A-Z\s]{3,}$',        # 1. DEFINITIONS
        r'^EXHIBIT\s+[A-Z\d]+',               # EXHIBIT A
        r'^SCHEDULE\s+[A-Z\d]+',              # SCHEDULE 1
        r'^APPENDIX\s+[A-Z\d]+',              # APPENDIX A
        r'^ANNEX\s+[A-Z\d]+',                 # ANNEX A
        r'^PART\s+[IVX\d]+',                  # PART I
    ]
    
    def _detect_section_boundaries(self, pages: List[PageText]) -> List[Dict]:
        """
        Detect section boundaries using regex markers.
        
        Returns list of:
            {"page": int, "title": str, "char_offset": int}
        """
        boundaries = []
        compiled_patterns = [re.compile(p, re.MULTILINE | re.IGNORECASE) for p in self.SECTION_MARKERS]
        
        total_offset = 0
        for page in pages:
            for pattern in compiled_patterns:
                for match in pattern.finditer(page.text):
                    boundaries.append({
                        "page": page.page_number,
                        "title": match.group().strip(),
                        "char_offset": total_offset + match.start()
                    })
            total_offset += len(page.text) + 2  # +2 for page separator
        
        # Sort by char_offset
        boundaries.sort(key=lambda x: x["char_offset"])
        
        if boundaries:
            logger.info(f"Detected {len(boundaries)} section boundaries")
        
        return boundaries

    def _intelligent_chunk(self, pages: List[PageText], max_tokens: int = 40000) -> List[str]:
        """
        Split long contracts at section boundaries instead of mid-clause.
        
        Args:
            pages: List of PageText objects
            max_tokens: Approximate max tokens per chunk (~4 chars per token)
        
        Returns:
            List of text chunks with overlap at boundaries
        """
        max_chars = max_tokens * 4  # Rough estimate
        
        # Build full text
        full_text = self._format_pages_for_llm(pages)
        
        # If short enough, return as single chunk
        if len(full_text) <= max_chars:
            logger.info("Contract fits in single chunk, no splitting needed")
            return [full_text]
        
        # Detect section boundaries
        boundaries = self._detect_section_boundaries(pages)
        
        # If no boundaries detected, fall back to page-based splitting
        if not boundaries:
            logger.warning("No section boundaries detected, falling back to page-based splitting")
            return self._chunk_by_pages(pages, max_chars)
        
        # Build chunks at section boundaries
        chunks = []
        current_chunk = ""
        overlap_paragraphs = []  # Last 2 paragraphs for context overlap
        
        # Add boundary offsets to full text splitting
        boundary_offsets = [b["char_offset"] for b in boundaries]
        
        prev_offset = 0
        for i, offset in enumerate(boundary_offsets):
            section_text = full_text[prev_offset:offset]
            
            # Check if adding this section would exceed limit
            if len(current_chunk) + len(section_text) > max_chars and current_chunk:
                # Save current chunk with overlap
                if overlap_paragraphs:
                    current_chunk = "\n\n[...context from previous section...]\n" + "\n\n".join(overlap_paragraphs[-2:]) + "\n\n" + current_chunk
                chunks.append(current_chunk)
                
                # Extract overlap from end of current chunk
                paragraphs = current_chunk.split("\n\n")
                overlap_paragraphs = paragraphs[-2:] if len(paragraphs) >= 2 else paragraphs
                
                # Start new chunk
                current_chunk = section_text
            else:
                current_chunk += section_text
            
            prev_offset = offset
        
        # Add remaining text
        remaining = full_text[prev_offset:]
        if current_chunk or remaining:
            final_chunk = current_chunk + remaining
            if overlap_paragraphs and chunks:  # Not the first chunk
                final_chunk = "\n\n[...context from previous section...]\n" + "\n\n".join(overlap_paragraphs[-2:]) + "\n\n" + final_chunk
            chunks.append(final_chunk)
        
        logger.info(f"Split contract into {len(chunks)} chunks at section boundaries")
        return chunks

    def _chunk_by_pages(self, pages: List[PageText], max_chars: int) -> List[str]:
        """Fallback: chunk by page count when no section markers found."""
        chunks = []
        current_chunk = ""
        
        for page in pages:
            page_text = f"Page {page.page_number}:\n{page.text}\n\n"
            
            if len(current_chunk) + len(page_text) > max_chars and current_chunk:
                chunks.append(current_chunk)
                # Keep last page for overlap
                current_chunk = f"[...continuing from page {page.page_number - 1}...]\n\n" + page_text
            else:
                current_chunk += page_text
        
        if current_chunk:
            chunks.append(current_chunk)
        
        logger.info(f"Split contract into {len(chunks)} chunks by page boundaries")
        return chunks

    # ═══════════════════════════════════════════════════════════════════════════════
    # TABLE EXTRACTION (Payment Schedules, Fee Tables)
    # ═══════════════════════════════════════════════════════════════════════════════
    
    # Keywords that indicate a payment/fee table
    PAYMENT_TABLE_KEYWORDS = {
        'amount', 'fee', 'price', 'cost', 'payment', 'rate',
        'milestone', 'phase', 'schedule', 'date', 'due',
        'total', 'subtotal', 'invoice', 'billing'
    }

    def _extract_tables(self, doc: fitz.Document) -> List[Dict]:
        """
        Extract tables from PDF using PyMuPDF's built-in table detection.
        
        Returns list of:
            {"page": int, "type": str, "headers": list, "rows": list}
        """
        tables_found = []
        
        for page_idx, page in enumerate(doc, start=1):
            try:
                # PyMuPDF table detection
                tables = page.find_tables()
                
                for table_idx, table in enumerate(tables):
                    # Extract table data
                    data = table.extract()
                    
                    if not data or len(data) < 2:  # Need at least header + 1 row
                        continue
                    
                    headers = data[0] if data else []
                    rows = data[1:] if len(data) > 1 else []
                    
                    # Classify table type
                    table_type = self._classify_table(headers)
                    
                    tables_found.append({
                        "page": page_idx,
                        "table_index": table_idx,
                        "type": table_type,
                        "headers": [h.strip() if h else "" for h in headers],
                        "rows": [[cell.strip() if cell else "" for cell in row] for row in rows]
                    })
                    
                    logger.info(f"Extracted {table_type} table from page {page_idx} ({len(rows)} rows)")
                    
            except Exception as e:
                logger.warning(f"Table extraction failed on page {page_idx}: {e}")
                continue
        
        return tables_found

    def _classify_table(self, headers: List[str]) -> str:
        """
        Classify table type based on header keywords.
        
        Returns: "payment_schedule" | "fee_table" | "party_table" | "generic"
        """
        if not headers:
            return "generic"
        
        header_text = " ".join([h.lower() for h in headers if h])
        
        # Check for payment/fee indicators
        payment_matches = sum(1 for kw in self.PAYMENT_TABLE_KEYWORDS if kw in header_text)
        
        if payment_matches >= 2:
            if any(kw in header_text for kw in ['milestone', 'phase', 'schedule']):
                return "payment_schedule"
            return "fee_table"
        
        if any(kw in header_text for kw in ['party', 'name', 'entity', 'signatory']):
            return "party_table"
        
        return "generic"

    def _format_tables_for_llm(self, tables: List[Dict]) -> str:
        """
        Format extracted tables as structured text for LLM context.
        
        Only includes payment_schedule and fee_table types.
        """
        if not tables:
            return ""
        
        relevant_tables = [t for t in tables if t["type"] in ("payment_schedule", "fee_table")]
        
        if not relevant_tables:
            return ""
        
        formatted_parts = ["\n\n═══ EXTRACTED PAYMENT/FEE TABLES ═══\n"]
        
        for table in relevant_tables:
            formatted_parts.append(f"\n[{table['type'].upper()} - Page {table['page']}]")
            
            # Format as simple table
            headers = table["headers"]
            formatted_parts.append(" | ".join(headers))
            formatted_parts.append("-" * 40)
            
            for row in table["rows"][:20]:  # Limit to 20 rows
                formatted_parts.append(" | ".join(row))
            
            if len(table["rows"]) > 20:
                formatted_parts.append(f"... and {len(table['rows']) - 20} more rows")
        
        return "\n".join(formatted_parts)

    def _normalize_payment_schedule(self, tables: List[Dict]) -> List[Dict]:
        """
        Normalize payment schedule tables into structured format.
        
        Returns list of:
            {"milestone": str, "amount": str, "date": str, "description": str}
        """
        normalized = []
        
        for table in tables:
            if table["type"] != "payment_schedule":
                continue
            
            headers = [h.lower() for h in table["headers"]]
            
            # Find column indices
            amount_col = next((i for i, h in enumerate(headers) if any(kw in h for kw in ['amount', 'fee', 'price', 'cost', 'payment'])), None)
            date_col = next((i for i, h in enumerate(headers) if any(kw in h for kw in ['date', 'due', 'when'])), None)
            milestone_col = next((i for i, h in enumerate(headers) if any(kw in h for kw in ['milestone', 'phase', 'deliverable', 'description', 'item'])), None)
            
            for row in table["rows"]:
                entry = {
                    "milestone": row[milestone_col] if milestone_col is not None and milestone_col < len(row) else "",
                    "amount": row[amount_col] if amount_col is not None and amount_col < len(row) else "",
                    "date": row[date_col] if date_col is not None and date_col < len(row) else "",
                    "page": table["page"]
                }
                
                # Only add if we have at least amount or milestone
                if entry["amount"] or entry["milestone"]:
                    normalized.append(entry)
        
        if normalized:
            logger.info(f"Normalized {len(normalized)} payment schedule entries")
        
        return normalized

    def _calculate_cost(self, input_tokens: int, output_tokens: int) -> float:
        return (input_tokens * INPUT_TOKEN_COST) + (output_tokens * OUTPUT_TOKEN_COST)

    @retry(
        retry=retry_if_exception_type((APIError, RateLimitError, Exception)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def _call_claude_with_retry(self, messages, system_prompt, max_tokens):
        """
        Call Claude API with retry logic.

        🔒 FIX: Added 30-second timeout to prevent hanging requests
        """
        if not self.client:
            raise ValueError("Anthropic client not initialized")

        logger.info("Calling Claude API...")

        try:
            response = self.client.messages.create(
                model="claude-sonnet-4-20250514",
                system=system_prompt,
                messages=messages,
                max_tokens=max_tokens,
                temperature=0,
                timeout=60.0  # Increased timeout for larger extractions
            )
            return response

        except Exception as e:
            logger.error(f"Claude API call failed: {e}")
            raise

    def _find_visual_coordinates(self, doc: fitz.Document, page_num: int, text_to_find: str) -> List[List[float]]:
        """
        Searches the actual PDF page for the visual coordinates of the text.
        Returns a list of [x0, y0, x1, y1] rects.
        """
        if not text_to_find or not page_num or page_num < 1 or page_num > doc.page_count:
            return []

        try:
            page = doc[page_num - 1]  # 0-indexed in fitz

            # Clean the search text
            clean_search = " ".join(text_to_find.split())

            # Try exact match
            rects = page.search_for(clean_search)

            # If no match, try shorter snippet
            if not rects and len(clean_search) > 30:
                short_snippet = clean_search[:30]
                rects = page.search_for(short_snippet)

                if not rects:
                    mid = len(clean_search) // 2
                    mid_snippet = clean_search[mid:mid+30]
                    rects = page.search_for(mid_snippet)

            bbox_list = [[r.x0, r.y0, r.x1, r.y1] for r in rects]
            return bbox_list

        except Exception as e:
            logger.warning(f"Error searching for coordinates on page {page_num}: {e}")
            return []

    def _validate_and_fill_gaps(self, initial_results: Dict, pages: List[PageText], target_fields: set) -> Dict:
        """
        Pass 2: Self-correction and gap-filling.
        
        Reviews fields that returned "Not Found" or have suspicious values.
        Uses a focused prompt to re-extract only problematic fields.
        
        Args:
            initial_results: Results from Pass 1
            pages: List of PageText objects
            target_fields: Set of field names to validate (not tier lookup)
        
        Returns:
            Updated results dict with filled gaps and corrections
        """
        if not self.client:
            return initial_results
        
        # Identify fields that need a second look
        fields_to_recheck = []
        
        for field in target_fields:
            result = initial_results.get(field, {})
            value = result.get("value")
            
            # Check for clearly failed extractions
            needs_recheck = False
            
            if value is None or value == "null" or value == "Not Found":
                needs_recheck = True
                logger.info(f"Pass 2: Re-checking '{field}' (was Not Found)")
            elif isinstance(value, str) and len(value.strip()) < 3:
                needs_recheck = True
                logger.info(f"Pass 2: Re-checking '{field}' (suspiciously short: '{value}')")
            
            if needs_recheck:
                fields_to_recheck.append(field)
        
        # Skip Pass 2 if everything looks good
        if not fields_to_recheck:
            logger.info("Pass 2: All fields extracted successfully, skipping re-check")
            return initial_results
        
        logger.info(f"Pass 2: Re-checking {len(fields_to_recheck)} fields: {fields_to_recheck}")
        
        # Build focused re-extraction prompt
        field_descriptions_text = "\n".join([
            f"- {field}: {FIELD_DESCRIPTIONS.get(field, field)}"
            for field in fields_to_recheck
        ])
        
        recheck_prompt_template = os.getenv("CLAUDE_RECHECK_PROMPT", DEFAULT_RECHECK_PROMPT)
        recheck_prompt = f"{recheck_prompt_template}\n\nFIELDS TO RE-EXTRACT:\n{field_descriptions_text}"
        
        # Format text for LLM
        full_text = self._format_pages_for_llm(pages)
        truncated_text, _ = self._smart_truncate(full_text, 80000)  # Smaller for focused search
        
        messages = [{"role": "user", "content": f"Re-analyze this contract:\n\n{truncated_text}"}]
        
        try:
            response = self._call_claude_with_retry(
                messages,
                system_prompt=recheck_prompt,
                max_tokens=2000  # Smaller output for focused fields
            )
            
            if hasattr(response, "usage"):
                cost = self._calculate_cost(response.usage.input_tokens, response.usage.output_tokens)
                logger.info(f"Pass 2 cost: ${cost:.6f} ({response.usage.input_tokens} in, {response.usage.output_tokens} out)")
            
            content_text = response.content[0].text
            
            # Parse JSON
            json_match = re.search(r'\{.*\}', content_text, re.DOTALL)
            if json_match:
                recheck_data = json.loads(json_match.group(0))
            else:
                recheck_data = json.loads(content_text)
            
            # Merge results - only update if new value is better
            updated_results = initial_results.copy()
            fixes_applied = 0
            
            for field, new_result in recheck_data.items():
                if field not in fields_to_recheck:
                    continue
                    
                new_value = new_result.get("value")
                if new_value and new_value != "null" and new_value != "Not Found":
                    updated_results[field] = new_result
                    fixes_applied += 1
                    logger.info(f"Pass 2 FIX: '{field}' -> '{str(new_value)[:50]}...'")
            
            logger.info(f"Pass 2 complete: {fixes_applied}/{len(fields_to_recheck)} gaps filled")
            return updated_results
            
        except Exception as e:
            logger.warning(f"Pass 2 failed, continuing with initial results: {e}")
            return initial_results


    def _build_schema_prompt(self, target_fields: set, tier_label: str = "CUSTOM") -> str:
        """Build the extraction prompt based on target fields.
        
        Args:
            target_fields: Set of field names to extract
            tier_label: Label for logging (e.g., 'essential', 'professional', 'CUSTOM')
        """
        fields = target_fields
        
        # Group fields by category for better organization
        categories = {
            "CORE DATES & PARTIES": ["effective_date", "expiration_date", "parties"],
            "FINANCIAL TERMS": ["total_contract_value", "payment_terms", "currency"],
            "TERMINATION & RENEWAL": ["termination_notice_period", "renewal_terms", "governing_law"],
            "LIABILITY & RISK": ["liability_cap", "indemnification_clauses", "insurance_requirements", "limitation_of_liability"],
            "PERFORMANCE & OBLIGATIONS": ["deliverables", "sla_terms", "performance_metrics", "acceptance_criteria"],
            "INTELLECTUAL PROPERTY": ["ip_ownership", "license_scope", "usage_restrictions"],
            "COMPLIANCE & DISPUTE": ["confidentiality_period", "non_compete_terms", "arbitration_clause", "audit_rights", "data_protection"],
            "ADMINISTRATIVE": ["notice_address", "amendment_process", "assignment_rights", "force_majeure"],
            # NEW EXTENDED CATEGORIES
            "PAYMENT WORKFLOW": ["late_fees", "payment_milestones", "invoice_frequency", "dispute_procedures", "escrow_terms"],
            "COMPLIANCE EXTENDED": ["gdpr_obligations", "ccpa_compliance", "security_standards", "audit_frequency", "certification_requirements"],
            "PERFORMANCE EXTENDED": ["penalties", "cure_periods", "escalation_procedures", "change_order_process", "warranty_terms"],
            "RISK MANAGEMENT": ["risk_allocation", "contingency_provisions", "material_breach_definition", "remedies"],
            "TERMINATION EXTENDED": ["termination_for_cause", "termination_for_convenience", "transition_assistance", "survival_clauses"],
            "IP EXTENDED": ["background_ip", "foreground_ip", "joint_ip", "moral_rights_waiver", "source_code_escrow"],
            "COMMERCIAL TERMS": ["exclusivity", "territory_restrictions", "volume_commitments", "price_adjustments", "benchmarking_rights"],
            "RELATIONSHIP TERMS": ["subcontracting_rights", "key_personnel", "governance_structure", "reporting_requirements"],
        }
        
        # Build field list for prompt
        field_sections = []
        field_count = 0
        
        for category_name, category_fields in categories.items():
            active_fields = [f for f in category_fields if f in fields]
            if active_fields:
                section = f"\n═══ {category_name} ═══"
                for field in active_fields:
                    field_count += 1
                    description = FIELD_DESCRIPTIONS.get(field, field)
                    section += f"\n{field_count}. {field}\n   - {description}"
                field_sections.append(section)
        
        fields_list = "\n".join(field_sections)
        
        # Get prompt from environment or use default
        prompt_template = os.getenv("CLAUDE_EXTRACTION_PROMPT", DEFAULT_EXTRACTION_PROMPT)
        
        # Simple formatting if the template expects it
        try:
             # Check if the template has the formatting keys we expect
             if "{fields_list}" in prompt_template:
                 return prompt_template.format(fields_list=fields_list, tier_label=tier_label.upper())
             else:
                 return f"{prompt_template}\n\nFIELDS TO EXTRACT:\n{fields_list}"
        except Exception:
             return f"{prompt_template}\n\nFIELDS TO EXTRACT:\n{fields_list}"

    def _extract_all_fields_with_llm(self, pages: List[PageText], target_fields: set, tier_label: str = "custom", table_context: str = "") -> Dict[str, Dict[str, Any]]:
        if not self.client:
            logger.warning("No API key available for LLM extraction.")
            return {}

        full_contract_text = self._format_pages_for_llm(pages)
        logger.info(f"Formatted {len(pages)} pages into {len(full_contract_text)} chars for LLM")

        # Adjust truncation based on field count (more fields need more context)
        field_count = len(target_fields)
        if field_count <= 9:
            max_chars = 100000
        elif field_count <= 18:
            max_chars = 125000
        else:
            max_chars = 150000
        
        truncated_text, was_truncated = self._smart_truncate(full_contract_text, max_chars)
        
        # Append table context if we have it (helps with financial field extraction)
        if table_context:
            truncated_text = truncated_text + table_context
            logger.info(f"Added {len(table_context)} chars of table context to LLM input")

        # Build prompt with target fields
        schema_prompt = self._build_schema_prompt(target_fields, tier_label)

        messages = [
            {"role": "user", "content": f"Analyze this contract text:\n\n{truncated_text}"}
        ]

        # Max tokens for all tiers - 25 fields × ~150 tokens = ~4000 output tokens
        # Using 4500 to provide headroom for all extraction tiers (max 25 fields)
        max_tokens = 4500

        try:
            logger.info(f"Sending request to Claude for {tier_label} extraction ({len(target_fields)} fields)...")
            response = self._call_claude_with_retry(
                messages,
                system_prompt=schema_prompt,
                max_tokens=max_tokens
            )

            # 🔒 FIX: Log cost and alert if high
            if hasattr(response, "usage"):
                cost = self._calculate_cost(
                    response.usage.input_tokens,
                    response.usage.output_tokens
                )

                logger.info(
                    f"LLM Usage: {response.usage.input_tokens} input tokens, "
                    f"{response.usage.output_tokens} output tokens, "
                    f"cost=${cost:.6f}"
                )

                # 🔒 FIX: Cost monitoring alert
                if cost > HIGH_COST_ALERT_THRESHOLD:
                    logger.critical(f"🚨 HIGH COST ALERT: ${cost:.4f} for this extraction")

            content_text = response.content[0].text

            # Extract JSON from potential Markdown blocks
            json_match = re.search(r'\{.*\}', content_text, re.DOTALL)
            if json_match:
                json_str = json_match.group(0)
                data = json.loads(json_str)
            else:
                data = json.loads(content_text)

            logger.info("Successfully parsed JSON from LLM response")
            return data

        except json.JSONDecodeError as e:
            logger.error(f"JSON parsing failed: {e}")
            return {}
        except Exception as e:
            logger.error(f"LLM Extraction failed: {e}", exc_info=True)
            return {}

    def _extract_from_chunks(self, chunks: List[str], target_fields: set, tier_label: str = "custom", table_context: str = "") -> Dict[str, Dict[str, Any]]:
        """
        Extract from multiple chunks and merge results.
        
        For long contracts (>50 pages), we extract from each chunk and merge,
        preferring non-null values from any chunk.
        """
        logger.info(f"Multi-chunk extraction: {len(chunks)} chunks")
        
        # Add table context to first chunk only (it's the most relevant)
        chunk_with_tables = chunks[0] + table_context if table_context else chunks[0]
        modified_chunks = [chunk_with_tables] + chunks[1:]
        
        all_results = []
        
        for i, chunk in enumerate(modified_chunks):
            logger.info(f"Processing chunk {i + 1}/{len(modified_chunks)} ({len(chunk)} chars)")
            
            chunk_result = self._extract_from_text_chunk(chunk, target_fields, tier_label)
            if chunk_result:
                all_results.append(chunk_result)
        
        # Merge results: prefer non-null values
        return self._merge_chunk_results(all_results, target_fields)

    def _extract_from_text_chunk(self, text: str, target_fields: set, tier_label: str = "custom") -> Dict[str, Dict[str, Any]]:
        """Extract fields from a single text chunk."""
        if not self.client:
            return {}
        
        truncated, _ = self._smart_truncate(text, 100000)
        schema_prompt = self._build_schema_prompt(target_fields, tier_label)
        
        messages = [
            {"role": "user", "content": f"Analyze this contract text:\n\n{truncated}"}
        ]
        
        try:
            response = self._call_claude_with_retry(
                messages,
                system_prompt=schema_prompt,
                max_tokens=4500
            )
            
            content_text = response.content[0].text
            
            json_match = re.search(r'\{.*\}', content_text, re.DOTALL)
            if json_match:
                return json.loads(json_match.group(0))
            else:
                return json.loads(content_text)
                
        except Exception as e:
            logger.warning(f"Chunk extraction failed: {e}")
            return {}

    def _merge_chunk_results(self, results_list: List[Dict], target_fields: set) -> Dict[str, Dict[str, Any]]:
        """
        Merge extraction results from multiple chunks.
        
        Strategy: For each field, take the first non-null value found.
        This prefers earlier chunks (which usually have the key terms).
        """
        merged = {}
        
        for field in target_fields:
            # Try each result until we find a non-null value
            for result in results_list:
                if field in result:
                    value = result[field].get("value") if isinstance(result[field], dict) else result[field]
                    if value and value != "null" and value != "Not Found":
                        merged[field] = result[field]
                        break
            
            # If still not found, use the first result's value (even if null)
            if field not in merged:
                for result in results_list:
                    if field in result:
                        merged[field] = result[field]
                        break
        
        logger.info(f"Merged {len(merged)} fields from {len(results_list)} chunks")
        return merged

    def _format_pages_for_llm(self, pages: List[PageText]) -> str:
        parts = []
        for page in pages:
            parts.append(f"Page {page.page_number}:\n{page.text}")
        return "\n\n".join(parts)

    def _extract_text_from_pdf(self, doc: fitz.Document) -> List[PageText]:
        pages: List[PageText] = []
        logger.info("Processing %d pages from PDF.", doc.page_count)

        for page_index, page in enumerate(doc, start=1):
            text_content = ""
            try:
                text_content = page.get_text("text")
            except Exception as e:
                logger.warning("Could not extract text from page %d: %s", page_index, e)

            pages.append(PageText(page_number=page_index, text=text_content))

        return pages

    def _preprocess_contract(self, text: str) -> str:
        text = re.sub(r"\s*\n\s*", "\n", text)
        return text.strip()

    @performance_profiler
    def extract_from_pdf(self, pdf_file: Union[bytes, io.BytesIO], tier: str = "essential", custom_fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Extract contract data from PDF.
        
        Args:
            pdf_file: PDF file as bytes or BytesIO
            tier: Extraction tier - 'essential', 'professional', or 'enterprise'
            custom_fields: Optional list of specific field names to extract (overrides tier)
        
        Returns:
            Dictionary with extracted fields and metadata
        """
        # Determine target fields
        if custom_fields:
            # Validate custom fields against ALL_FIELDS
            valid_fields = set(custom_fields) & ALL_FIELDS
            invalid_fields = set(custom_fields) - ALL_FIELDS
            if invalid_fields:
                logger.warning(f"Ignoring unknown custom fields: {invalid_fields}")
            target_fields = valid_fields if valid_fields else TIER_FIELDS.get("essential", set())
            logger.info("="*80)
            logger.info(f"Starting PDF extraction pipeline - CUSTOM ({len(target_fields)} fields)")
            logger.info(f"Fields: {sorted(target_fields)}")
            logger.info("="*80)
        else:
            # Validate tier
            if tier not in TIER_FIELDS:
                logger.warning(f"Invalid tier '{tier}', defaulting to 'essential'")
                tier = "essential"
            target_fields = TIER_FIELDS[tier]
            logger.info("="*80)
            logger.info(f"Starting PDF extraction pipeline - Tier: {tier.upper()} ({len(target_fields)} fields)")
            logger.info("="*80)

        doc = None
        try:
            # 1. Open PDF
            pdf_stream = io.BytesIO(pdf_file) if isinstance(pdf_file, bytes) else pdf_file
            doc = fitz.open(stream=pdf_stream, filetype="pdf")

            if doc.is_encrypted:
                return {"error": "PDF is encrypted and cannot be processed."}

            # 2. Extract Text
            page_texts = self._extract_text_from_pdf(doc)
            page_count = len(page_texts)

            if not any(page.text.strip() for page in page_texts):
                return {"error": "Failed to extract text from PDF."}

            preprocessed_pages = [
                PageText(page_number=p.page_number, text=self._preprocess_contract(p.text))
                for p in page_texts
            ]

            raw_text_len = sum(len(p.text) for p in preprocessed_pages)

            # 2.5 Extract Tables (before LLM - adds context for financial field extraction)
            logger.info("="*80)
            logger.info("TABLE EXTRACTION")
            logger.info("="*80)
            extracted_tables = self._extract_tables(doc)
            table_context = self._format_tables_for_llm(extracted_tables)
            normalized_payments = self._normalize_payment_schedule(extracted_tables)
            
            if extracted_tables:
                logger.info(f"Extracted {len(extracted_tables)} tables, {len(normalized_payments)} payment entries")
            else:
                logger.info("No tables detected in document")

            # 2.6 Smart Chunking for Long Contracts
            use_chunking = page_count > 50
            if use_chunking:
                logger.info("="*80)
                logger.info(f"SMART CHUNKING (document has {page_count} pages)")
                logger.info("="*80)
                chunks = self._intelligent_chunk(preprocessed_pages)
            else:
                chunks = None  # Use single-pass extraction

            # 3. LLM Extraction with target fields
            logger.info("="*80)
            logger.info("PASS 1: Initial Extraction")
            logger.info("="*80)
            
            # Determine tier label for logging
            tier_label = "custom" if custom_fields else tier
            
            if chunks and len(chunks) > 1:
                # Multi-chunk extraction: extract from each chunk and merge
                llm_results = self._extract_from_chunks(chunks, target_fields, tier_label, table_context)
            else:
                # Single-pass extraction (original flow)
                llm_results = self._extract_all_fields_with_llm(preprocessed_pages, target_fields, tier_label, table_context)

            # 4. Pass 2: Validate and fill gaps
            logger.info("="*80)
            logger.info("PASS 2: Validation & Gap-Filling")
            logger.info("="*80)
            validated_results = self._validate_and_fill_gaps(llm_results, preprocessed_pages, target_fields)

            # 5. Pass 3: Build Final Analysis with Grounding Check
            logger.info("="*80)
            logger.info("PASS 3: Grounding Check & Coordinate Mapping")
            logger.info("="*80)
            analysis: Dict[str, Dict[str, Any]] = {}
            grounded_count = 0
            ungrounded_extractive = 0  # Only count extractive fields that failed
            
            # Build full text for fuzzy matching fallback
            full_text = " ".join([p.text for p in preprocessed_pages])

            for key in target_fields:
                item = validated_results.get(key, {})
                val = item.get("value")
                quote = item.get("verbatim_source")
                pg = item.get("page_number")

                bboxes = []
                snippet = None
                source = ExtractionSource.SYSTEM_FALLBACK
                grounded = False
                field_type = "derived" if is_derived_field(key) else "extractive"

                if val and val != "null":
                    source = ExtractionSource.INFERENCE
                    logger.info(f"Grounding '{key}' ({field_type}): Val='{str(val)[:50]}...' (Pg {pg})")

                    # PRIORITY 1: Exact coordinate search for verbatim source
                    if pg and quote:
                        bboxes = self._find_visual_coordinates(doc, pg, quote)
                        if bboxes:
                            snippet = quote
                            grounded = True
                            grounded_count += 1
                            logger.info(f" ✓ GROUNDED: Found exact citation in PDF")

                    # PRIORITY 2: Fallback to value if quote failed
                    if not bboxes and pg and val and isinstance(val, str):
                        bboxes = self._find_visual_coordinates(doc, pg, val)
                        if bboxes:
                            snippet = val
                            grounded = True
                            grounded_count += 1
                            logger.info(f" ✓ GROUNDED: Found value text in PDF")
                    
                    # PRIORITY 3: Fuzzy matching for OCR/line-break tolerance
                    if not grounded and quote:
                        if fuzzy_text_exists(quote, full_text):
                            grounded = True
                            grounded_count += 1
                            logger.info(f" ✓ GROUNDED (fuzzy): Text evidence found despite formatting differences")
                    
                    # PRIORITY 4: For derived fields, check if ANY supporting text exists
                    if not grounded and field_type == "derived" and val:
                        # For derived fields, we just check if key terms from value exist
                        if fuzzy_text_exists(str(val)[:100], full_text):
                            grounded = True
                            grounded_count += 1
                            logger.info(f" ✓ GROUNDED (derived): Supporting evidence found for synthesis field")

                    if not grounded:
                        if field_type == "extractive":
                            ungrounded_extractive += 1
                            logger.warning(f" ✗ UNGROUNDED: No text evidence found for '{key}'")
                        else:
                            # Derived fields without grounding are okay - they're synthesis
                            logger.info(f" ○ UNGROUNDED (derived): Synthesis field without single citation - acceptable")
                else:
                    val = "Not Found"
                    if field_type == "extractive":
                        ungrounded_extractive += 1

                result_obj = ExtractionResult(
                    value=val if isinstance(val, str) else json.dumps(val) if val else "Not Found",
                    source=source,
                    page_number=pg,
                    reference_snippet=snippet,
                    bbox=bboxes if bboxes else None,
                    grounded=grounded,
                    field_type=field_type
                )

                analysis[key] = result_obj.to_dict()

            # Summary logging
            total_fields = len(target_fields)
            extractive_fields = sum(1 for k in target_fields if not is_derived_field(k))
            grounding_rate = (grounded_count / total_fields * 100) if total_fields > 0 else 0
            logger.info("="*80)
            logger.info(f"EXTRACTION COMPLETE: {grounded_count}/{total_fields} fields grounded ({grounding_rate:.1f}%)")
            logger.info(f"  - Extractive fields ungrounded: {ungrounded_extractive}/{extractive_fields}")
            logger.info("="*80)

            return {
                "extraction_timestamp": datetime.now().isoformat(),
                "extraction_tier": tier,
                "credits_used": TIER_CREDITS[tier],
                "fields_extracted": len(target_fields),
                "fields_grounded": grounded_count,
                "grounding_rate": round(grounding_rate, 1),
                "contract_type": "General Agreement",
                "contract_length": raw_text_len,
                "pages_analysed": page_count,
                "chunks_processed": len(chunks) if chunks else 1,
                "tables_extracted": len(extracted_tables),
                "payment_schedule": normalized_payments if normalized_payments else [],
                "analysis": analysis,
            }

        except Exception as e:
            logger.error("Unexpected error processing PDF: %s", e, exc_info=True)
            return {"error": f"Failed to read PDF: {e}"}

        finally:
            if doc:
                doc.close()