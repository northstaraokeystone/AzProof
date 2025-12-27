"""
Medicaid Claim Ingestion Module

Ingests AHCCCS claim data into the receipts stream.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..core import emit_receipt, dual_hash, merkle, TENANT_ID


# Required fields for a valid claim
REQUIRED_CLAIM_FIELDS = [
    "claim_id",
    "provider_id",
    "billed_amount"
]

# Optional but recommended fields
OPTIONAL_CLAIM_FIELDS = [
    "provider_name",
    "patient_id",
    "patient_tribal_affiliation",
    "service_type",
    "service_date",
    "paid_amount",
    "facility_address",
    "facility_type"
]


def validate_claim(claim: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Check required fields and format of a claim.

    Args:
        claim: Claim dictionary to validate

    Returns:
        Tuple of (valid: bool, reason: str)
    """
    for field in REQUIRED_CLAIM_FIELDS:
        if field not in claim:
            return False, f"Missing required field: {field}"

    # Validate claim_id is non-empty
    if not claim.get("claim_id"):
        return False, "claim_id cannot be empty"

    # Validate provider_id is non-empty
    if not claim.get("provider_id"):
        return False, "provider_id cannot be empty"

    # Validate billed_amount is numeric and non-negative
    billed = claim.get("billed_amount")
    if not isinstance(billed, (int, float)):
        return False, "billed_amount must be numeric"
    if billed < 0:
        return False, "billed_amount cannot be negative"

    # Validate service_date format if provided
    if "service_date" in claim and claim["service_date"]:
        try:
            datetime.fromisoformat(claim["service_date"].replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return False, f"Invalid service_date format: {claim['service_date']}"

    return True, "valid"


def ingest_claim(claim: Dict[str, Any], tenant_id: str = TENANT_ID) -> Dict[str, Any]:
    """
    Validate claim structure and emit ingest_receipt.

    Args:
        claim: Claim dictionary
        tenant_id: Tenant identifier

    Returns:
        Receipt dict with claim_hash

    Raises:
        ValueError: If claim is invalid
    """
    valid, reason = validate_claim(claim)
    if not valid:
        raise ValueError(f"Invalid claim: {reason}")

    # Compute claim hash
    claim_hash = dual_hash(str(claim))

    # Determine AIHP flag
    aihp_flag = bool(claim.get("patient_tribal_affiliation"))

    # Build receipt data
    receipt_data = {
        "claim_hash": claim_hash,
        "claim_id": claim.get("claim_id"),
        "provider_id": claim.get("provider_id"),
        "provider_name": claim.get("provider_name"),
        "aihp_flag": aihp_flag,
        "billed_amount": claim.get("billed_amount"),
        "paid_amount": claim.get("paid_amount"),
        "service_type": claim.get("service_type"),
        "facility_type": claim.get("facility_type")
    }

    # Emit receipt
    receipt = emit_receipt("medicaid_ingest", receipt_data, tenant_id)

    return receipt


def batch_ingest(claims: List[Dict[str, Any]], tenant_id: str = TENANT_ID) -> Dict[str, Any]:
    """
    Batch ingest claims with merkle anchor.

    Args:
        claims: List of claim dictionaries
        tenant_id: Tenant identifier

    Returns:
        Batch receipt with merkle_root and individual claim hashes
    """
    if not claims:
        return emit_receipt("medicaid_batch_ingest", {
            "claim_count": 0,
            "merkle_root": merkle([]),
            "claims": []
        }, tenant_id)

    # Process each claim
    receipts = []
    claim_hashes = []
    errors = []

    for i, claim in enumerate(claims):
        try:
            receipt = ingest_claim(claim, tenant_id)
            receipts.append(receipt)
            claim_hashes.append(receipt["claim_hash"])
        except ValueError as e:
            errors.append({"index": i, "error": str(e)})

    # Compute merkle root
    merkle_root = merkle(claim_hashes) if claim_hashes else merkle([])

    # Build batch receipt
    batch_data = {
        "claim_count": len(receipts),
        "error_count": len(errors),
        "merkle_root": merkle_root,
        "claim_hashes": claim_hashes,
        "errors": errors if errors else None
    }

    return emit_receipt("medicaid_batch_ingest", batch_data, tenant_id)


def extract_claims_by_provider(receipts: List[Dict], provider_id: str) -> List[Dict]:
    """
    Extract all claims for a specific provider from receipts.

    Args:
        receipts: List of receipt dicts
        provider_id: Provider ID to filter by

    Returns:
        List of matching receipts
    """
    return [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("provider_id") == provider_id
    ]


def extract_aihp_claims(receipts: List[Dict]) -> List[Dict]:
    """
    Extract all AIHP-flagged claims from receipts.

    Args:
        receipts: List of receipt dicts

    Returns:
        List of AIHP-flagged receipts
    """
    return [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("aihp_flag") is True
    ]
