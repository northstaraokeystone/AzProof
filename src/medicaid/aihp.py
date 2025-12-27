"""
American Indian Health Program (AIHP) Exploitation Detection Module

AIHP is fee-for-service (not managed care), making it vulnerable to
billing without utilization controls. This module detects exploitation patterns.
"""

from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from ..core import (
    emit_receipt,
    TENANT_ID,
    AIHP_CONCENTRATION_THRESHOLD,
    get_risk_level
)


def flag_aihp_claims(receipts: List[Dict]) -> List[Dict]:
    """
    Filter claims with tribal affiliation.

    Args:
        receipts: List of medicaid_ingest receipts

    Returns:
        List of AIHP-flagged receipts
    """
    return [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("aihp_flag") is True
    ]


def detect_geographic_mismatch(claims: List[Dict], reservations: Optional[List[Dict]] = None) -> List[Dict]:
    """
    Flag when provider is far from reservation but billing AIHP.

    Args:
        claims: List of AIHP claims
        reservations: Optional list of reservation locations for distance calculation

    Returns:
        List of flagged claims with geographic mismatch
    """
    # Default Arizona reservation areas (approximate)
    default_reservations = [
        {"name": "Navajo Nation", "lat": 36.0, "lon": -110.0},
        {"name": "Salt River Pima-Maricopa", "lat": 33.5, "lon": -111.8},
        {"name": "Gila River", "lat": 33.0, "lon": -111.9},
        {"name": "Tohono O'odham", "lat": 32.0, "lon": -112.0},
        {"name": "San Carlos Apache", "lat": 33.3, "lon": -110.5},
        {"name": "White Mountain Apache", "lat": 33.8, "lon": -109.8}
    ]

    res_locations = reservations or default_reservations
    flagged = []

    for claim in claims:
        if not claim.get("aihp_flag"):
            continue

        facility_address = claim.get("facility_address", "").lower()

        # Check if facility is in known non-reservation urban areas
        urban_indicators = ["phoenix", "tucson", "scottsdale", "mesa", "tempe", "chandler", "gilbert"]
        is_urban = any(ind in facility_address for ind in urban_indicators)

        # Flag if urban location billing AIHP
        if is_urban:
            flagged.append({
                **claim,
                "mismatch_reason": "urban_location",
                "facility_address": facility_address
            })

    return flagged


def compute_aihp_concentration(provider_id: str, receipts: List[Dict]) -> float:
    """
    Ratio of AIHP claims to total claims. >80% = flag.

    Args:
        provider_id: Provider to analyze
        receipts: List of medicaid_ingest receipts

    Returns:
        AIHP concentration ratio (0.0 to 1.0)
    """
    provider_claims = [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("provider_id") == provider_id
    ]

    if not provider_claims:
        return 0.0

    aihp_claims = [c for c in provider_claims if c.get("aihp_flag")]

    return len(aihp_claims) / len(provider_claims)


def detect_recruitment_patterns(claims: List[Dict], window_days: int = 30, min_patients: int = 10) -> List[Dict]:
    """
    Multiple new patients from same source in short window.

    Args:
        claims: List of claims to analyze
        window_days: Time window in days
        min_patients: Minimum patient count to flag

    Returns:
        List of flagged recruitment patterns
    """
    # Group claims by provider
    provider_claims: Dict[str, List[Dict]] = defaultdict(list)

    for claim in claims:
        if claim.get("receipt_type") == "medicaid_ingest":
            provider_id = claim.get("provider_id")
            if provider_id:
                provider_claims[provider_id].append(claim)

    patterns = []

    for provider_id, claims_list in provider_claims.items():
        # Sort by date
        try:
            sorted_claims = sorted(
                claims_list,
                key=lambda c: c.get("ts", "") or c.get("service_date", "")
            )
        except (TypeError, ValueError):
            sorted_claims = claims_list

        # Find bursts of new patients
        patient_first_seen: Dict[str, str] = {}
        bursts: List[List[str]] = []
        current_burst: List[str] = []
        last_date = None

        for claim in sorted_claims:
            patient_id = claim.get("patient_id")
            claim_date = claim.get("ts") or claim.get("service_date")

            if not patient_id or not claim_date:
                continue

            # Check if new patient
            if patient_id not in patient_first_seen:
                patient_first_seen[patient_id] = claim_date

                # Check if within window of last new patient
                if last_date:
                    try:
                        current_dt = datetime.fromisoformat(claim_date.replace('Z', '+00:00'))
                        last_dt = datetime.fromisoformat(last_date.replace('Z', '+00:00'))
                        if (current_dt - last_dt).days <= window_days:
                            current_burst.append(patient_id)
                        else:
                            if len(current_burst) >= min_patients:
                                bursts.append(current_burst)
                            current_burst = [patient_id]
                    except (ValueError, AttributeError):
                        current_burst.append(patient_id)
                else:
                    current_burst = [patient_id]

                last_date = claim_date

        # Check final burst
        if len(current_burst) >= min_patients:
            bursts.append(current_burst)

        # Flag if bursts found
        for burst in bursts:
            patterns.append({
                "provider_id": provider_id,
                "patient_count": len(burst),
                "patients": burst[:10],  # First 10 for brevity
                "pattern_type": "recruitment_burst",
                "window_days": window_days
            })

    return patterns


def analyze_aihp_exploitation(
    provider_id: str,
    receipts: List[Dict],
    tenant_id: str = TENANT_ID
) -> Dict:
    """
    Full AIHP exploitation analysis with receipt emission.

    Args:
        provider_id: Provider to analyze
        receipts: All receipts
        tenant_id: Tenant identifier

    Returns:
        AIHP flag receipt
    """
    # Get provider's claims
    provider_claims = [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("provider_id") == provider_id
    ]

    # Get AIHP claims
    aihp_claims = flag_aihp_claims(provider_claims)

    # Compute metrics
    concentration = compute_aihp_concentration(provider_id, receipts)
    geo_mismatches = detect_geographic_mismatch(aihp_claims)
    recruitment = detect_recruitment_patterns(provider_claims)

    # Calculate risk score
    risk_score = 0.0
    if concentration > AIHP_CONCENTRATION_THRESHOLD:
        risk_score += 0.4
    if geo_mismatches:
        risk_score += 0.3
    if recruitment:
        risk_score += 0.3

    risk_level = get_risk_level(risk_score)

    receipt_data = {
        "provider_id": provider_id,
        "aihp_concentration": concentration,
        "aihp_threshold": AIHP_CONCENTRATION_THRESHOLD,
        "geographic_mismatch": len(geo_mismatches) > 0,
        "mismatch_count": len(geo_mismatches),
        "recruitment_score": len(recruitment) / max(1, len(provider_claims)),
        "recruitment_patterns": len(recruitment),
        "risk_level": risk_level,
        "risk_score": risk_score
    }

    return emit_receipt("aihp_flag", receipt_data, tenant_id)
