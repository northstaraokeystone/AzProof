"""
Billing Anomaly Detection Module

Detects billing anomalies: ghost claims, impossible volumes, pattern deviation.
"""

import gzip
import json
import math
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from ..core import (
    emit_receipt,
    TENANT_ID,
    MAX_PATIENTS_PER_PROVIDER_DAY,
    COMPRESSION_FRAUD_THRESHOLD,
    get_risk_level
)


def compute_billing_velocity(
    provider_id: str,
    receipts: List[Dict],
    window: str = "day"
) -> float:
    """
    Claims per day/week/month. Compare to service_type baseline.

    Args:
        provider_id: Provider to analyze
        receipts: List of receipts
        window: Time window ("day", "week", "month")

    Returns:
        Average claims per window period
    """
    provider_claims = [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("provider_id") == provider_id
    ]

    if not provider_claims:
        return 0.0

    # Group by date
    claims_by_date: Dict[str, int] = defaultdict(int)

    for claim in provider_claims:
        ts = claim.get("ts") or claim.get("service_date")
        if ts:
            try:
                dt = datetime.fromisoformat(ts.replace('Z', '+00:00'))
                if window == "day":
                    key = dt.strftime("%Y-%m-%d")
                elif window == "week":
                    key = dt.strftime("%Y-W%W")
                else:  # month
                    key = dt.strftime("%Y-%m")
                claims_by_date[key] += 1
            except (ValueError, AttributeError):
                pass

    if not claims_by_date:
        return float(len(provider_claims))

    return sum(claims_by_date.values()) / len(claims_by_date)


def detect_impossible_volume(provider_id: str, receipts: List[Dict]) -> bool:
    """
    Flag if billing > physically possible (e.g., 50 patients/day/provider).

    Args:
        provider_id: Provider to analyze
        receipts: List of receipts

    Returns:
        True if impossible volume detected
    """
    velocity = compute_billing_velocity(provider_id, receipts, "day")
    return velocity > MAX_PATIENTS_PER_PROVIDER_DAY


def compression_ratio_billing(claims: List[Dict]) -> float:
    """
    Compress claim patterns. Low ratio = anomalous.

    Args:
        claims: List of claims to compress

    Returns:
        Compression ratio (compressed_size / original_size)
    """
    if not claims:
        return 1.0

    # Extract key billing patterns
    patterns = []
    for claim in claims:
        pattern = {
            "provider_id": claim.get("provider_id"),
            "service_type": claim.get("service_type"),
            "facility_type": claim.get("facility_type"),
            "billed_amount": claim.get("billed_amount")
        }
        patterns.append(pattern)

    # Serialize and compress
    original = json.dumps(patterns, sort_keys=True).encode('utf-8')
    compressed = gzip.compress(original)

    return len(compressed) / len(original)


def detect_upcoding(claims: List[Dict], threshold: float = 0.8) -> List[Dict]:
    """
    Flag consistent billing at highest rate codes.

    Args:
        claims: List of claims
        threshold: Ratio above which to flag (default 0.8)

    Returns:
        List of flagged upcoding patterns
    """
    # Group by provider
    provider_claims: Dict[str, List[Dict]] = defaultdict(list)

    for claim in claims:
        provider_id = claim.get("provider_id")
        if provider_id:
            provider_claims[provider_id].append(claim)

    flagged = []

    for provider_id, claims_list in provider_claims.items():
        # Analyze billed amounts
        amounts = [c.get("billed_amount", 0) for c in claims_list if c.get("billed_amount")]

        if len(amounts) < 10:  # Need minimum claims
            continue

        # Find the highest amount tier
        max_amount = max(amounts)
        high_tier_threshold = max_amount * 0.8  # Within 80% of max

        high_tier_count = sum(1 for a in amounts if a >= high_tier_threshold)
        high_tier_ratio = high_tier_count / len(amounts)

        if high_tier_ratio >= threshold:
            flagged.append({
                "provider_id": provider_id,
                "claim_count": len(claims_list),
                "high_tier_ratio": high_tier_ratio,
                "max_amount": max_amount,
                "pattern_type": "upcoding"
            })

    return flagged


def compare_to_baseline(
    provider_id: str,
    receipts: List[Dict],
    baseline: Optional[Dict] = None
) -> Dict:
    """
    Compare provider to peer baseline. Return deviation metrics.

    Args:
        provider_id: Provider to analyze
        receipts: All receipts
        baseline: Optional baseline dict (computed if not provided)

    Returns:
        Dict with deviation metrics
    """
    provider_claims = [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("provider_id") == provider_id
    ]

    if not provider_claims:
        return {"error": "no_claims_found"}

    # Compute provider metrics
    provider_velocity = compute_billing_velocity(provider_id, receipts, "day")
    provider_amounts = [c.get("billed_amount", 0) for c in provider_claims if c.get("billed_amount")]
    provider_avg_amount = sum(provider_amounts) / len(provider_amounts) if provider_amounts else 0

    # Get or compute baseline
    if baseline is None:
        # Compute from all providers
        all_claims = [r for r in receipts if r.get("receipt_type") == "medicaid_ingest"]
        all_amounts = [c.get("billed_amount", 0) for c in all_claims if c.get("billed_amount")]

        # Get unique providers
        providers = set(c.get("provider_id") for c in all_claims if c.get("provider_id"))
        velocities = [compute_billing_velocity(p, receipts, "day") for p in providers]

        baseline = {
            "avg_velocity": sum(velocities) / len(velocities) if velocities else 0,
            "std_velocity": _std(velocities) if velocities else 1,
            "avg_amount": sum(all_amounts) / len(all_amounts) if all_amounts else 0,
            "std_amount": _std(all_amounts) if all_amounts else 1
        }

    # Compute deviations
    velocity_sigma = (provider_velocity - baseline["avg_velocity"]) / max(baseline["std_velocity"], 0.001)
    amount_sigma = (provider_avg_amount - baseline["avg_amount"]) / max(baseline["std_amount"], 0.001)

    return {
        "provider_id": provider_id,
        "provider_velocity": provider_velocity,
        "provider_avg_amount": provider_avg_amount,
        "baseline_velocity": baseline["avg_velocity"],
        "baseline_amount": baseline["avg_amount"],
        "velocity_deviation_sigma": velocity_sigma,
        "amount_deviation_sigma": amount_sigma,
        "combined_deviation": math.sqrt(velocity_sigma**2 + amount_sigma**2)
    }


def _std(values: List[float]) -> float:
    """Calculate standard deviation."""
    if not values or len(values) < 2:
        return 1.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def analyze_billing_anomalies(
    provider_id: str,
    receipts: List[Dict],
    tenant_id: str = TENANT_ID
) -> Optional[Dict]:
    """
    Full billing anomaly analysis with receipt emission.

    Args:
        provider_id: Provider to analyze
        receipts: All receipts
        tenant_id: Tenant identifier

    Returns:
        Billing anomaly receipt if anomalies found, None otherwise
    """
    provider_claims = [
        r for r in receipts
        if r.get("receipt_type") == "medicaid_ingest"
        and r.get("provider_id") == provider_id
    ]

    if not provider_claims:
        return None

    # Run all checks
    impossible = detect_impossible_volume(provider_id, receipts)
    compression = compression_ratio_billing(provider_claims)
    upcoding = detect_upcoding(provider_claims)
    baseline_comparison = compare_to_baseline(provider_id, receipts)

    # Determine anomaly type and severity
    anomalies = []

    if impossible:
        anomalies.append("impossible_volume")

    if compression < COMPRESSION_FRAUD_THRESHOLD:
        anomalies.append("low_compression")

    if upcoding:
        anomalies.append("upcoding")

    velocity_sigma = baseline_comparison.get("velocity_deviation_sigma", 0)
    if abs(velocity_sigma) > 3:
        anomalies.append("velocity_spike")

    if not anomalies:
        return None

    # Calculate risk
    risk_score = min(1.0, len(anomalies) * 0.25 + abs(velocity_sigma) * 0.1)
    risk_level = get_risk_level(risk_score)

    receipt_data = {
        "provider_id": provider_id,
        "anomaly_type": anomalies[0] if len(anomalies) == 1 else "multiple",
        "anomalies": anomalies,
        "metric_value": compression if "low_compression" in anomalies else velocity_sigma,
        "baseline_value": COMPRESSION_FRAUD_THRESHOLD if "low_compression" in anomalies else baseline_comparison.get("baseline_velocity", 0),
        "deviation_sigma": velocity_sigma,
        "compression_ratio": compression,
        "risk_level": risk_level
    }

    return emit_receipt("billing_anomaly", receipt_data, tenant_id)
