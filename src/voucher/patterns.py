"""
Spending Pattern Detection Module

Detects spending patterns indicating voucher abuse.
"""

import math
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..core import (
    emit_receipt,
    TENANT_ID,
    ESA_REVIEW_THRESHOLD,
    ESA_EGREGIOUS_KEYWORDS,
    get_risk_level
)


def detect_threshold_gaming(account_id: str, txns: List[Dict]) -> bool:
    """
    Multiple transactions just under $2,000 review threshold.

    Args:
        account_id: Account to analyze
        txns: List of transactions

    Returns:
        True if threshold gaming detected
    """
    account_txns = [t for t in txns if t.get("account_id") == account_id]

    if len(account_txns) < 3:
        return False

    # Look for transactions in the $1,800-$1,999 range
    threshold_zone = 0.9 * ESA_REVIEW_THRESHOLD  # $1,800
    upper_limit = ESA_REVIEW_THRESHOLD - 1  # $1,999

    near_threshold = [
        t for t in account_txns
        if threshold_zone <= t.get("amount", 0) <= upper_limit
    ]

    # Flag if 3+ transactions just under threshold
    if len(near_threshold) >= 3:
        return True

    # Also check for clustering of amounts
    total_near = len(near_threshold)
    total_txns = len(account_txns)

    if total_txns >= 5 and total_near / total_txns >= 0.3:
        return True

    return False


def detect_seasonal_spike(account_id: str, txns: List[Dict]) -> bool:
    """
    Unusual spending at non-school times (summer ski passes).

    Args:
        account_id: Account to analyze
        txns: List of transactions

    Returns:
        True if seasonal spike detected
    """
    account_txns = [t for t in txns if t.get("account_id") == account_id]

    if len(account_txns) < 5:
        return False

    # Group by month
    monthly_spend: Dict[int, float] = defaultdict(float)

    for txn in account_txns:
        txn_date = txn.get("ts") or txn.get("txn_date")
        if not txn_date:
            continue

        try:
            dt = datetime.fromisoformat(txn_date.replace('Z', '+00:00'))
            month = dt.month
            monthly_spend[month] += txn.get("amount", 0)
        except (ValueError, AttributeError):
            pass

    if not monthly_spend:
        return False

    # School months: Sep-May (9, 10, 11, 12, 1, 2, 3, 4, 5)
    school_months = {9, 10, 11, 12, 1, 2, 3, 4, 5}
    summer_months = {6, 7, 8}

    school_spend = sum(monthly_spend.get(m, 0) for m in school_months)
    summer_spend = sum(monthly_spend.get(m, 0) for m in summer_months)

    # Flag if summer spending is disproportionately high
    if summer_spend > 0 and school_spend > 0:
        # Summer is 3 months vs 9 school months
        # So summer should be about 1/3 of school spending
        expected_summer_ratio = 3 / 9  # 0.33
        actual_ratio = summer_spend / school_spend

        if actual_ratio > expected_summer_ratio * 2:  # More than 0.66
            return True

    # Also check for large winter purchases (ski season: Dec-Mar)
    ski_months = {12, 1, 2, 3}
    ski_spend = sum(monthly_spend.get(m, 0) for m in ski_months)

    if ski_spend > 0:
        # Check if ski-season spending is concentrated
        ski_txns = [
            t for t in account_txns
            if _get_month(t) in ski_months
        ]

        for txn in ski_txns:
            desc = str(txn.get("description", "")).lower()
            merchant = str(txn.get("merchant_name", "")).lower()
            if "ski" in desc or "ski" in merchant or "snowbowl" in merchant:
                return True

    return False


def _get_month(txn: Dict) -> Optional[int]:
    """Extract month from transaction date."""
    txn_date = txn.get("ts") or txn.get("txn_date")
    if not txn_date:
        return None
    try:
        dt = datetime.fromisoformat(txn_date.replace('Z', '+00:00'))
        return dt.month
    except (ValueError, AttributeError):
        return None


def compute_peer_deviation(
    account_id: str,
    txns: List[Dict],
    baseline: Optional[Dict] = None
) -> float:
    """
    Compare to peer spending patterns. Return sigma.

    Args:
        account_id: Account to analyze
        txns: All transactions
        baseline: Optional baseline stats

    Returns:
        Deviation in standard deviations
    """
    account_txns = [t for t in txns if t.get("account_id") == account_id]

    if not account_txns:
        return 0.0

    # Calculate account metrics
    account_total = sum(t.get("amount", 0) for t in account_txns)
    account_avg = account_total / len(account_txns) if account_txns else 0

    # Get or compute baseline
    if baseline is None:
        # Compute from all accounts
        account_totals: Dict[str, float] = defaultdict(float)
        account_counts: Dict[str, int] = defaultdict(int)

        for txn in txns:
            acc_id = txn.get("account_id")
            if acc_id:
                account_totals[acc_id] += txn.get("amount", 0)
                account_counts[acc_id] += 1

        if not account_totals:
            return 0.0

        totals = list(account_totals.values())
        avg_total = sum(totals) / len(totals)
        std_total = _std(totals)

        baseline = {
            "avg_total": avg_total,
            "std_total": std_total
        }

    # Calculate deviation
    std = baseline.get("std_total", 1)
    if std == 0:
        std = 1

    deviation = (account_total - baseline.get("avg_total", 0)) / std

    return deviation


def _std(values: List[float]) -> float:
    """Calculate standard deviation."""
    if not values or len(values) < 2:
        return 1.0
    mean = sum(values) / len(values)
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance)


def flag_egregious_items(txns: List[Dict]) -> List[Dict]:
    """
    Flag documented abuse items (pianos, ski equipment, etc.).

    Args:
        txns: List of transactions

    Returns:
        List of flagged transactions
    """
    flagged = []

    for txn in txns:
        description = str(txn.get("description", "")).lower()
        merchant_name = str(txn.get("merchant_name", "")).lower()
        amount = txn.get("amount", 0)

        combined_text = f"{description} {merchant_name}"

        egregious_match = None
        for keyword in ESA_EGREGIOUS_KEYWORDS:
            if keyword in combined_text:
                egregious_match = keyword
                break

        if egregious_match:
            # Additional checks for specific items
            risk_level = "high"

            # Piano over $1,000
            if egregious_match == "piano" and amount >= 1000:
                risk_level = "critical"

            # Ski equipment/passes
            if egregious_match in ["ski", "snowbowl"]:
                risk_level = "critical"

            flagged.append({
                **txn,
                "egregious_keyword": egregious_match,
                "risk_level": risk_level,
                "flag_reason": f"egregious_item:{egregious_match}"
            })

    return flagged


def analyze_account_patterns(
    account_id: str,
    txns: List[Dict],
    tenant_id: str = TENANT_ID
) -> Optional[Dict]:
    """
    Full pattern analysis with receipt emission.

    Args:
        account_id: Account to analyze
        txns: All transactions
        tenant_id: Tenant identifier

    Returns:
        Pattern receipt if patterns detected, None otherwise
    """
    account_txns = [t for t in txns if t.get("account_id") == account_id]

    if not account_txns:
        return None

    # Run all pattern checks
    threshold_gaming = detect_threshold_gaming(account_id, txns)
    seasonal_spike = detect_seasonal_spike(account_id, txns)
    peer_deviation = compute_peer_deviation(account_id, txns)
    egregious = flag_egregious_items(account_txns)

    # Collect detected patterns
    patterns = []
    evidence = {}

    if threshold_gaming:
        patterns.append("threshold_gaming")
        near_threshold = [
            t.get("amount") for t in account_txns
            if 0.9 * ESA_REVIEW_THRESHOLD <= t.get("amount", 0) < ESA_REVIEW_THRESHOLD
        ]
        evidence["threshold_gaming"] = {
            "near_threshold_count": len(near_threshold),
            "amounts": near_threshold[:5]
        }

    if seasonal_spike:
        patterns.append("seasonal_spike")
        evidence["seasonal_spike"] = {"detected": True}

    if abs(peer_deviation) > 2:
        patterns.append("peer_deviation")
        evidence["peer_deviation"] = {"sigma": peer_deviation}

    if egregious:
        patterns.append("egregious_item")
        evidence["egregious_items"] = [
            {"keyword": e["egregious_keyword"], "amount": e.get("amount")}
            for e in egregious[:5]
        ]

    if not patterns:
        return None

    # Calculate overall risk
    risk_score = len(patterns) * 0.2 + abs(peer_deviation) * 0.1
    if egregious:
        risk_score += 0.3 * len(egregious)

    risk_level = get_risk_level(min(1.0, risk_score))

    receipt_data = {
        "account_id": account_id,
        "pattern_type": patterns[0] if len(patterns) == 1 else "multiple",
        "patterns": patterns,
        "evidence": evidence,
        "risk_level": risk_level,
        "risk_score": min(1.0, risk_score),
        "txn_count": len(account_txns),
        "total_amount": sum(t.get("amount", 0) for t in account_txns)
    }

    return emit_receipt("voucher_pattern", receipt_data, tenant_id)
