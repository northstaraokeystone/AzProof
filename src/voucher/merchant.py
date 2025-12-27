"""
Merchant Mapping and Flagging Module

Build merchant database with educational classification.
"""

import gzip
import json
import math
from collections import defaultdict
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, TENANT_ID, get_risk_level


# Merchant thresholds
MERCHANT_REVIEW_THRESHOLD = 10_000  # New merchant + >$10K = review
MERCHANT_FRONT_THRESHOLD = 0.7  # Front score > 0.7 = flag


def build_merchant_index(txns: List[Dict]) -> Dict[str, Dict]:
    """
    Index merchants by ID with category, total spend, frequency.

    Args:
        txns: List of transactions

    Returns:
        Dict mapping merchant_id to merchant info
    """
    merchants: Dict[str, Dict] = {}

    for txn in txns:
        merchant_id = txn.get("merchant_id")
        if not merchant_id:
            continue

        if merchant_id not in merchants:
            merchants[merchant_id] = {
                "merchant_id": merchant_id,
                "merchant_name": txn.get("merchant_name"),
                "merchant_category_code": txn.get("merchant_category_code"),
                "total_spend": 0.0,
                "txn_count": 0,
                "accounts": set(),
                "first_seen": txn.get("ts") or txn.get("txn_date"),
                "last_seen": txn.get("ts") or txn.get("txn_date"),
                "amounts": []
            }

        merchant = merchants[merchant_id]
        amount = txn.get("amount", 0)

        merchant["total_spend"] += amount
        merchant["txn_count"] += 1
        merchant["amounts"].append(amount)

        if txn.get("account_id"):
            merchant["accounts"].add(txn.get("account_id"))

        # Update last_seen
        txn_date = txn.get("ts") or txn.get("txn_date")
        if txn_date:
            if merchant["last_seen"] is None or txn_date > merchant["last_seen"]:
                merchant["last_seen"] = txn_date

    # Convert sets to counts and compute stats
    for merchant in merchants.values():
        merchant["unique_accounts"] = len(merchant["accounts"])
        del merchant["accounts"]

        amounts = merchant.get("amounts", [])
        if amounts:
            merchant["avg_amount"] = sum(amounts) / len(amounts)
            merchant["max_amount"] = max(amounts)
            merchant["min_amount"] = min(amounts)
        del merchant["amounts"]

    return merchants


def flag_new_merchant(merchant: Dict, existing_merchants: Optional[Dict] = None) -> bool:
    """
    Flag first-time merchants for review.

    Args:
        merchant: Merchant dict to check
        existing_merchants: Optional dict of known merchants

    Returns:
        True if merchant should be flagged for review
    """
    merchant_id = merchant.get("merchant_id")

    # Check if in existing merchants
    if existing_merchants and merchant_id in existing_merchants:
        return False

    # Flag if high volume
    if merchant.get("total_spend", 0) >= MERCHANT_REVIEW_THRESHOLD:
        return True

    # Flag if many unique accounts quickly
    if merchant.get("unique_accounts", 0) >= 10:
        return True

    return False


def detect_merchant_front(merchant_id: str, txns: List[Dict]) -> float:
    """
    Score likelihood merchant is front (educational name, non-ed goods).

    Args:
        merchant_id: Merchant to analyze
        txns: List of transactions

    Returns:
        Front score (0.0 to 1.0)
    """
    from .category import classify_transaction, NON_EDUCATIONAL_MERCHANTS

    merchant_txns = [t for t in txns if t.get("merchant_id") == merchant_id]

    if not merchant_txns:
        return 0.0

    # Get merchant name
    merchant_name = ""
    for txn in merchant_txns:
        if txn.get("merchant_name"):
            merchant_name = txn.get("merchant_name", "").lower()
            break

    # Check for educational keywords in name
    educational_name_keywords = [
        "academy", "school", "learning", "education", "tutor",
        "curriculum", "study", "college", "prep", "teach"
    ]

    has_educational_name = any(
        keyword in merchant_name for keyword in educational_name_keywords
    )

    # Classify transactions
    non_educational_count = 0
    total_classified = 0

    for txn in merchant_txns:
        classification = classify_transaction(txn)
        total_classified += 1
        if classification.get("category") == "non_educational":
            non_educational_count += 1

    if total_classified == 0:
        return 0.0

    non_ed_ratio = non_educational_count / total_classified

    # Front score: educational name + non-educational transactions
    front_score = 0.0

    if has_educational_name:
        # Higher score if name is educational but transactions aren't
        front_score = non_ed_ratio * 0.8

        # Additional penalty for non-educational merchant patterns in name
        for pattern in NON_EDUCATIONAL_MERCHANTS:
            if pattern in merchant_name:
                front_score += 0.2
                break
    else:
        # Lower front likelihood if name isn't trying to appear educational
        front_score = non_ed_ratio * 0.3

    return min(1.0, front_score)


def compute_merchant_entropy(merchant_id: str, txns: List[Dict]) -> float:
    """
    Entropy of transaction patterns. Low = suspicious regularity.

    Args:
        merchant_id: Merchant to analyze
        txns: List of transactions

    Returns:
        Entropy value (bits)
    """
    merchant_txns = [t for t in txns if t.get("merchant_id") == merchant_id]

    if len(merchant_txns) < 2:
        return 0.0

    # Analyze amount distribution
    amounts = [t.get("amount", 0) for t in merchant_txns]

    if not amounts:
        return 0.0

    # Bin amounts
    min_amount = min(amounts)
    max_amount = max(amounts)

    if min_amount == max_amount:
        return 0.0  # All same amount = zero entropy

    # Create bins
    n_bins = min(10, len(amounts))
    bin_width = (max_amount - min_amount) / n_bins

    bins: Dict[int, int] = defaultdict(int)
    for amount in amounts:
        bin_idx = int((amount - min_amount) / bin_width) if bin_width > 0 else 0
        bin_idx = min(bin_idx, n_bins - 1)
        bins[bin_idx] += 1

    # Calculate Shannon entropy
    total = sum(bins.values())
    entropy = 0.0

    for count in bins.values():
        if count > 0:
            p = count / total
            entropy -= p * math.log2(p)

    return entropy


def analyze_merchant(
    merchant_id: str,
    txns: List[Dict],
    existing_merchants: Optional[Dict] = None,
    tenant_id: str = TENANT_ID
) -> Optional[Dict]:
    """
    Full merchant analysis with receipt emission.

    Args:
        merchant_id: Merchant to analyze
        txns: All transactions
        existing_merchants: Optional dict of known merchants
        tenant_id: Tenant identifier

    Returns:
        Merchant flag receipt if flagged, None otherwise
    """
    # Build merchant info
    merchant_txns = [t for t in txns if t.get("merchant_id") == merchant_id]

    if not merchant_txns:
        return None

    # Get merchant info from first transaction
    merchant_name = merchant_txns[0].get("merchant_name", "")
    total_spend = sum(t.get("amount", 0) for t in merchant_txns)
    txn_count = len(merchant_txns)

    # Calculate scores
    front_score = detect_merchant_front(merchant_id, txns)
    entropy = compute_merchant_entropy(merchant_id, txns)

    # Determine if should flag
    is_new = existing_merchants is None or merchant_id not in existing_merchants
    high_volume = total_spend >= MERCHANT_REVIEW_THRESHOLD
    suspected_front = front_score >= MERCHANT_FRONT_THRESHOLD
    low_entropy = entropy < 1.0 and txn_count >= 5

    flag_reasons = []
    if is_new and high_volume:
        flag_reasons.append("new_high_volume")
    if suspected_front:
        flag_reasons.append("front_suspected")
    if low_entropy:
        flag_reasons.append("suspicious_regularity")

    if not flag_reasons:
        return None

    receipt_data = {
        "merchant_id": merchant_id,
        "merchant_name": merchant_name,
        "flag_reason": flag_reasons[0] if len(flag_reasons) == 1 else "multiple",
        "flag_reasons": flag_reasons,
        "total_spend": total_spend,
        "txn_count": txn_count,
        "front_score": front_score,
        "entropy": entropy,
        "is_new": is_new,
        "risk_level": get_risk_level(front_score)
    }

    return emit_receipt("merchant_flag", receipt_data, tenant_id)
