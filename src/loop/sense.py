"""
Receipt Stream Sensing Module

Query and filter the receipt stream for recent activity.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ..core import load_receipts, TENANT_ID


def sense_receipts(
    since_minutes: int = 60,
    receipt_types: Optional[List[str]] = None,
    tenant_id: str = TENANT_ID
) -> List[Dict]:
    """
    Query receipt stream for recent activity.

    Args:
        since_minutes: Look back this many minutes
        receipt_types: Optional list of receipt types to filter
        tenant_id: Tenant to filter by

    Returns:
        List of recent receipts
    """
    all_receipts = load_receipts()

    # Calculate cutoff time
    cutoff = datetime.now(timezone.utc) - timedelta(minutes=since_minutes)
    cutoff_str = cutoff.isoformat()

    # Filter by time and tenant
    recent = []
    for receipt in all_receipts:
        ts = receipt.get("ts", "")
        if ts >= cutoff_str and receipt.get("tenant_id") == tenant_id:
            if receipt_types is None or receipt.get("receipt_type") in receipt_types:
                recent.append(receipt)

    return recent


def query_recent(
    minutes: int = 60,
    limit: Optional[int] = None
) -> List[Dict]:
    """
    Query recent receipts with optional limit.

    Args:
        minutes: Look back period
        limit: Maximum number of receipts to return

    Returns:
        List of recent receipts
    """
    receipts = sense_receipts(since_minutes=minutes)

    # Sort by timestamp descending
    receipts.sort(key=lambda r: r.get("ts", ""), reverse=True)

    if limit:
        return receipts[:limit]

    return receipts


def filter_by_type(
    receipts: List[Dict],
    receipt_type: str
) -> List[Dict]:
    """
    Filter receipts by type.

    Args:
        receipts: List of receipts
        receipt_type: Type to filter by

    Returns:
        Filtered list
    """
    return [r for r in receipts if r.get("receipt_type") == receipt_type]


def filter_by_domain(
    receipts: List[Dict],
    domain: str
) -> List[Dict]:
    """
    Filter receipts by domain (medicaid, voucher, fiscal).

    Args:
        receipts: List of receipts
        domain: Domain to filter by

    Returns:
        Filtered list
    """
    domain_types = {
        "medicaid": [
            "medicaid_ingest", "medicaid_batch_ingest",
            "network_analysis", "aihp_flag", "shell_detection", "billing_anomaly"
        ],
        "voucher": [
            "voucher_ingest", "voucher_batch_ingest",
            "voucher_category", "merchant_flag", "voucher_pattern"
        ],
        "fiscal": [
            "revenue_ingest", "policy_ingest", "policy_tracking", "fiscal_analysis"
        ],
        "entropy": [
            "entropy_analysis"
        ],
        "loop": [
            "gap", "helper_blueprint", "loop_cycle"
        ]
    }

    valid_types = domain_types.get(domain, [])
    return [r for r in receipts if r.get("receipt_type") in valid_types]


def count_by_type(receipts: List[Dict]) -> Dict[str, int]:
    """
    Count receipts by type.

    Args:
        receipts: List of receipts

    Returns:
        Dict mapping type to count
    """
    counts: Dict[str, int] = {}

    for receipt in receipts:
        rtype = receipt.get("receipt_type", "unknown")
        counts[rtype] = counts.get(rtype, 0) + 1

    return counts


def summarize_activity(minutes: int = 60) -> Dict[str, Any]:
    """
    Summarize recent activity.

    Args:
        minutes: Look back period

    Returns:
        Summary dict
    """
    receipts = sense_receipts(since_minutes=minutes)

    return {
        "period_minutes": minutes,
        "total_receipts": len(receipts),
        "by_type": count_by_type(receipts),
        "domains": {
            "medicaid": len(filter_by_domain(receipts, "medicaid")),
            "voucher": len(filter_by_domain(receipts, "voucher")),
            "fiscal": len(filter_by_domain(receipts, "fiscal")),
            "entropy": len(filter_by_domain(receipts, "entropy")),
            "loop": len(filter_by_domain(receipts, "loop"))
        }
    }
