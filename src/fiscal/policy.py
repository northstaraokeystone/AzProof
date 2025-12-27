"""
Policy Impact Attribution Module

Tracks policy implementations and their fiscal impact.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, dual_hash, TENANT_ID, AZ_DEFICIT


def ingest_policy_change(
    policy: Dict[str, Any],
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Track policy implementations with effective dates.

    Args:
        policy: Policy dict with id, name, effective_date, etc.
        tenant_id: Tenant identifier

    Returns:
        Policy ingest receipt
    """
    # Validate required fields
    policy_id = policy.get("policy_id") or policy.get("id")
    if not policy_id:
        raise ValueError("policy_id is required")

    # Compute policy hash
    policy_hash = dual_hash(str(policy))

    receipt_data = {
        "policy_hash": policy_hash,
        "policy_id": policy_id,
        "policy_name": policy.get("name", policy.get("policy_name", "")),
        "effective_date": policy.get("effective_date"),
        "policy_type": policy.get("type", policy.get("policy_type", "unknown")),
        "projected_cost": policy.get("projected_cost", 0),
        "projected_revenue_impact": policy.get("projected_revenue_impact", 0),
        "category": policy.get("category", "general"),
        "status": policy.get("status", "active")
    }

    return emit_receipt("policy_ingest", receipt_data, tenant_id)


def compute_policy_cost(
    policy_id: str,
    fiscal_data: List[Dict],
    projected_cost: Optional[float] = None
) -> float:
    """
    Compute actual cost vs projected for a policy.

    Args:
        policy_id: Policy identifier
        fiscal_data: List of fiscal data points
        projected_cost: Optional projected cost to compare

    Returns:
        Actual cost (or estimated if data insufficient)
    """
    # Filter relevant fiscal data
    policy_costs = [
        d.get("amount", 0) for d in fiscal_data
        if d.get("policy_id") == policy_id or policy_id in str(d.get("metadata", {}))
    ]

    if policy_costs:
        actual_cost = sum(policy_costs)
    else:
        # Use projected if no actual data
        actual_cost = projected_cost or 0

    return actual_cost


def detect_budget_stress(
    deficit: float,
    threshold: float = AZ_DEFICIT
) -> bool:
    """
    Flag when deficit exceeds threshold.

    Args:
        deficit: Current deficit amount
        threshold: Threshold for stress flag

    Returns:
        True if deficit exceeds threshold
    """
    return abs(deficit) >= threshold


def track_policy_effectiveness(
    policy_id: str,
    fiscal_data: List[Dict],
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Track policy effectiveness over time.

    Args:
        policy_id: Policy to track
        fiscal_data: Fiscal data points
        tenant_id: Tenant identifier

    Returns:
        Policy tracking receipt
    """
    # Find policy-related data points
    policy_data = [
        d for d in fiscal_data
        if d.get("policy_id") == policy_id
    ]

    if not policy_data:
        return emit_receipt("policy_tracking", {
            "policy_id": policy_id,
            "data_points": 0,
            "status": "insufficient_data"
        }, tenant_id)

    # Calculate metrics
    costs = [d.get("cost", d.get("amount", 0)) for d in policy_data]
    periods = [d.get("period") for d in policy_data if d.get("period")]

    total_cost = sum(costs)
    avg_cost = total_cost / len(costs) if costs else 0

    # Compare to projections if available
    projected = policy_data[0].get("projected_cost", 0) if policy_data else 0
    cost_variance = total_cost - projected if projected else 0
    variance_pct = (cost_variance / projected * 100) if projected else 0

    receipt_data = {
        "policy_id": policy_id,
        "data_points": len(policy_data),
        "total_cost": total_cost,
        "avg_period_cost": avg_cost,
        "projected_cost": projected,
        "cost_variance": cost_variance,
        "variance_pct": variance_pct,
        "periods_tracked": len(periods),
        "status": "over_budget" if cost_variance > 0 else "on_track"
    }

    return emit_receipt("policy_tracking", receipt_data, tenant_id)


def analyze_policy_impact(
    policies: List[Dict],
    fiscal_data: List[Dict],
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Analyze impact of multiple policies.

    Args:
        policies: List of policy dicts
        fiscal_data: Fiscal data points
        tenant_id: Tenant identifier

    Returns:
        Policy analysis receipt
    """
    policy_impacts = []
    total_impact = 0

    for policy in policies:
        policy_id = policy.get("policy_id") or policy.get("id")
        if not policy_id:
            continue

        actual_cost = compute_policy_cost(policy_id, fiscal_data, policy.get("projected_cost"))
        projected = policy.get("projected_cost", 0)
        variance = actual_cost - projected

        impact = {
            "policy_id": policy_id,
            "policy_name": policy.get("name", policy.get("policy_name", "")),
            "actual_cost": actual_cost,
            "projected_cost": projected,
            "variance": variance,
            "variance_pct": (variance / projected * 100) if projected else 0
        }

        policy_impacts.append(impact)
        total_impact += actual_cost

    receipt_data = {
        "analysis_type": "policy",
        "period": datetime.now().strftime("%Y"),
        "current_value": total_impact,
        "prior_value": sum(p.get("projected_cost", 0) for p in policies),
        "policies_analyzed": len(policy_impacts),
        "attribution": {
            "policy_impacts": policy_impacts,
            "total_variance": sum(p["variance"] for p in policy_impacts)
        }
    }

    return emit_receipt("fiscal_analysis", receipt_data, tenant_id)
