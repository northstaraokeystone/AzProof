"""
Revenue Flow Tracking Module

Tracks state revenue by source and analyzes changes.
"""

from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, dual_hash, TENANT_ID


# Revenue source categories
REVENUE_SOURCES = [
    "income_tax",
    "sales_tax",
    "corporate_tax",
    "property_tax",
    "federal_transfers",
    "fees_fines",
    "lottery",
    "other"
]


def ingest_revenue_data(
    data: Dict[str, Any],
    source: str,
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Ingest revenue figures by source (income, sales, corporate).

    Args:
        data: Revenue data dict with period, amount, etc.
        source: Revenue source category
        tenant_id: Tenant identifier

    Returns:
        Revenue ingest receipt
    """
    # Validate source
    if source not in REVENUE_SOURCES:
        source = "other"

    # Extract key fields
    period = data.get("period", "")
    amount = data.get("amount", 0)
    prior_amount = data.get("prior_amount")

    # Compute hash
    data_hash = dual_hash(str(data))

    receipt_data = {
        "data_hash": data_hash,
        "source": source,
        "period": period,
        "amount": amount,
        "prior_amount": prior_amount,
        "metadata": data.get("metadata", {})
    }

    return emit_receipt("revenue_ingest", receipt_data, tenant_id)


def compute_yoy_change(
    current: Dict[str, float],
    prior: Dict[str, float]
) -> Dict[str, Any]:
    """
    Year-over-year change by category.

    Args:
        current: Current period amounts by category
        prior: Prior period amounts by category

    Returns:
        Dict with absolute and percentage changes
    """
    changes = {}

    all_categories = set(current.keys()) | set(prior.keys())

    total_current = 0
    total_prior = 0

    for category in all_categories:
        curr_val = current.get(category, 0)
        prior_val = prior.get(category, 0)

        total_current += curr_val
        total_prior += prior_val

        absolute_change = curr_val - prior_val
        pct_change = (absolute_change / prior_val * 100) if prior_val != 0 else 0

        changes[category] = {
            "current": curr_val,
            "prior": prior_val,
            "absolute_change": absolute_change,
            "pct_change": pct_change
        }

    # Total change
    total_absolute = total_current - total_prior
    total_pct = (total_absolute / total_prior * 100) if total_prior != 0 else 0

    return {
        "by_category": changes,
        "total": {
            "current": total_current,
            "prior": total_prior,
            "absolute_change": total_absolute,
            "pct_change": total_pct
        }
    }


def attribute_policy_impact(
    revenue_change: float,
    policy: str,
    policy_data: Optional[Dict] = None
) -> Dict[str, Any]:
    """
    Estimate policy contribution to revenue change.

    Args:
        revenue_change: Total revenue change amount
        policy: Policy identifier
        policy_data: Optional policy details

    Returns:
        Attribution dict with estimated impact
    """
    # Known Arizona policy impacts (from research)
    known_impacts = {
        "flat_tax_2.5": {
            "description": "Flat 2.5% income tax implementation",
            "estimated_impact": -700_000_000,  # $700M revenue drop
            "confidence": 0.85,
            "source": "arizona_budget_analysis"
        },
        "esa_universal": {
            "description": "Universal ESA voucher expansion",
            "estimated_impact": -1_000_000_000,  # ~$1B/year
            "confidence": 0.80,
            "source": "esa_expenditure_tracking"
        },
        "medicaid_fraud_loss": {
            "description": "Medicaid fraud unrecovered losses",
            "estimated_impact": -2_675_000_000,  # $2.8B - $125M recovered
            "confidence": 0.70,
            "source": "ahcccs_fraud_reporting"
        }
    }

    if policy in known_impacts:
        impact_data = known_impacts[policy]
        estimated = impact_data["estimated_impact"]

        # Calculate what portion of the change this policy explains
        if revenue_change != 0:
            explanation_ratio = abs(estimated / revenue_change)
        else:
            explanation_ratio = 0

        return {
            "policy": policy,
            "description": impact_data["description"],
            "estimated_impact": estimated,
            "revenue_change": revenue_change,
            "explanation_ratio": min(1.0, explanation_ratio),
            "confidence": impact_data["confidence"],
            "source": impact_data["source"]
        }
    else:
        # Unknown policy - use provided data or default
        if policy_data:
            estimated = policy_data.get("estimated_impact", 0)
            confidence = policy_data.get("confidence", 0.5)
        else:
            estimated = 0
            confidence = 0.1

        return {
            "policy": policy,
            "description": policy_data.get("description", "Unknown policy") if policy_data else "Unknown policy",
            "estimated_impact": estimated,
            "revenue_change": revenue_change,
            "explanation_ratio": abs(estimated / revenue_change) if revenue_change != 0 else 0,
            "confidence": confidence,
            "source": "user_provided"
        }


def analyze_revenue(
    current_data: Dict[str, float],
    prior_data: Dict[str, float],
    policies: Optional[List[str]] = None,
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Full revenue analysis with receipt emission.

    Args:
        current_data: Current period revenue by source
        prior_data: Prior period revenue by source
        policies: List of policies to attribute
        tenant_id: Tenant identifier

    Returns:
        Fiscal analysis receipt
    """
    # Compute YoY changes
    yoy = compute_yoy_change(current_data, prior_data)

    # Attribute policies if provided
    attributions = []
    total_change = yoy["total"]["absolute_change"]

    if policies:
        for policy in policies:
            attribution = attribute_policy_impact(total_change, policy)
            attributions.append(attribution)

    receipt_data = {
        "analysis_type": "revenue",
        "period": datetime.now().strftime("%Y"),
        "current_value": yoy["total"]["current"],
        "prior_value": yoy["total"]["prior"],
        "yoy_change": yoy["total"]["absolute_change"],
        "yoy_pct": yoy["total"]["pct_change"],
        "attribution": {
            "policies_analyzed": len(attributions),
            "attributions": attributions,
            "explained_ratio": sum(a.get("explanation_ratio", 0) for a in attributions)
        },
        "by_source": yoy["by_category"]
    }

    return emit_receipt("fiscal_analysis", receipt_data, tenant_id)
