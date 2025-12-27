"""
Deficit Causation Analysis Module

Analyzes budget deficit and attributes to policy factors.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..core import emit_receipt, TENANT_ID, AZ_DEFICIT, FLAT_TAX_COST


# Known deficit factors for Arizona
KNOWN_DEFICIT_FACTORS = {
    "flat_tax": {
        "description": "2.5% flat income tax",
        "estimated_contribution": 700_000_000,
        "confidence": 0.85
    },
    "esa_voucher": {
        "description": "Universal ESA voucher expansion",
        "estimated_contribution": 1_000_000_000,
        "confidence": 0.80
    },
    "medicaid_fraud": {
        "description": "Unrecovered Medicaid fraud losses",
        "estimated_contribution": 2_675_000_000,
        "confidence": 0.70
    },
    "sales_tax_dip": {
        "description": "Sales tax revenue fluctuations",
        "estimated_contribution": 200_000_000,
        "confidence": 0.60
    }
}


def compute_deficit(revenue: float, expenditure: float) -> float:
    """
    Simple deficit calculation.

    Args:
        revenue: Total revenue
        expenditure: Total expenditure

    Returns:
        Deficit (negative if revenue < expenditure)
    """
    return revenue - expenditure


def attribute_deficit(
    deficit: float,
    factors: List[str]
) -> Dict[str, Any]:
    """
    Causal attribution to policy factors.

    Args:
        deficit: Total deficit amount
        factors: List of factor identifiers to analyze

    Returns:
        Attribution dict with factor contributions
    """
    attributions = []
    total_explained = 0

    for factor in factors:
        if factor in KNOWN_DEFICIT_FACTORS:
            factor_data = KNOWN_DEFICIT_FACTORS[factor]
            contribution = factor_data["estimated_contribution"]

            # Calculate what portion of deficit this explains
            if abs(deficit) > 0:
                explanation_ratio = min(1.0, contribution / abs(deficit))
            else:
                explanation_ratio = 0

            attributions.append({
                "factor": factor,
                "description": factor_data["description"],
                "estimated_contribution": contribution,
                "explanation_ratio": explanation_ratio,
                "confidence": factor_data["confidence"]
            })

            total_explained += contribution
        else:
            attributions.append({
                "factor": factor,
                "description": f"Unknown factor: {factor}",
                "estimated_contribution": 0,
                "explanation_ratio": 0,
                "confidence": 0.1
            })

    # Calculate unexplained portion
    unexplained = abs(deficit) - total_explained
    unexplained_ratio = max(0, unexplained / abs(deficit)) if deficit != 0 else 0

    return {
        "deficit": deficit,
        "factors_analyzed": len(factors),
        "attributions": attributions,
        "total_explained": total_explained,
        "total_explained_ratio": min(1.0, total_explained / abs(deficit)) if deficit != 0 else 0,
        "unexplained": max(0, unexplained),
        "unexplained_ratio": unexplained_ratio
    }


def project_deficit(
    current: float,
    trend: List[float],
    years: int = 5
) -> Tuple[float, List[float]]:
    """
    Project future deficit based on trend.

    Args:
        current: Current deficit
        trend: Historical deficit values
        years: Years to project

    Returns:
        Tuple of (projected_value, yearly_projections)
    """
    if not trend:
        # No trend data - assume flat
        return current, [current] * years

    # Calculate average annual change
    if len(trend) >= 2:
        changes = []
        for i in range(1, len(trend)):
            changes.append(trend[i] - trend[i-1])
        avg_change = sum(changes) / len(changes)
    else:
        avg_change = 0

    # Project forward
    projections = []
    projected = current

    for _ in range(years):
        projected += avg_change
        projections.append(projected)

    return projections[-1] if projections else current, projections


def analyze_deficit(
    revenue: float,
    expenditure: float,
    factors: Optional[List[str]] = None,
    trend: Optional[List[float]] = None,
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Full deficit analysis with receipt emission.

    Args:
        revenue: Total revenue
        expenditure: Total expenditure
        factors: Optional list of factors to analyze
        trend: Optional historical trend
        tenant_id: Tenant identifier

    Returns:
        Fiscal analysis receipt
    """
    # Compute deficit
    deficit = compute_deficit(revenue, expenditure)

    # Attribute if factors provided
    if factors:
        attribution = attribute_deficit(deficit, factors)
    else:
        # Use all known factors
        attribution = attribute_deficit(deficit, list(KNOWN_DEFICIT_FACTORS.keys()))

    # Project if trend provided
    if trend:
        projected_5y, projections = project_deficit(deficit, trend, 5)
    else:
        projected_5y = deficit
        projections = []

    # Determine severity
    if abs(deficit) >= AZ_DEFICIT * 2:
        severity = "critical"
    elif abs(deficit) >= AZ_DEFICIT:
        severity = "high"
    elif abs(deficit) >= AZ_DEFICIT * 0.5:
        severity = "medium"
    else:
        severity = "low"

    receipt_data = {
        "analysis_type": "deficit",
        "period": datetime.now().strftime("%Y"),
        "current_value": deficit,
        "prior_value": trend[-1] if trend else 0,
        "revenue": revenue,
        "expenditure": expenditure,
        "deficit": deficit,
        "severity": severity,
        "attribution": attribution,
        "projection": {
            "years": 5,
            "projected_deficit": projected_5y,
            "yearly_projections": projections
        },
        "comparison": {
            "az_deficit_baseline": AZ_DEFICIT,
            "vs_baseline_pct": (deficit / AZ_DEFICIT * 100) if AZ_DEFICIT != 0 else 0
        }
    }

    return emit_receipt("fiscal_analysis", receipt_data, tenant_id)
