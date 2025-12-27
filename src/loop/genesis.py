"""
Helper Blueprint Creation Module

Synthesizes automation helpers from gap patterns.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, TENANT_ID


def synthesize_helper(pattern: Dict) -> Dict:
    """
    Create helper blueprint from gap pattern.

    Args:
        pattern: Gap pattern from identify_patterns()

    Returns:
        Helper blueprint dict
    """
    blueprint_id = str(uuid.uuid4())

    # Extract trigger from pattern
    problem_type = pattern.get("problem_type", "unknown")
    domain = pattern.get("domain", "unknown")

    # Create trigger condition
    trigger = f"receipt_type:{domain}_* AND anomaly_flag:true"

    # Create action from resolution steps
    resolution_steps = pattern.get("resolution_steps", [])
    if resolution_steps:
        action = f"execute_steps:{','.join(resolution_steps[:5])}"
    else:
        action = "alert:operator"

    # Calculate origin stats
    gap_count = pattern.get("count", 0)
    avg_resolution = pattern.get("avg_resolution_ms", 0)
    total_hours = (gap_count * avg_resolution) / (1000 * 60 * 60)  # ms to hours

    # Estimate risk score
    automation_likelihood = pattern.get("automation_likelihood", 0)
    if automation_likelihood > 0.8:
        risk_score = 0.2
    elif automation_likelihood > 0.5:
        risk_score = 0.4
    else:
        risk_score = 0.6

    blueprint = {
        "blueprint_id": blueprint_id,
        "origin": {
            "gap_count": gap_count,
            "total_hours_saved": total_hours
        },
        "trigger": trigger,
        "action": action,
        "parameters": {
            "problem_type": problem_type,
            "domain": domain,
            "resolution_steps": resolution_steps
        },
        "validation": {
            "backtested": 0,
            "success_rate": 0.0
        },
        "risk_score": risk_score,
        "requires_approval": risk_score >= 0.2,
        "status": "proposed",
        "created_at": datetime.now(timezone.utc).isoformat()
    }

    return blueprint


def validate_blueprint(
    blueprint: Dict,
    historical: List[Dict]
) -> Dict:
    """
    Backtest blueprint against historical gaps.

    Args:
        blueprint: Helper blueprint
        historical: Historical gap receipts

    Returns:
        Updated blueprint with validation results
    """
    # Find matching historical gaps
    problem_type = blueprint["parameters"].get("problem_type")
    domain = blueprint["parameters"].get("domain")

    matching = [
        g for g in historical
        if g.get("problem_type") == problem_type
        and g.get("domain") == domain
    ]

    if not matching:
        blueprint["validation"] = {
            "backtested": 0,
            "success_rate": 0.0,
            "status": "no_historical_data"
        }
        return blueprint

    # Simulate success rate based on automation confidence
    successes = sum(
        1 for g in matching
        if g.get("automation_confidence", 0) > 0.5
    )

    success_rate = successes / len(matching)

    blueprint["validation"] = {
        "backtested": len(matching),
        "success_rate": success_rate,
        "status": "validated" if success_rate > 0.7 else "needs_review"
    }

    # Update risk score based on validation
    if success_rate > 0.9:
        blueprint["risk_score"] = max(0.1, blueprint["risk_score"] - 0.2)
    elif success_rate < 0.5:
        blueprint["risk_score"] = min(0.9, blueprint["risk_score"] + 0.2)

    blueprint["requires_approval"] = blueprint["risk_score"] >= 0.2

    return blueprint


def estimate_savings(blueprint: Dict) -> float:
    """
    Estimate human hours saved if deployed.

    Args:
        blueprint: Helper blueprint

    Returns:
        Estimated hours saved per month
    """
    origin = blueprint.get("origin", {})
    gap_count = origin.get("gap_count", 0)
    total_hours = origin.get("total_hours_saved", 0)

    if gap_count == 0:
        return 0.0

    # Assume gaps continue at same rate
    hours_per_gap = total_hours / gap_count

    # Project monthly savings
    # Assume validation period was ~1 week, so multiply by 4
    monthly_gaps = gap_count * 4
    monthly_savings = monthly_gaps * hours_per_gap

    # Apply success rate discount
    success_rate = blueprint.get("validation", {}).get("success_rate", 0.5)
    adjusted_savings = monthly_savings * success_rate

    return adjusted_savings


def emit_blueprint(
    blueprint: Dict,
    tenant_id: str = TENANT_ID
) -> Dict:
    """
    Emit helper blueprint receipt.

    Args:
        blueprint: Blueprint to emit
        tenant_id: Tenant identifier

    Returns:
        Blueprint receipt
    """
    return emit_receipt("helper_blueprint", blueprint, tenant_id)


def create_helper_from_pattern(
    pattern: Dict,
    historical: List[Dict],
    tenant_id: str = TENANT_ID
) -> Dict:
    """
    Full helper creation workflow.

    Args:
        pattern: Gap pattern
        historical: Historical gaps for validation
        tenant_id: Tenant identifier

    Returns:
        Validated and emitted blueprint
    """
    # Synthesize
    blueprint = synthesize_helper(pattern)

    # Validate
    blueprint = validate_blueprint(blueprint, historical)

    # Estimate savings
    savings = estimate_savings(blueprint)
    blueprint["estimated_monthly_savings_hours"] = savings

    # Emit
    receipt = emit_blueprint(blueprint, tenant_id)

    return receipt
