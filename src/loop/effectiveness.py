"""
Helper Effectiveness Measurement Module

Tracks and measures helper performance.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, load_receipts, TENANT_ID


# In-memory helper tracking (would be database in production)
_helpers: Dict[str, Dict] = {}


def register_helper(blueprint: Dict) -> str:
    """
    Register a helper for tracking.

    Args:
        blueprint: Deployed helper blueprint

    Returns:
        Helper ID
    """
    helper_id = blueprint.get("blueprint_id", "")

    _helpers[helper_id] = {
        "helper_id": helper_id,
        "blueprint": blueprint,
        "deployed_at": datetime.now(timezone.utc).isoformat(),
        "status": "active",
        "executions": 0,
        "successes": 0,
        "failures": 0,
        "total_time_saved_ms": 0,
        "entropy_before": [],
        "entropy_after": []
    }

    return helper_id


def record_execution(
    helper_id: str,
    success: bool,
    time_saved_ms: int = 0,
    entropy_before: Optional[float] = None,
    entropy_after: Optional[float] = None
) -> None:
    """
    Record a helper execution.

    Args:
        helper_id: Helper ID
        success: Whether execution succeeded
        time_saved_ms: Time saved in milliseconds
        entropy_before: Entropy before execution
        entropy_after: Entropy after execution
    """
    if helper_id not in _helpers:
        return

    helper = _helpers[helper_id]
    helper["executions"] += 1

    if success:
        helper["successes"] += 1
        helper["total_time_saved_ms"] += time_saved_ms
    else:
        helper["failures"] += 1

    if entropy_before is not None:
        helper["entropy_before"].append(entropy_before)
    if entropy_after is not None:
        helper["entropy_after"].append(entropy_after)


def measure_effectiveness(helper_id: str, window: str = "7d") -> float:
    """
    Entropy reduction per action.

    Args:
        helper_id: Helper ID to measure
        window: Time window (e.g., "7d", "30d")

    Returns:
        Effectiveness score (0-1)
    """
    if helper_id not in _helpers:
        return 0.0

    helper = _helpers[helper_id]

    # Calculate success rate
    executions = helper.get("executions", 0)
    if executions == 0:
        return 0.0

    success_rate = helper.get("successes", 0) / executions

    # Calculate entropy reduction
    entropy_before = helper.get("entropy_before", [])
    entropy_after = helper.get("entropy_after", [])

    if entropy_before and entropy_after:
        avg_before = sum(entropy_before) / len(entropy_before)
        avg_after = sum(entropy_after) / len(entropy_after)

        if avg_before > 0:
            entropy_reduction = (avg_before - avg_after) / avg_before
        else:
            entropy_reduction = 0
    else:
        entropy_reduction = 0

    # Combined effectiveness score
    effectiveness = (success_rate * 0.6) + (max(0, entropy_reduction) * 0.4)

    return effectiveness


def track_helper(helper_id: str) -> Dict:
    """
    Return performance metrics for a helper.

    Args:
        helper_id: Helper ID

    Returns:
        Performance metrics dict
    """
    if helper_id not in _helpers:
        return {"error": "not_found"}

    helper = _helpers[helper_id]

    executions = helper.get("executions", 0)
    successes = helper.get("successes", 0)
    failures = helper.get("failures", 0)

    return {
        "helper_id": helper_id,
        "status": helper.get("status", "unknown"),
        "deployed_at": helper.get("deployed_at"),
        "executions": executions,
        "successes": successes,
        "failures": failures,
        "success_rate": successes / executions if executions > 0 else 0,
        "total_time_saved_ms": helper.get("total_time_saved_ms", 0),
        "total_time_saved_hours": helper.get("total_time_saved_ms", 0) / (1000 * 60 * 60),
        "effectiveness": measure_effectiveness(helper_id)
    }


def retire_helper(helper_id: str, reason: str) -> Dict:
    """
    Mark helper as retired.

    Args:
        helper_id: Helper ID
        reason: Retirement reason

    Returns:
        Updated helper state
    """
    if helper_id not in _helpers:
        return {"error": "not_found"}

    helper = _helpers[helper_id]
    helper["status"] = "retired"
    helper["retired_at"] = datetime.now(timezone.utc).isoformat()
    helper["retirement_reason"] = reason

    # Emit retirement receipt
    emit_receipt("helper_retired", {
        "helper_id": helper_id,
        "reason": reason,
        "final_metrics": track_helper(helper_id)
    })

    return helper


def get_active_helpers() -> List[Dict]:
    """
    Get all active helpers.

    Returns:
        List of active helper states
    """
    return [
        h for h in _helpers.values()
        if h.get("status") == "active"
    ]


def get_helper_summary() -> Dict[str, Any]:
    """
    Get summary of all helpers.

    Returns:
        Summary dict
    """
    active = [h for h in _helpers.values() if h.get("status") == "active"]
    retired = [h for h in _helpers.values() if h.get("status") == "retired"]

    total_executions = sum(h.get("executions", 0) for h in _helpers.values())
    total_successes = sum(h.get("successes", 0) for h in _helpers.values())
    total_time_saved = sum(h.get("total_time_saved_ms", 0) for h in _helpers.values())

    return {
        "total_helpers": len(_helpers),
        "active": len(active),
        "retired": len(retired),
        "total_executions": total_executions,
        "total_successes": total_successes,
        "overall_success_rate": total_successes / total_executions if total_executions > 0 else 0,
        "total_time_saved_hours": total_time_saved / (1000 * 60 * 60)
    }


def clear_helpers() -> None:
    """Clear all helpers (for testing)."""
    global _helpers
    _helpers = {}
