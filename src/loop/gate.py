"""
HITL (Human In The Loop) Approval Management Module

Manages approval workflow for helper deployment.
"""

import uuid
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, load_receipts, TENANT_ID


# Risk thresholds
RISK_AUTO_APPROVE = 0.2
RISK_SINGLE_APPROVAL = 0.5
RISK_DOUBLE_APPROVAL = 0.8

# In-memory approval store (would be database in production)
_approvals: Dict[str, Dict] = {}


def calculate_risk(action: Dict) -> float:
    """
    Return 0-1 risk score for an action.

    Args:
        action: Action dict (blueprint or other action)

    Returns:
        Risk score 0-1
    """
    # Start with base risk
    risk = 0.3

    # Adjust based on action type
    action_type = action.get("action", "")

    if "delete" in action_type.lower():
        risk += 0.3
    if "modify" in action_type.lower():
        risk += 0.2
    if "alert" in action_type.lower():
        risk -= 0.1

    # Adjust based on validation
    validation = action.get("validation", {})
    success_rate = validation.get("success_rate", 0)

    if success_rate > 0.9:
        risk -= 0.2
    elif success_rate < 0.5:
        risk += 0.2

    # Adjust based on origin
    origin = action.get("origin", {})
    gap_count = origin.get("gap_count", 0)

    if gap_count > 100:
        risk -= 0.1  # Well-tested pattern
    elif gap_count < 5:
        risk += 0.1  # Not enough data

    # Use explicit risk_score if provided
    if "risk_score" in action:
        risk = (risk + action["risk_score"]) / 2

    return max(0.0, min(1.0, risk))


def request_approval(blueprint: Dict) -> str:
    """
    Submit blueprint for approval. Return approval_id.

    Args:
        blueprint: Helper blueprint to approve

    Returns:
        Approval ID
    """
    approval_id = str(uuid.uuid4())
    risk = calculate_risk(blueprint)

    # Determine required approvals
    if risk >= RISK_DOUBLE_APPROVAL:
        required_approvals = 2
        observation_period_hours = 24
    elif risk >= RISK_SINGLE_APPROVAL:
        required_approvals = 1
        observation_period_hours = 0
    else:
        required_approvals = 1
        observation_period_hours = 0

    approval_request = {
        "approval_id": approval_id,
        "blueprint_id": blueprint.get("blueprint_id"),
        "risk_score": risk,
        "required_approvals": required_approvals,
        "current_approvals": 0,
        "observation_period_hours": observation_period_hours,
        "status": "pending",
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "approvers": [],
        "blueprint": blueprint
    }

    _approvals[approval_id] = approval_request

    # Emit approval request receipt
    emit_receipt("approval_request", {
        "approval_id": approval_id,
        "blueprint_id": blueprint.get("blueprint_id"),
        "risk_score": risk,
        "required_approvals": required_approvals
    })

    return approval_id


def check_approval(approval_id: str) -> str:
    """
    Return approval status: "pending"|"approved"|"rejected".

    Args:
        approval_id: Approval ID to check

    Returns:
        Status string
    """
    if approval_id not in _approvals:
        return "not_found"

    return _approvals[approval_id].get("status", "pending")


def approve(approval_id: str, approver: str = "system") -> Dict:
    """
    Add approval to request.

    Args:
        approval_id: Approval ID
        approver: Approver identifier

    Returns:
        Updated approval state
    """
    if approval_id not in _approvals:
        return {"error": "not_found"}

    approval = _approvals[approval_id]

    if approval["status"] != "pending":
        return {"error": f"already_{approval['status']}"}

    # Add approval
    approval["approvers"].append({
        "approver": approver,
        "approved_at": datetime.now(timezone.utc).isoformat()
    })
    approval["current_approvals"] += 1

    # Check if fully approved
    if approval["current_approvals"] >= approval["required_approvals"]:
        approval["status"] = "approved"
        approval["approved_at"] = datetime.now(timezone.utc).isoformat()

        emit_receipt("approval_granted", {
            "approval_id": approval_id,
            "blueprint_id": approval["blueprint_id"],
            "approvers": [a["approver"] for a in approval["approvers"]]
        })

    return approval


def reject(approval_id: str, rejector: str = "system", reason: str = "") -> Dict:
    """
    Reject an approval request.

    Args:
        approval_id: Approval ID
        rejector: Rejector identifier
        reason: Rejection reason

    Returns:
        Updated approval state
    """
    if approval_id not in _approvals:
        return {"error": "not_found"}

    approval = _approvals[approval_id]

    if approval["status"] != "pending":
        return {"error": f"already_{approval['status']}"}

    approval["status"] = "rejected"
    approval["rejected_at"] = datetime.now(timezone.utc).isoformat()
    approval["rejected_by"] = rejector
    approval["rejection_reason"] = reason

    emit_receipt("approval_rejected", {
        "approval_id": approval_id,
        "blueprint_id": approval["blueprint_id"],
        "rejected_by": rejector,
        "reason": reason
    })

    return approval


def auto_approve(blueprint: Dict) -> bool:
    """
    Auto-approve if risk < 0.2.

    Args:
        blueprint: Blueprint to evaluate

    Returns:
        True if auto-approved
    """
    risk = calculate_risk(blueprint)

    if risk < RISK_AUTO_APPROVE:
        # Auto-approve
        approval_id = request_approval(blueprint)
        approve(approval_id, approver="auto_approve_system")
        return True

    return False


def get_pending_approvals() -> List[Dict]:
    """
    Get all pending approvals.

    Returns:
        List of pending approval requests
    """
    return [
        a for a in _approvals.values()
        if a.get("status") == "pending"
    ]


def clear_approvals() -> None:
    """Clear all approvals (for testing)."""
    global _approvals
    _approvals = {}
