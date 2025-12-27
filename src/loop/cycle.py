"""
Main SENSE->ACTUATE Cycle Module

Orchestrates the full meta-loop cycle.
"""

import signal
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, TENANT_ID
from .sense import sense_receipts, summarize_activity
from .harvest import harvest_gaps, rank_gaps, identify_patterns
from .genesis import synthesize_helper, validate_blueprint, estimate_savings
from .gate import calculate_risk, request_approval, check_approval, auto_approve
from .effectiveness import (
    register_helper,
    record_execution,
    measure_effectiveness,
    get_helper_summary
)


# Global loop state
_loop_running = False
_cycle_count = 0


def run_cycle(
    sense_minutes: int = 60,
    harvest_days: int = 7,
    min_pattern_count: int = 3,
    tenant_id: str = TENANT_ID
) -> Dict:
    """
    Execute full SENSE->EMIT cycle. Return cycle metrics.

    Args:
        sense_minutes: Minutes to look back for sensing
        harvest_days: Days to look back for gap harvesting
        min_pattern_count: Minimum occurrences for pattern detection
        tenant_id: Tenant identifier

    Returns:
        Cycle metrics dict
    """
    global _cycle_count
    _cycle_count += 1

    cycle_start = time.time()

    # === SENSE ===
    recent_receipts = sense_receipts(since_minutes=sense_minutes)
    activity = summarize_activity(sense_minutes)

    # === ANALYZE ===
    # Count anomalies in recent receipts
    anomalies = [
        r for r in recent_receipts
        if r.get("anomaly_flag") or r.get("risk_level") in ["high", "critical"]
    ]

    # === HARVEST ===
    gaps = harvest_gaps(days=harvest_days)
    ranked_gaps = rank_gaps(gaps)
    patterns = identify_patterns(gaps, min_count=min_pattern_count)

    # === HYPOTHESIZE ===
    helpers_proposed = 0
    new_blueprints = []

    for pattern in patterns[:3]:  # Top 3 patterns
        if pattern.get("automation_likelihood", 0) > 0.5:
            blueprint = synthesize_helper(pattern)
            blueprint = validate_blueprint(blueprint, gaps)

            if blueprint["validation"].get("success_rate", 0) > 0.7:
                new_blueprints.append(blueprint)
                helpers_proposed += 1

    # === GATE ===
    helpers_approved = 0
    helpers_deployed = 0

    for blueprint in new_blueprints:
        # Try auto-approve first
        if auto_approve(blueprint):
            helpers_approved += 1
            helpers_deployed += 1
            register_helper(blueprint)
        else:
            # Request manual approval
            approval_id = request_approval(blueprint)
            status = check_approval(approval_id)
            if status == "approved":
                helpers_approved += 1
                helpers_deployed += 1
                register_helper(blueprint)

    # === ACTUATE ===
    # Execute active helpers (simulation - would execute real helpers in production)
    helper_summary = get_helper_summary()

    # Calculate entropy delta
    entropy_values = [
        r.get("entropy_value", 0) for r in recent_receipts
        if r.get("receipt_type") == "entropy_analysis"
    ]
    if len(entropy_values) >= 2:
        entropy_delta = entropy_values[-1] - entropy_values[0]
    else:
        entropy_delta = 0

    cycle_time = time.time() - cycle_start

    # === EMIT ===
    receipt_data = {
        "cycle_id": _cycle_count,
        "receipts_processed": len(recent_receipts),
        "anomalies_detected": len(anomalies),
        "gaps_harvested": len(gaps),
        "patterns_identified": len(patterns),
        "helpers_proposed": helpers_proposed,
        "helpers_approved": helpers_approved,
        "helpers_deployed": helpers_deployed,
        "entropy_delta": entropy_delta,
        "cycle_time_ms": int(cycle_time * 1000),
        "activity_summary": activity,
        "helper_summary": helper_summary
    }

    receipt = emit_receipt("loop_cycle", receipt_data, tenant_id)

    return receipt


def start_loop(
    interval_sec: int = 60,
    max_cycles: Optional[int] = None,
    tenant_id: str = TENANT_ID
) -> None:
    """
    Start continuous loop with interval.

    Args:
        interval_sec: Seconds between cycles
        max_cycles: Maximum cycles to run (None = infinite)
        tenant_id: Tenant identifier
    """
    global _loop_running
    _loop_running = True

    def signal_handler(sig, frame):
        global _loop_running
        _loop_running = False

    # Handle graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    cycle_count = 0

    while _loop_running:
        try:
            run_cycle(tenant_id=tenant_id)
            cycle_count += 1

            if max_cycles and cycle_count >= max_cycles:
                break

            time.sleep(interval_sec)

        except Exception as e:
            # Log error and continue
            emit_receipt("loop_error", {
                "cycle_id": _cycle_count,
                "error": str(e),
                "error_type": type(e).__name__
            }, tenant_id)

            # Brief pause before retry
            time.sleep(5)


def stop_loop() -> None:
    """Graceful shutdown of the loop."""
    global _loop_running
    _loop_running = False


def get_cycle_count() -> int:
    """Get current cycle count."""
    return _cycle_count


def reset_cycle_count() -> None:
    """Reset cycle count (for testing)."""
    global _cycle_count
    _cycle_count = 0
