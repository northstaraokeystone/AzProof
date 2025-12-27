"""
Gap Collection Module

Collects and analyzes manual intervention gaps.
"""

from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, load_receipts, TENANT_ID


def harvest_gaps(days: int = 7) -> List[Dict]:
    """
    Collect gap_receipts from past N days.

    Args:
        days: Number of days to look back

    Returns:
        List of gap receipts
    """
    all_receipts = load_receipts()

    # Calculate cutoff
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)
    cutoff_str = cutoff.isoformat()

    gaps = []
    for receipt in all_receipts:
        if receipt.get("receipt_type") == "gap":
            ts = receipt.get("ts", "")
            if ts >= cutoff_str:
                gaps.append(receipt)

    return gaps


def rank_gaps(gaps: List[Dict]) -> List[Dict]:
    """
    Rank gaps by frequency x resolution_time.

    Args:
        gaps: List of gap receipts

    Returns:
        Ranked list of gaps
    """
    # Group by problem_type
    problem_groups: Dict[str, List[Dict]] = defaultdict(list)

    for gap in gaps:
        problem_type = gap.get("problem_type", "unknown")
        problem_groups[problem_type].append(gap)

    # Calculate ranking score for each problem type
    ranked = []

    for problem_type, group in problem_groups.items():
        frequency = len(group)

        # Average resolution time
        resolution_times = [
            g.get("time_to_resolve_ms", 0) for g in group
            if g.get("time_to_resolve_ms")
        ]
        avg_resolution = sum(resolution_times) / len(resolution_times) if resolution_times else 0

        # Score = frequency * avg_resolution (higher = more impactful to automate)
        score = frequency * (avg_resolution / 1000)  # Convert to seconds

        ranked.append({
            "problem_type": problem_type,
            "frequency": frequency,
            "avg_resolution_ms": avg_resolution,
            "score": score,
            "gaps": group,
            "could_automate": any(g.get("could_automate") for g in group),
            "automation_confidence": max(
                (g.get("automation_confidence", 0) for g in group),
                default=0
            )
        })

    # Sort by score descending
    return sorted(ranked, key=lambda x: x["score"], reverse=True)


def identify_patterns(gaps: List[Dict], min_count: int = 3) -> List[Dict]:
    """
    Find recurring gap patterns (>= min_count).

    Args:
        gaps: List of gap receipts
        min_count: Minimum occurrences to be considered a pattern

    Returns:
        List of identified patterns
    """
    # Group by problem_type and domain
    patterns: Dict[str, Dict[str, Any]] = {}

    for gap in gaps:
        problem_type = gap.get("problem_type", "unknown")
        domain = gap.get("domain", "unknown")
        key = f"{domain}:{problem_type}"

        if key not in patterns:
            patterns[key] = {
                "key": key,
                "problem_type": problem_type,
                "domain": domain,
                "count": 0,
                "gaps": [],
                "resolution_steps": set(),
                "could_automate_votes": 0
            }

        patterns[key]["count"] += 1
        patterns[key]["gaps"].append(gap)

        # Collect resolution steps
        for step in gap.get("resolution_steps", []):
            patterns[key]["resolution_steps"].add(step)

        if gap.get("could_automate"):
            patterns[key]["could_automate_votes"] += 1

    # Filter by min_count and convert sets
    result = []
    for pattern in patterns.values():
        if pattern["count"] >= min_count:
            pattern["resolution_steps"] = list(pattern["resolution_steps"])
            pattern["automation_likelihood"] = (
                pattern["could_automate_votes"] / pattern["count"]
            )
            result.append(pattern)

    return sorted(result, key=lambda x: x["count"], reverse=True)


def emit_gap(
    problem_type: str,
    domain: str,
    time_to_resolve_ms: int,
    resolution_steps: List[str],
    could_automate: bool = False,
    automation_confidence: float = 0.0,
    tenant_id: str = TENANT_ID
) -> Dict:
    """
    Emit a gap receipt for a manual intervention.

    Args:
        problem_type: Type of problem encountered
        domain: Domain (medicaid, voucher, fiscal)
        time_to_resolve_ms: Time taken to resolve
        resolution_steps: Steps taken to resolve
        could_automate: Whether this could be automated
        automation_confidence: Confidence in automation potential
        tenant_id: Tenant identifier

    Returns:
        Gap receipt
    """
    receipt_data = {
        "problem_type": problem_type,
        "domain": domain,
        "time_to_resolve_ms": time_to_resolve_ms,
        "resolution_steps": resolution_steps,
        "could_automate": could_automate,
        "automation_confidence": automation_confidence
    }

    return emit_receipt("gap", receipt_data, tenant_id)


def analyze_gap_trends(days: int = 30) -> Dict[str, Any]:
    """
    Analyze gap trends over time.

    Args:
        days: Analysis period

    Returns:
        Trend analysis dict
    """
    gaps = harvest_gaps(days=days)

    if not gaps:
        return {"status": "no_gaps", "period_days": days}

    # Group by day
    daily_counts: Dict[str, int] = defaultdict(int)

    for gap in gaps:
        ts = gap.get("ts", "")
        if ts:
            day = ts[:10]  # YYYY-MM-DD
            daily_counts[day] += 1

    days_list = sorted(daily_counts.keys())

    # Calculate trend
    counts = [daily_counts[d] for d in days_list]
    if len(counts) >= 2:
        first_half = sum(counts[:len(counts)//2])
        second_half = sum(counts[len(counts)//2:])
        trend = "increasing" if second_half > first_half else "decreasing"
    else:
        trend = "stable"

    return {
        "period_days": days,
        "total_gaps": len(gaps),
        "avg_daily": len(gaps) / max(1, len(days_list)),
        "trend": trend,
        "by_day": dict(daily_counts),
        "patterns": identify_patterns(gaps)
    }
