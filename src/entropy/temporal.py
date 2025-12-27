"""
Time-Series Entropy Module

Entropy analysis of temporal patterns.
"""

import math
from collections import defaultdict
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, TENANT_ID


def time_series_entropy(values: List[float], bins: int = 10) -> float:
    """
    Entropy of binned time series values.

    Args:
        values: Time series values
        bins: Number of bins for discretization

    Returns:
        Entropy value in bits
    """
    if not values or len(values) < 2:
        return 0.0

    # Normalize to [0, 1] range
    min_val = min(values)
    max_val = max(values)

    if min_val == max_val:
        return 0.0  # All same value

    range_val = max_val - min_val

    # Bin the values
    bin_counts: Dict[int, int] = defaultdict(int)

    for v in values:
        bin_idx = int((v - min_val) / range_val * (bins - 1))
        bin_idx = min(bin_idx, bins - 1)
        bin_counts[bin_idx] += 1

    # Calculate entropy
    total = len(values)
    entropy = 0.0

    for count in bin_counts.values():
        if count > 0:
            p = count / total
            entropy -= p * math.log2(p)

    return entropy


def detect_regularity(values: List[float]) -> float:
    """
    Score 0-1 where high = suspicious regularity.

    Args:
        values: Time series values

    Returns:
        Regularity score (0 = random, 1 = perfectly regular)
    """
    if not values or len(values) < 3:
        return 0.0

    # Calculate coefficient of variation
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0

    variance = sum((v - mean) ** 2 for v in values) / len(values)
    std = math.sqrt(variance)
    cv = std / abs(mean)

    # Low CV = high regularity
    # Convert to regularity score (inverse relationship)
    # CV of 0 = regularity of 1
    # CV of 1+ = regularity near 0
    regularity = max(0, 1 - cv)

    # Also check for exact repeats
    unique_ratio = len(set(values)) / len(values)
    if unique_ratio < 0.5:
        # Many repeats - boost regularity
        regularity = min(1.0, regularity + 0.3)

    # Check for arithmetic patterns
    if len(values) >= 3:
        diffs = [values[i+1] - values[i] for i in range(len(values) - 1)]
        diff_variance = sum((d - sum(diffs)/len(diffs)) ** 2 for d in diffs) / len(diffs) if diffs else 0

        if diff_variance < 0.01:  # Very consistent differences
            regularity = min(1.0, regularity + 0.2)

    return regularity


def entropy_change_point(values: List[float], window: int = 20) -> List[int]:
    """
    Detect points where entropy regime changes.

    Args:
        values: Time series values
        window: Window size for entropy calculation

    Returns:
        List of change point indices
    """
    if len(values) < window * 2:
        return []

    change_points = []

    # Calculate rolling entropy
    entropies = []
    for i in range(len(values) - window + 1):
        window_vals = values[i:i + window]
        entropy = time_series_entropy(window_vals)
        entropies.append(entropy)

    if len(entropies) < 3:
        return []

    # Detect significant changes
    for i in range(1, len(entropies) - 1):
        # Look for local extrema
        prev_entropy = entropies[i - 1]
        curr_entropy = entropies[i]
        next_entropy = entropies[i + 1]

        # Check for significant jump
        avg_neighbors = (prev_entropy + next_entropy) / 2
        if avg_neighbors > 0:
            change_ratio = abs(curr_entropy - avg_neighbors) / avg_neighbors
            if change_ratio > 0.3:  # 30% change
                change_points.append(i + window // 2)

    return change_points


def analyze_temporal_entropy(
    values: List[float],
    labels: Optional[List[str]] = None,
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Full temporal entropy analysis with receipt emission.

    Args:
        values: Time series values
        labels: Optional labels for time points
        tenant_id: Tenant identifier

    Returns:
        Entropy analysis receipt
    """
    if not values:
        return emit_receipt("entropy_analysis", {
            "analysis_type": "temporal",
            "error": "no_values"
        }, tenant_id)

    # Calculate metrics
    entropy = time_series_entropy(values)
    regularity = detect_regularity(values)
    change_points = entropy_change_point(values)

    # Interpret
    if regularity > 0.8:
        interpretation = "highly_regular_suspicious"
        anomaly_flag = True
    elif regularity > 0.6:
        interpretation = "moderately_regular"
        anomaly_flag = False
    elif entropy < 1.0:
        interpretation = "low_entropy_concentrated"
        anomaly_flag = True
    elif entropy > 3.0:
        interpretation = "high_entropy_random"
        anomaly_flag = False
    else:
        interpretation = "normal_pattern"
        anomaly_flag = False

    receipt_data = {
        "analysis_type": "temporal",
        "domain": "time_series",
        "entropy_value": entropy,
        "baseline_value": 2.5,  # Expected entropy for normal patterns
        "regularity_score": regularity,
        "anomaly_flag": anomaly_flag,
        "change_points": change_points[:10],  # First 10
        "change_point_count": len(change_points),
        "interpretation": interpretation,
        "stats": {
            "n_values": len(values),
            "min": min(values),
            "max": max(values),
            "mean": sum(values) / len(values),
            "unique_count": len(set(values))
        }
    }

    return emit_receipt("entropy_analysis", receipt_data, tenant_id)


def detect_periodicity(values: List[float], max_period: int = 30) -> Optional[Dict]:
    """
    Detect periodic patterns in time series.

    Args:
        values: Time series values
        max_period: Maximum period to check

    Returns:
        Period info if detected, None otherwise
    """
    if len(values) < max_period * 2:
        return None

    best_period = None
    best_correlation = 0

    for period in range(2, min(max_period, len(values) // 2)):
        # Calculate autocorrelation at this lag
        n = len(values) - period
        if n < period:
            continue

        mean = sum(values) / len(values)
        variance = sum((v - mean) ** 2 for v in values) / len(values)

        if variance == 0:
            continue

        correlation = 0
        for i in range(n):
            correlation += (values[i] - mean) * (values[i + period] - mean)
        correlation /= (n * variance)

        if correlation > best_correlation and correlation > 0.5:
            best_correlation = correlation
            best_period = period

    if best_period:
        return {
            "period": best_period,
            "correlation": best_correlation,
            "confidence": min(1.0, best_correlation)
        }

    return None
