"""
Network Entropy Calculation Module

Shannon entropy of network structures for fraud detection.
"""

import math
from collections import defaultdict
from typing import Any, Dict, List, Optional

from ..core import emit_receipt, TENANT_ID, NETWORK_ENTROPY_BASELINE


def network_entropy(graph: Dict) -> float:
    """
    Shannon entropy of network degree distribution.

    Args:
        graph: Graph dict with nodes and edges

    Returns:
        Entropy value in bits
    """
    edges = graph.get("edges", [])

    if not edges:
        return 0.0

    # Compute degree distribution
    degree_count: Dict[str, int] = defaultdict(int)

    for edge in edges:
        degree_count[edge.get("source", "")] += 1
        degree_count[edge.get("target", "")] += 1

    degrees = list(degree_count.values())

    if not degrees:
        return 0.0

    # Calculate Shannon entropy
    total = sum(degrees)
    if total == 0:
        return 0.0

    entropy = 0.0
    for d in degrees:
        if d > 0:
            p = d / total
            entropy -= p * math.log2(p)

    return entropy


def detect_entropy_anomaly(
    current: float,
    baseline: float = NETWORK_ENTROPY_BASELINE,
    sigma: float = 0.5
) -> bool:
    """
    Flag if entropy deviates > sigma from baseline.

    Args:
        current: Current entropy value
        baseline: Expected baseline entropy
        sigma: Deviation threshold

    Returns:
        True if anomaly detected
    """
    deviation = abs(current - baseline)
    return deviation > sigma


def temporal_network_entropy(
    graphs: List[Dict],
    window: int = 10
) -> List[float]:
    """
    Rolling entropy over time series of graphs.

    Args:
        graphs: List of graph dicts over time
        window: Window size for rolling calculation

    Returns:
        List of entropy values
    """
    if not graphs:
        return []

    # Calculate entropy for each graph
    entropies = [network_entropy(g) for g in graphs]

    if len(entropies) <= window:
        return entropies

    # Calculate rolling average
    rolling = []
    for i in range(len(entropies)):
        start = max(0, i - window + 1)
        window_vals = entropies[start:i + 1]
        avg = sum(window_vals) / len(window_vals)
        rolling.append(avg)

    return rolling


def compute_edge_entropy(graph: Dict) -> float:
    """
    Entropy of edge weight distribution.

    Args:
        graph: Graph dict with weighted edges

    Returns:
        Entropy value in bits
    """
    edges = graph.get("edges", [])

    if not edges:
        return 0.0

    weights = [e.get("weight", 1) for e in edges]

    if not weights:
        return 0.0

    total = sum(weights)
    if total == 0:
        return 0.0

    entropy = 0.0
    for w in weights:
        if w > 0:
            p = w / total
            entropy -= p * math.log2(p)

    return entropy


def compute_cluster_entropy(clusters: List[Dict]) -> float:
    """
    Entropy of cluster size distribution.

    Args:
        clusters: List of cluster dicts with size field

    Returns:
        Entropy value in bits
    """
    if not clusters:
        return 0.0

    sizes = [c.get("size", 1) for c in clusters]

    if not sizes:
        return 0.0

    total = sum(sizes)
    if total == 0:
        return 0.0

    entropy = 0.0
    for s in sizes:
        if s > 0:
            p = s / total
            entropy -= p * math.log2(p)

    return entropy


def analyze_network_entropy(
    graph: Dict,
    historical: Optional[List[Dict]] = None,
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Full network entropy analysis with receipt emission.

    Args:
        graph: Current graph to analyze
        historical: Optional historical graphs for trend
        tenant_id: Tenant identifier

    Returns:
        Entropy analysis receipt
    """
    # Calculate current entropy
    current_entropy = network_entropy(graph)
    edge_entropy = compute_edge_entropy(graph)

    # Check for anomaly
    anomaly = detect_entropy_anomaly(current_entropy, NETWORK_ENTROPY_BASELINE)

    # Historical trend if available
    trend = None
    if historical:
        trend_entropies = temporal_network_entropy(historical + [graph])
        trend = {
            "values": trend_entropies[-10:],  # Last 10
            "avg": sum(trend_entropies) / len(trend_entropies),
            "direction": "increasing" if trend_entropies[-1] > trend_entropies[0] else "decreasing"
        }

    receipt_data = {
        "analysis_type": "network",
        "domain": "provider_network",
        "entropy_value": current_entropy,
        "edge_entropy": edge_entropy,
        "baseline_value": NETWORK_ENTROPY_BASELINE,
        "anomaly_flag": anomaly,
        "deviation": abs(current_entropy - NETWORK_ENTROPY_BASELINE),
        "interpretation": _interpret_entropy(current_entropy),
        "graph_stats": {
            "n_nodes": len(graph.get("nodes", [])),
            "n_edges": len(graph.get("edges", []))
        },
        "trend": trend
    }

    return emit_receipt("entropy_analysis", receipt_data, tenant_id)


def _interpret_entropy(entropy: float) -> str:
    """Interpret entropy value."""
    if entropy < 1.0:
        return "very_low_potential_fraud_ring"
    elif entropy < 2.0:
        return "low_suspicious_concentration"
    elif entropy < 3.0:
        return "normal_healthy_distribution"
    elif entropy < 4.0:
        return "high_fragmented_network"
    else:
        return "very_high_random_distribution"
