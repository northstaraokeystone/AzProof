"""
Provider Network Graph Analysis Module

Build and analyze provider network graph for fraud ring detection.
"""

import math
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from ..core import emit_receipt, TENANT_ID, NETWORK_ENTROPY_BASELINE


def build_provider_graph(receipts: List[Dict]) -> Dict[str, Any]:
    """
    Construct graph: providers as nodes, shared patients/referrals as edges.

    Args:
        receipts: List of medicaid_ingest receipts

    Returns:
        Graph dict with nodes and edges
    """
    # Filter to medicaid ingest receipts
    claims = [r for r in receipts if r.get("receipt_type") == "medicaid_ingest"]

    # Build patient -> provider mapping
    patient_providers: Dict[str, Set[str]] = defaultdict(set)
    providers: Set[str] = set()
    provider_data: Dict[str, Dict] = {}

    for claim in claims:
        provider_id = claim.get("provider_id")
        patient_id = claim.get("patient_id")

        if provider_id:
            providers.add(provider_id)
            provider_data[provider_id] = {
                "provider_id": provider_id,
                "provider_name": claim.get("provider_name"),
                "claim_count": provider_data.get(provider_id, {}).get("claim_count", 0) + 1,
                "total_billed": provider_data.get(provider_id, {}).get("total_billed", 0) + (claim.get("billed_amount") or 0)
            }

        if patient_id and provider_id:
            patient_providers[patient_id].add(provider_id)

    # Build edges: providers connected by shared patients
    edges: List[Dict] = []
    edge_weights: Dict[tuple, int] = defaultdict(int)

    for patient_id, provider_set in patient_providers.items():
        provider_list = list(provider_set)
        for i in range(len(provider_list)):
            for j in range(i + 1, len(provider_list)):
                edge_key = tuple(sorted([provider_list[i], provider_list[j]]))
                edge_weights[edge_key] += 1

    for (p1, p2), weight in edge_weights.items():
        edges.append({
            "source": p1,
            "target": p2,
            "weight": weight,
            "type": "shared_patient"
        })

    # Build nodes list
    nodes = [provider_data.get(p, {"provider_id": p}) for p in providers]

    return {
        "nodes": nodes,
        "edges": edges,
        "n_providers": len(providers),
        "n_edges": len(edges)
    }


def detect_clusters(graph: Dict, min_size: int = 3) -> List[Dict]:
    """
    Find connected components >= min_size. Flag unusual clustering.

    Args:
        graph: Graph dict from build_provider_graph
        min_size: Minimum cluster size to return

    Returns:
        List of cluster dicts
    """
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    if not nodes:
        return []

    # Build adjacency list
    adjacency: Dict[str, Set[str]] = defaultdict(set)
    for edge in edges:
        adjacency[edge["source"]].add(edge["target"])
        adjacency[edge["target"]].add(edge["source"])

    # Find connected components using BFS
    visited: Set[str] = set()
    clusters = []

    for node in nodes:
        node_id = node.get("provider_id") or node
        if node_id in visited:
            continue

        # BFS to find component
        component = []
        queue = [node_id]
        visited.add(node_id)

        while queue:
            current = queue.pop(0)
            component.append(current)

            for neighbor in adjacency.get(current, []):
                if neighbor not in visited:
                    visited.add(neighbor)
                    queue.append(neighbor)

        if len(component) >= min_size:
            # Calculate cluster metrics
            cluster_edges = [
                e for e in edges
                if e["source"] in component and e["target"] in component
            ]
            total_weight = sum(e.get("weight", 1) for e in cluster_edges)

            clusters.append({
                "cluster_id": f"cluster_{len(clusters) + 1}",
                "providers": component,
                "size": len(component),
                "edge_count": len(cluster_edges),
                "total_weight": total_weight,
                "density": len(cluster_edges) / max(1, len(component) * (len(component) - 1) / 2)
            })

    return clusters


def compute_network_entropy(graph: Dict) -> float:
    """
    Shannon entropy of edge distribution. Low entropy = fraud ring.

    Args:
        graph: Graph dict from build_provider_graph

    Returns:
        Entropy value (bits)
    """
    edges = graph.get("edges", [])

    if not edges:
        return 0.0

    # Compute degree distribution
    degree_count: Dict[str, int] = defaultdict(int)
    for edge in edges:
        degree_count[edge["source"]] += 1
        degree_count[edge["target"]] += 1

    degrees = list(degree_count.values())
    if not degrees:
        return 0.0

    total = sum(degrees)
    if total == 0:
        return 0.0

    # Shannon entropy
    entropy = 0.0
    for d in degrees:
        if d > 0:
            p = d / total
            entropy -= p * math.log2(p)

    return entropy


def flag_hub_providers(graph: Dict, threshold: float = 2.0) -> List[Dict]:
    """
    Providers with degree > threshold * mean. Return flagged list.

    Args:
        graph: Graph dict
        threshold: Multiplier for mean degree

    Returns:
        List of flagged provider dicts
    """
    edges = graph.get("edges", [])
    nodes = graph.get("nodes", [])

    if not edges or not nodes:
        return []

    # Compute degrees
    degree_count: Dict[str, int] = defaultdict(int)
    for edge in edges:
        degree_count[edge["source"]] += 1
        degree_count[edge["target"]] += 1

    if not degree_count:
        return []

    # Calculate mean and threshold
    mean_degree = sum(degree_count.values()) / len(degree_count)
    degree_threshold = threshold * mean_degree

    # Flag providers exceeding threshold
    flagged = []
    for node in nodes:
        provider_id = node.get("provider_id") if isinstance(node, dict) else node
        degree = degree_count.get(provider_id, 0)

        if degree > degree_threshold:
            flagged.append({
                "provider_id": provider_id,
                "degree": degree,
                "mean_degree": mean_degree,
                "deviation": degree / mean_degree if mean_degree > 0 else 0
            })

    return sorted(flagged, key=lambda x: x["degree"], reverse=True)


def trace_referral_chains(graph: Dict, provider_id: str, depth: int = 3) -> Dict:
    """
    BFS to trace referral network to specified depth.

    Args:
        graph: Graph dict
        provider_id: Starting provider ID
        depth: Maximum depth to trace

    Returns:
        Dict with chain structure and metrics
    """
    edges = graph.get("edges", [])

    # Build adjacency list
    adjacency: Dict[str, List[Dict]] = defaultdict(list)
    for edge in edges:
        adjacency[edge["source"]].append({
            "target": edge["target"],
            "weight": edge.get("weight", 1)
        })
        adjacency[edge["target"]].append({
            "target": edge["source"],
            "weight": edge.get("weight", 1)
        })

    # BFS with depth tracking
    visited: Set[str] = {provider_id}
    layers: List[List[str]] = [[provider_id]]
    all_edges = []

    for d in range(depth):
        current_layer = layers[-1]
        next_layer = []

        for node in current_layer:
            for neighbor_info in adjacency.get(node, []):
                neighbor = neighbor_info["target"]
                if neighbor not in visited:
                    visited.add(neighbor)
                    next_layer.append(neighbor)
                    all_edges.append({
                        "from": node,
                        "to": neighbor,
                        "depth": d + 1,
                        "weight": neighbor_info["weight"]
                    })

        if next_layer:
            layers.append(next_layer)
        else:
            break

    return {
        "origin": provider_id,
        "depth_reached": len(layers) - 1,
        "total_providers": len(visited),
        "layers": layers,
        "edges": all_edges
    }


def analyze_network(receipts: List[Dict], tenant_id: str = TENANT_ID) -> Dict:
    """
    Full network analysis with receipt emission.

    Args:
        receipts: List of medicaid_ingest receipts
        tenant_id: Tenant identifier

    Returns:
        Network analysis receipt
    """
    graph = build_provider_graph(receipts)
    clusters = detect_clusters(graph, min_size=3)
    entropy = compute_network_entropy(graph)
    hubs = flag_hub_providers(graph, threshold=2.0)

    receipt_data = {
        "n_providers": graph.get("n_providers", 0),
        "n_edges": graph.get("n_edges", 0),
        "n_clusters": len(clusters),
        "network_entropy": entropy,
        "entropy_baseline": NETWORK_ENTROPY_BASELINE,
        "entropy_anomaly": entropy < NETWORK_ENTROPY_BASELINE - 0.5,
        "flagged_hubs": [h["provider_id"] for h in hubs[:10]],
        "largest_cluster_size": max([c["size"] for c in clusters], default=0)
    }

    return emit_receipt("network_analysis", receipt_data, tenant_id)
