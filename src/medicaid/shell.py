"""
Shell LLC Detection Module

Detects shell LLC proliferation pattern (Ali case: 41+ clinics from single operator).
"""

import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

from ..core import (
    emit_receipt,
    TENANT_ID,
    SHELL_MIN_CLUSTER,
    SHELL_BILLING_THRESHOLD,
    get_risk_level
)


# Shell detection constants
SHELL_REGISTRATION_WINDOW = 365  # 1 year


def extract_principals(provider_data: Dict) -> List[str]:
    """
    Extract registered agents, officers, owners from LLC data.

    Args:
        provider_data: Provider/LLC data dict

    Returns:
        List of principal identifiers
    """
    principals = []

    # Check various fields that might contain principal info
    principal_fields = [
        "registered_agent",
        "officers",
        "owners",
        "principals",
        "directors",
        "members",
        "managers"
    ]

    for field in principal_fields:
        value = provider_data.get(field)
        if value:
            if isinstance(value, list):
                principals.extend([str(v) for v in value])
            elif isinstance(value, str):
                principals.append(value)
            elif isinstance(value, dict):
                # Extract names from dict
                for k, v in value.items():
                    if isinstance(v, str):
                        principals.append(v)

    # Also check provider name for patterns
    provider_name = provider_data.get("provider_name", "")
    if provider_name:
        # Track provider name as potential identifier
        principals.append(f"name:{provider_name}")

    # Normalize and deduplicate
    normalized = []
    seen = set()
    for p in principals:
        p_lower = p.lower().strip()
        if p_lower and p_lower not in seen:
            seen.add(p_lower)
            normalized.append(p)

    return normalized


def build_ownership_graph(providers: List[Dict]) -> Dict[str, Any]:
    """
    Graph: LLCs as nodes, shared principals as edges.

    Args:
        providers: List of provider data dicts

    Returns:
        Graph dict with nodes and edges
    """
    nodes = []
    edges = []

    # Extract principals for each provider
    provider_principals: Dict[str, List[str]] = {}

    for provider in providers:
        provider_id = provider.get("provider_id")
        if not provider_id:
            continue

        principals = extract_principals(provider)
        provider_principals[provider_id] = principals

        nodes.append({
            "provider_id": provider_id,
            "provider_name": provider.get("provider_name"),
            "principals": principals,
            "registration_date": provider.get("registration_date"),
            "total_billed": provider.get("total_billed", 0)
        })

    # Build principal -> providers mapping
    principal_providers: Dict[str, Set[str]] = defaultdict(set)
    for provider_id, principals in provider_principals.items():
        for principal in principals:
            principal_providers[principal.lower()].add(provider_id)

    # Create edges for shared principals
    edge_weights: Dict[tuple, Dict] = {}

    for principal, provider_set in principal_providers.items():
        if len(provider_set) > 1:
            provider_list = list(provider_set)
            for i in range(len(provider_list)):
                for j in range(i + 1, len(provider_list)):
                    edge_key = tuple(sorted([provider_list[i], provider_list[j]]))
                    if edge_key not in edge_weights:
                        edge_weights[edge_key] = {
                            "source": edge_key[0],
                            "target": edge_key[1],
                            "shared_principals": [],
                            "weight": 0
                        }
                    edge_weights[edge_key]["shared_principals"].append(principal)
                    edge_weights[edge_key]["weight"] += 1

    edges = list(edge_weights.values())

    return {
        "nodes": nodes,
        "edges": edges,
        "n_providers": len(nodes),
        "n_edges": len(edges)
    }


def detect_shell_clusters(graph: Dict, min_shared: int = 2) -> List[Dict]:
    """
    Find LLC groups with >= min_shared common principals.

    Args:
        graph: Ownership graph
        min_shared: Minimum shared principals to form edge

    Returns:
        List of shell cluster dicts
    """
    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    if not nodes:
        return []

    # Filter edges by minimum shared principals
    strong_edges = [e for e in edges if e.get("weight", 0) >= min_shared]

    # Build adjacency list from strong edges
    adjacency: Dict[str, Set[str]] = defaultdict(set)
    for edge in strong_edges:
        adjacency[edge["source"]].add(edge["target"])
        adjacency[edge["target"]].add(edge["source"])

    # Find connected components
    visited: Set[str] = set()
    clusters = []

    node_ids = [n.get("provider_id") for n in nodes if n.get("provider_id")]

    for node_id in node_ids:
        if node_id in visited:
            continue
        if node_id not in adjacency:
            continue

        # BFS
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

        if len(component) >= SHELL_MIN_CLUSTER:
            # Find shared principals across cluster
            all_principals: Dict[str, int] = defaultdict(int)
            node_lookup = {n.get("provider_id"): n for n in nodes}

            total_billed = 0
            for provider_id in component:
                node_data = node_lookup.get(provider_id, {})
                total_billed += node_data.get("total_billed", 0)
                for p in node_data.get("principals", []):
                    all_principals[p.lower()] += 1

            # Find principals appearing in majority of cluster
            shared = [p for p, count in all_principals.items() if count >= len(component) * 0.5]

            clusters.append({
                "cluster_id": str(uuid.uuid4()),
                "providers": component,
                "n_entities": len(component),
                "shared_principals": shared[:10],  # Top 10
                "combined_billing": total_billed,
                "exceeds_threshold": total_billed >= SHELL_BILLING_THRESHOLD
            })

    return sorted(clusters, key=lambda c: c["combined_billing"], reverse=True)


def compute_registration_burst(
    provider_id: str,
    providers: List[Dict],
    window_days: int = SHELL_REGISTRATION_WINDOW
) -> int:
    """
    Count new LLCs registered by same principal in window.

    Args:
        provider_id: Starting provider
        providers: All provider data
        window_days: Time window in days

    Returns:
        Count of LLCs registered in window by same principals
    """
    # Find principals for target provider
    target_provider = None
    for p in providers:
        if p.get("provider_id") == provider_id:
            target_provider = p
            break

    if not target_provider:
        return 0

    target_principals = set(p.lower() for p in extract_principals(target_provider))
    target_date = target_provider.get("registration_date")

    if not target_date or not target_principals:
        return 0

    try:
        target_dt = datetime.fromisoformat(target_date.replace('Z', '+00:00'))
    except (ValueError, AttributeError):
        return 0

    # Find other LLCs by same principals within window
    window_start = target_dt - timedelta(days=window_days // 2)
    window_end = target_dt + timedelta(days=window_days // 2)

    burst_count = 0

    for provider in providers:
        if provider.get("provider_id") == provider_id:
            continue

        reg_date = provider.get("registration_date")
        if not reg_date:
            continue

        try:
            reg_dt = datetime.fromisoformat(reg_date.replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            continue

        if window_start <= reg_dt <= window_end:
            provider_principals = set(p.lower() for p in extract_principals(provider))
            if target_principals & provider_principals:
                burst_count += 1

    return burst_count


def flag_shell_network(cluster: Dict, billing_total: Optional[float] = None) -> Dict:
    """
    Flag cluster if combined billing > threshold.

    Args:
        cluster: Shell cluster dict
        billing_total: Override billing total (uses cluster value if not provided)

    Returns:
        Flagged cluster with risk assessment
    """
    total = billing_total if billing_total is not None else cluster.get("combined_billing", 0)

    # Calculate risk score
    risk_score = 0.0

    # Size factor
    n_entities = cluster.get("n_entities", 0)
    if n_entities >= 40:
        risk_score += 0.4
    elif n_entities >= 20:
        risk_score += 0.3
    elif n_entities >= 10:
        risk_score += 0.2
    elif n_entities >= SHELL_MIN_CLUSTER:
        risk_score += 0.1

    # Billing factor
    if total >= SHELL_BILLING_THRESHOLD * 10:  # $100M+
        risk_score += 0.4
    elif total >= SHELL_BILLING_THRESHOLD:  # $10M+
        risk_score += 0.3
    elif total >= SHELL_BILLING_THRESHOLD / 2:  # $5M+
        risk_score += 0.2
    elif total >= SHELL_BILLING_THRESHOLD / 10:  # $1M+
        risk_score += 0.1

    # Principal concentration factor
    shared_principals = cluster.get("shared_principals", [])
    if len(shared_principals) <= 2:
        risk_score += 0.2  # Very concentrated ownership

    risk_level = get_risk_level(risk_score)

    return {
        **cluster,
        "billing_total": total,
        "risk_score": risk_score,
        "risk_level": risk_level,
        "flagged": total >= SHELL_BILLING_THRESHOLD or n_entities >= SHELL_MIN_CLUSTER * 2
    }


def analyze_shell_networks(
    providers: List[Dict],
    tenant_id: str = TENANT_ID
) -> List[Dict]:
    """
    Full shell network analysis with receipt emission.

    Args:
        providers: Provider data list
        tenant_id: Tenant identifier

    Returns:
        List of shell detection receipts
    """
    graph = build_ownership_graph(providers)
    clusters = detect_shell_clusters(graph, min_shared=2)

    receipts = []

    for cluster in clusters:
        flagged = flag_shell_network(cluster)

        # Compute registration burst for first entity
        first_provider = cluster["providers"][0] if cluster["providers"] else None
        reg_burst = compute_registration_burst(first_provider, providers) if first_provider else 0

        receipt_data = {
            "cluster_id": cluster["cluster_id"],
            "n_entities": cluster["n_entities"],
            "shared_principals": cluster["shared_principals"],
            "combined_billing": cluster["combined_billing"],
            "registration_burst": reg_burst,
            "risk_level": flagged["risk_level"],
            "risk_score": flagged["risk_score"],
            "exceeds_billing_threshold": cluster["combined_billing"] >= SHELL_BILLING_THRESHOLD,
            "providers": cluster["providers"][:10]  # First 10 for brevity
        }

        receipt = emit_receipt("shell_detection", receipt_data, tenant_id)
        receipts.append(receipt)

    return receipts
