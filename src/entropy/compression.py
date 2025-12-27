"""
Compression-Based Fraud Detection Module

Core insight: Legitimate operations follow patterns (compressible).
Fraud must evade patterns (incompressible). Compression ratio = fraud indicator.
"""

import gzip
import json
from typing import Any, Dict, List, Optional, Tuple

from ..core import (
    emit_receipt,
    TENANT_ID,
    COMPRESSION_BASELINE_MEDICAID,
    COMPRESSION_BASELINE_VOUCHER,
    COMPRESSION_FRAUD_THRESHOLD
)


def compress_records(records: List[Dict]) -> Tuple[bytes, float]:
    """
    Compress records and return compressed data and ratio.

    Args:
        records: List of record dicts to compress

    Returns:
        Tuple of (compressed_bytes, compression_ratio)
    """
    if not records:
        return b"", 1.0

    # Serialize to JSON
    original = json.dumps(records, sort_keys=True, default=str).encode('utf-8')

    if not original:
        return b"", 1.0

    # Compress with gzip
    compressed = gzip.compress(original, compresslevel=9)

    # Calculate ratio
    ratio = len(compressed) / len(original)

    return compressed, ratio


def compression_fraud_score(
    ratio: float,
    baseline: float = COMPRESSION_BASELINE_MEDICAID
) -> float:
    """
    Score 0-1 where lower ratio = higher fraud likelihood.

    Args:
        ratio: Compression ratio (compressed/original)
        baseline: Expected baseline ratio for legitimate data

    Returns:
        Fraud score 0-1 (1 = highest fraud likelihood)
    """
    if ratio <= 0:
        return 1.0

    if ratio >= baseline:
        # At or above baseline - low fraud likelihood
        return 0.0

    if ratio <= COMPRESSION_FRAUD_THRESHOLD:
        # Below fraud threshold - high fraud likelihood
        return 1.0

    # Linear interpolation between threshold and baseline
    range_size = baseline - COMPRESSION_FRAUD_THRESHOLD
    position = ratio - COMPRESSION_FRAUD_THRESHOLD

    # Invert so lower ratio = higher score
    score = 1.0 - (position / range_size)

    return max(0.0, min(1.0, score))


def batch_compression_analysis(
    records: List[Dict],
    window_size: int = 100,
    domain: str = "medicaid"
) -> List[Dict]:
    """
    Sliding window compression analysis.

    Args:
        records: All records to analyze
        window_size: Size of sliding window
        domain: Domain for baseline selection ("medicaid" or "voucher")

    Returns:
        List of analysis results per window
    """
    if not records:
        return []

    # Select baseline
    if domain == "voucher":
        baseline = COMPRESSION_BASELINE_VOUCHER
    else:
        baseline = COMPRESSION_BASELINE_MEDICAID

    results = []

    for i in range(0, len(records), window_size):
        window = records[i:i + window_size]

        if len(window) < 10:  # Skip small windows
            continue

        _, ratio = compress_records(window)
        fraud_score = compression_fraud_score(ratio, baseline)

        results.append({
            "window_start": i,
            "window_end": i + len(window),
            "record_count": len(window),
            "compression_ratio": ratio,
            "baseline": baseline,
            "fraud_score": fraud_score,
            "anomaly": ratio < COMPRESSION_FRAUD_THRESHOLD
        })

    return results


def analyze_compression_anomalies(
    records: List[Dict],
    domain: str = "medicaid",
    group_by: Optional[str] = None,
    tenant_id: str = TENANT_ID
) -> Dict[str, Any]:
    """
    Full compression analysis with receipt emission.

    Args:
        records: Records to analyze
        domain: Domain for baseline selection
        group_by: Optional field to group by (e.g., "provider_id")
        tenant_id: Tenant identifier

    Returns:
        Entropy analysis receipt
    """
    # Select baseline
    if domain == "voucher":
        baseline = COMPRESSION_BASELINE_VOUCHER
    else:
        baseline = COMPRESSION_BASELINE_MEDICAID

    if group_by:
        # Group records and analyze each group
        groups: Dict[str, List[Dict]] = {}
        for record in records:
            key = record.get(group_by, "unknown")
            if key not in groups:
                groups[key] = []
            groups[key].append(record)

        group_results = []
        anomalies = []

        for key, group_records in groups.items():
            if len(group_records) < 5:
                continue

            _, ratio = compress_records(group_records)
            fraud_score = compression_fraud_score(ratio, baseline)

            result = {
                "group_key": key,
                "record_count": len(group_records),
                "compression_ratio": ratio,
                "fraud_score": fraud_score
            }
            group_results.append(result)

            if ratio < COMPRESSION_FRAUD_THRESHOLD:
                anomalies.append(result)

        # Overall stats
        all_ratios = [r["compression_ratio"] for r in group_results]
        avg_ratio = sum(all_ratios) / len(all_ratios) if all_ratios else 0

        receipt_data = {
            "analysis_type": "compression",
            "domain": domain,
            "group_by": group_by,
            "groups_analyzed": len(group_results),
            "entropy_value": avg_ratio,
            "baseline_value": baseline,
            "anomaly_flag": len(anomalies) > 0,
            "anomaly_count": len(anomalies),
            "anomalies": anomalies[:10],  # Top 10
            "summary": {
                "min_ratio": min(all_ratios) if all_ratios else 0,
                "max_ratio": max(all_ratios) if all_ratios else 0,
                "avg_ratio": avg_ratio
            }
        }
    else:
        # Analyze all records together
        _, ratio = compress_records(records)
        fraud_score = compression_fraud_score(ratio, baseline)

        receipt_data = {
            "analysis_type": "compression",
            "domain": domain,
            "record_count": len(records),
            "entropy_value": ratio,
            "baseline_value": baseline,
            "anomaly_flag": ratio < COMPRESSION_FRAUD_THRESHOLD,
            "fraud_score": fraud_score
        }

    return emit_receipt("entropy_analysis", receipt_data, tenant_id)
