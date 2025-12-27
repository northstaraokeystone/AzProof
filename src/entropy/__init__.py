"""
AzProof Entropy Module

Information-theoretic fraud detection.
Compression ratio as fraud signal.

Core Insight:
- Legitimate operations follow PATTERNS (compressible)
- Fraud must EVADE patterns (incompressible)
- High entropy in billing = fraud signal
"""

from .compression import (
    compress_records,
    compression_fraud_score,
    batch_compression_analysis
)
from .network import (
    network_entropy,
    detect_entropy_anomaly,
    temporal_network_entropy
)
from .temporal import (
    time_series_entropy,
    detect_regularity,
    entropy_change_point
)

__all__ = [
    # compression
    'compress_records', 'compression_fraud_score', 'batch_compression_analysis',
    # network
    'network_entropy', 'detect_entropy_anomaly', 'temporal_network_entropy',
    # temporal
    'time_series_entropy', 'detect_regularity', 'entropy_change_point'
]
