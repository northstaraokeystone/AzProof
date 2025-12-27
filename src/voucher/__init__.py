"""
AzProof Voucher Module

Detects ESA voucher misuse (non-educational spending).

Key Arizona-Specific Patterns:
- Recreational Spending: Ski passes, trampolines
- Luxury Items: Grand pianos, high-end equipment
- Non-Educational Services: Ninja gyms, recreational sports
- Split Purchases: Staying under $2K review threshold
"""

from .ingest import ingest_transaction, batch_ingest, validate_transaction
from .category import (
    classify_transaction,
    load_category_rules,
    detect_category_gaming,
    compute_educational_ratio
)
from .merchant import (
    build_merchant_index,
    flag_new_merchant,
    detect_merchant_front,
    compute_merchant_entropy
)
from .patterns import (
    detect_threshold_gaming,
    detect_seasonal_spike,
    compute_peer_deviation,
    flag_egregious_items
)

__all__ = [
    # ingest
    'ingest_transaction', 'batch_ingest', 'validate_transaction',
    # category
    'classify_transaction', 'load_category_rules',
    'detect_category_gaming', 'compute_educational_ratio',
    # merchant
    'build_merchant_index', 'flag_new_merchant',
    'detect_merchant_front', 'compute_merchant_entropy',
    # patterns
    'detect_threshold_gaming', 'detect_seasonal_spike',
    'compute_peer_deviation', 'flag_egregious_items'
]
