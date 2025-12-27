"""
AzProof Fiscal Module

Tracks policy impact on state budget, correlates fraud to deficit.

Key Arizona Patterns:
- Flat 2.5% income tax: $700M+ revenue drop
- ESA universal expansion: ~$1B/year uncapped
- Medicaid fraud losses: ~$125M recovered of $2.8B
"""

from .revenue import (
    ingest_revenue_data,
    compute_yoy_change,
    attribute_policy_impact
)
from .policy import (
    ingest_policy_change,
    compute_policy_cost,
    detect_budget_stress
)
from .deficit import (
    compute_deficit,
    attribute_deficit,
    project_deficit
)

__all__ = [
    # revenue
    'ingest_revenue_data', 'compute_yoy_change', 'attribute_policy_impact',
    # policy
    'ingest_policy_change', 'compute_policy_cost', 'detect_budget_stress',
    # deficit
    'compute_deficit', 'attribute_deficit', 'project_deficit'
]
