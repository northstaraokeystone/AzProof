"""
AzProof Medicaid Module

Detects AHCCCS/Medicaid billing fraud, shell LLC networks, and AIHP exploitation.

Key Arizona-Specific Patterns:
- Shell LLC Proliferation: Provider creates 41+ clinics to bill (Ali case)
- AIHP Exploitation: Target Native Americans via fee-for-service
- Kickback Networks: Sober homes refer to specific clinics
- Ghost Billing: Services never rendered
- Patient Recruitment: Pay for referrals
"""

from .ingest import ingest_claim, batch_ingest, validate_claim
from .network import (
    build_provider_graph,
    detect_clusters,
    compute_network_entropy,
    flag_hub_providers,
    trace_referral_chains
)
from .aihp import (
    flag_aihp_claims,
    detect_geographic_mismatch,
    compute_aihp_concentration,
    detect_recruitment_patterns
)
from .billing import (
    compute_billing_velocity,
    detect_impossible_volume,
    compression_ratio_billing,
    detect_upcoding,
    compare_to_baseline
)
from .shell import (
    extract_principals,
    build_ownership_graph,
    detect_shell_clusters,
    compute_registration_burst,
    flag_shell_network
)

__all__ = [
    # ingest
    'ingest_claim', 'batch_ingest', 'validate_claim',
    # network
    'build_provider_graph', 'detect_clusters', 'compute_network_entropy',
    'flag_hub_providers', 'trace_referral_chains',
    # aihp
    'flag_aihp_claims', 'detect_geographic_mismatch',
    'compute_aihp_concentration', 'detect_recruitment_patterns',
    # billing
    'compute_billing_velocity', 'detect_impossible_volume',
    'compression_ratio_billing', 'detect_upcoding', 'compare_to_baseline',
    # shell
    'extract_principals', 'build_ownership_graph', 'detect_shell_clusters',
    'compute_registration_burst', 'flag_shell_network'
]
