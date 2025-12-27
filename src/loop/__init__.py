"""
AzProof Loop Module

Self-improving meta-layer. Learns from gaps, creates helpers, measures effectiveness.

The LOOP Cycle (every 60 seconds):
1. SENSE: Query receipt stream for recent activity
2. ANALYZE: Run HUNTER patterns on receipts
3. HARVEST: Collect manual intervention receipts
4. HYPOTHESIZE: Create automation blueprints from gaps
5. GATE: HITL gate based on risk
6. ACTUATE: Deploy approved helpers
7. EMIT: Record cycle results
"""

from .cycle import run_cycle, start_loop, stop_loop
from .sense import sense_receipts, query_recent, filter_by_type
from .harvest import harvest_gaps, rank_gaps, identify_patterns
from .genesis import synthesize_helper, validate_blueprint, estimate_savings
from .gate import calculate_risk, request_approval, check_approval, auto_approve
from .effectiveness import measure_effectiveness, track_helper, retire_helper

__all__ = [
    # cycle
    'run_cycle', 'start_loop', 'stop_loop',
    # sense
    'sense_receipts', 'query_recent', 'filter_by_type',
    # harvest
    'harvest_gaps', 'rank_gaps', 'identify_patterns',
    # genesis
    'synthesize_helper', 'validate_blueprint', 'estimate_savings',
    # gate
    'calculate_risk', 'request_approval', 'check_approval', 'auto_approve',
    # effectiveness
    'measure_effectiveness', 'track_helper', 'retire_helper'
]
