#!/usr/bin/env python3
"""
AzProof CLI - Arizona Receipts-Native Fraud Detection System

Usage:
    python cli.py ingest-medicaid <file>
    python cli.py ingest-voucher <file>
    python cli.py analyze-network
    python cli.py detect-shells
    python cli.py run-simulation [--cycles=N]
    python cli.py run-loop [--interval=SEC]
    python cli.py emit-receipt <type> <data>
"""

import argparse
import json
import sys
from datetime import datetime

from src.core import emit_receipt, dual_hash, TENANT_ID


def cmd_ingest_medicaid(args):
    """Ingest Medicaid claims from file."""
    from src.medicaid.ingest import batch_ingest

    with open(args.file, 'r') as f:
        claims = json.load(f)

    receipt = batch_ingest(claims, TENANT_ID)
    print(json.dumps(receipt, indent=2))
    return 0


def cmd_ingest_voucher(args):
    """Ingest ESA voucher transactions from file."""
    from src.voucher.ingest import batch_ingest

    with open(args.file, 'r') as f:
        txns = json.load(f)

    receipt = batch_ingest(txns, TENANT_ID)
    print(json.dumps(receipt, indent=2))
    return 0


def cmd_analyze_network(args):
    """Run provider network analysis."""
    from src.medicaid.network import build_provider_graph, detect_clusters, compute_network_entropy
    from src.core import load_receipts

    receipts = load_receipts()
    medicaid_receipts = [r for r in receipts if r.get('receipt_type') == 'medicaid_ingest']

    graph = build_provider_graph(medicaid_receipts)
    clusters = detect_clusters(graph, min_size=3)
    entropy = compute_network_entropy(graph)

    result = {
        "n_providers": len(graph.get("nodes", [])),
        "n_clusters": len(clusters),
        "network_entropy": entropy
    }
    print(json.dumps(result, indent=2))
    return 0


def cmd_detect_shells(args):
    """Detect shell LLC clusters."""
    from src.medicaid.shell import build_ownership_graph, detect_shell_clusters
    from src.core import load_receipts

    receipts = load_receipts()
    providers = [r for r in receipts if r.get('receipt_type') == 'medicaid_ingest']

    graph = build_ownership_graph(providers)
    clusters = detect_shell_clusters(graph, min_shared=2)

    print(json.dumps({"clusters": clusters}, indent=2))
    return 0


def cmd_run_simulation(args):
    """Run Monte Carlo simulation."""
    from src.sim import run_simulation, SimConfig

    config = SimConfig(n_cycles=args.cycles)
    result = run_simulation(config)

    summary = {
        "cycles_completed": result.cycle,
        "violations": len(result.violations),
        "medicaid_receipts": len(result.medicaid_receipts),
        "voucher_receipts": len(result.voucher_receipts),
        "detection_receipts": len(result.detection_receipts)
    }
    print(json.dumps(summary, indent=2))
    return 0


def cmd_run_loop(args):
    """Run the meta-loop."""
    from src.loop.cycle import start_loop

    print(f"Starting meta-loop with {args.interval}s interval...")
    start_loop(interval_sec=args.interval)
    return 0


def cmd_emit_receipt(args):
    """Emit a receipt to stdout."""
    data = json.loads(args.data)
    receipt = emit_receipt(args.type, data)
    print(json.dumps(receipt, indent=2))
    return 0


def cmd_verify(args):
    """Run verification protocol."""
    from src.core import dual_hash, emit_receipt

    # Test dual_hash
    h = dual_hash("test")
    assert ":" in h, "dual_hash must produce colon-separated format"
    print("PASS: dual_hash")

    # Test emit_receipt
    r = emit_receipt("test", {"key": "value"})
    assert r["receipt_type"] == "test"
    assert r["tenant_id"] == TENANT_ID
    assert "payload_hash" in r
    print("PASS: emit_receipt")

    print("\nVerification complete.")
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="AzProof - Arizona Receipts-Native Fraud Detection"
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # ingest-medicaid
    p_med = subparsers.add_parser("ingest-medicaid", help="Ingest Medicaid claims")
    p_med.add_argument("file", help="JSON file with claims")
    p_med.set_defaults(func=cmd_ingest_medicaid)

    # ingest-voucher
    p_voucher = subparsers.add_parser("ingest-voucher", help="Ingest ESA voucher transactions")
    p_voucher.add_argument("file", help="JSON file with transactions")
    p_voucher.set_defaults(func=cmd_ingest_voucher)

    # analyze-network
    p_network = subparsers.add_parser("analyze-network", help="Analyze provider network")
    p_network.set_defaults(func=cmd_analyze_network)

    # detect-shells
    p_shells = subparsers.add_parser("detect-shells", help="Detect shell LLC clusters")
    p_shells.set_defaults(func=cmd_detect_shells)

    # run-simulation
    p_sim = subparsers.add_parser("run-simulation", help="Run Monte Carlo simulation")
    p_sim.add_argument("--cycles", type=int, default=100, help="Number of simulation cycles")
    p_sim.set_defaults(func=cmd_run_simulation)

    # run-loop
    p_loop = subparsers.add_parser("run-loop", help="Run the meta-loop")
    p_loop.add_argument("--interval", type=int, default=60, help="Loop interval in seconds")
    p_loop.set_defaults(func=cmd_run_loop)

    # emit-receipt
    p_emit = subparsers.add_parser("emit-receipt", help="Emit a receipt")
    p_emit.add_argument("type", help="Receipt type")
    p_emit.add_argument("data", help="JSON data for receipt")
    p_emit.set_defaults(func=cmd_emit_receipt)

    # verify
    p_verify = subparsers.add_parser("verify", help="Run verification protocol")
    p_verify.set_defaults(func=cmd_verify)

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
