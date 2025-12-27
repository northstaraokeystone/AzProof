# AzProof v1.0 Specification

## Overview

AzProof is a receipts-native fraud detection system targeting Arizona's three documented fraud ecosystems:
1. Medicaid/AHCCCS billing fraud ($2-2.8B scale)
2. ESA Voucher misuse (~$1B/year)
3. Budget Deficit Correlation ($1.4B+ shortfall)

## Inputs

### Medicaid Claims
```json
{
    "claim_id": "string",
    "provider_id": "string",
    "provider_name": "string",
    "patient_id": "string",
    "patient_tribal_affiliation": "string|null",
    "service_type": "string",
    "service_date": "ISO8601",
    "billed_amount": "float",
    "paid_amount": "float",
    "facility_address": "string",
    "facility_type": "string"
}
```

### ESA Voucher Transactions
```json
{
    "txn_id": "string",
    "account_id": "string",
    "merchant_id": "string",
    "merchant_name": "string",
    "merchant_category_code": "string",
    "amount": "float",
    "txn_date": "ISO8601",
    "description": "string"
}
```

### Fiscal Data
```json
{
    "period": "string",
    "revenue_source": "string",
    "amount": "float",
    "policy_context": "string"
}
```

## Outputs

### Detection Receipts
All outputs are JSON receipts with:
- `receipt_type`: Type identifier
- `ts`: ISO8601 timestamp
- `tenant_id`: Always "azproof"
- `payload_hash`: SHA256:BLAKE3 dual hash

### Receipt Types
- `medicaid_ingest`: Claim ingestion confirmation
- `network_analysis`: Provider network analysis results
- `aihp_flag`: AIHP exploitation detection
- `shell_detection`: Shell LLC cluster detection
- `billing_anomaly`: Billing pattern anomalies
- `voucher_category`: Transaction classification
- `merchant_flag`: Suspicious merchant flags
- `voucher_pattern`: Spending pattern detection
- `fiscal_analysis`: Revenue/policy/deficit analysis
- `entropy_analysis`: Entropy-based anomaly detection
- `gap`: Manual intervention gap
- `helper_blueprint`: Automation blueprint
- `loop_cycle`: Meta-loop cycle metrics

## SLOs (Service Level Objectives)

| Metric | Threshold | Description |
|--------|-----------|-------------|
| Detection Precision | >= 0.85 | True positives / (True positives + False positives) |
| Detection Recall | >= 0.90 | True positives / (True positives + False negatives) |
| False Positive Rate | <= 0.15 | False positives / (False positives + True negatives) |
| Ingest Latency | <= 50ms p95 | Time to process and emit ingest receipt |
| Detection Latency | <= 1000ms p95 | Time to run detection and emit result |
| Compression Ratio | >= 0.40 | Legitimate data compression threshold |
| Network Entropy | 2.0-3.0 | Healthy network entropy range |

## Stoprules

Stoprules halt execution on critical errors:
- `stoprule_hash_mismatch`: Expected vs actual hash mismatch
- `stoprule_invalid_receipt`: Malformed receipt structure
- `stoprule_slo_violation`: SLO threshold breach

## Receipts Ledger

All receipts are appended to `receipts.jsonl` in append-only mode.
Each line is a complete JSON receipt with dual-hash verification.

## Tenant Configuration

```
TENANT_ID = "azproof"
```

## Version

v1.0 - Initial Arizona deployment
