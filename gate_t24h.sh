#!/bin/bash
# Gate T+24h: MVP
#
# Validates MVP functionality.

set -e

echo "=========================================="
echo "AzProof Gate T+24h: MVP"
echo "=========================================="

PASS=0
FAIL=0

# Run T+2h gate first
echo "Running T+2h gate as prerequisite..."
if ./gate_t2h.sh; then
    echo "T+2h gate passed"
else
    echo "T+2h gate failed - aborting T+24h"
    exit 1
fi

echo ""
echo "=========================================="
echo "T+24h Specific Checks"
echo "=========================================="

# Check medicaid ingest
echo -n "Checking medicaid ingest... "
if python3 -c "
from src.medicaid.ingest import ingest_claim
r = ingest_claim({
    'claim_id': 'test',
    'provider_id': 'NPI123',
    'billed_amount': 100.0
}, 'azproof')
assert r['receipt_type'] == 'medicaid_ingest'
print('PASS')
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check voucher category
echo -n "Checking voucher category... "
if python3 -c "
from src.voucher.category import classify_transaction
r = classify_transaction({
    'merchant_name': 'Arizona Snowbowl',
    'amount': 500
})
assert r['category'] == 'non_educational'
print('PASS')
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check entropy compression
echo -n "Checking entropy compression... "
if python3 -c "
from src.entropy.compression import compress_records
data = [{'a': i, 'b': 'test'} for i in range(100)]
_, ratio = compress_records(data)
assert 0 < ratio < 1
print('PASS (ratio={:.3f})'.format(ratio))
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# 100-cycle smoke test
echo -n "Running 100-cycle smoke test... "
if python3 -c "
from src.sim import run_simulation, SimConfig
r = run_simulation(SimConfig(n_cycles=100))
print('PASS ({} violations)'.format(len(r.violations)))
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=========================================="
echo "Gate T+24h Results: $PASS passed, $FAIL failed"
echo "=========================================="

if [ $FAIL -gt 0 ]; then
    echo "GATE FAILED"
    exit 1
else
    echo "GATE PASSED"
    exit 0
fi
