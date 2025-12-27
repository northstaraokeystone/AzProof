#!/bin/bash
# Gate T+48h: HARDENED
#
# Final validation before ship.

set -e

echo "=========================================="
echo "AzProof Gate T+48h: HARDENED"
echo "=========================================="

PASS=0
FAIL=0

# Run T+24h gate first
echo "Running T+24h gate as prerequisite..."
if ./gate_t24h.sh; then
    echo "T+24h gate passed"
else
    echo "T+24h gate failed - aborting T+48h"
    exit 1
fi

echo ""
echo "=========================================="
echo "T+48h Specific Checks"
echo "=========================================="

# Scenario 3: ALI_PATTERN
echo -n "Running ALI_PATTERN scenario... "
if python3 -c "
from src.sim import run_scenario
r = run_scenario('ALI_PATTERN')
print('PASS (detected={}, entities={})'.format(r.ali_detected, r.entities_flagged))
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Scenario 4: VOUCHER_EGREGIOUS
echo -n "Running VOUCHER_EGREGIOUS scenario... "
if python3 -c "
from src.sim import run_scenario
r = run_scenario('VOUCHER_EGREGIOUS')
detections = len(r.detection_receipts)
print('PASS (detections={})'.format(detections))
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Scenario 6: GODEL (edge cases)
echo -n "Running GODEL scenario... "
if python3 -c "
from src.sim import run_scenario
r = run_scenario('GODEL')
crashes = [v for v in r.violations if v.get('type') == 'crash']
print('PASS (crashes={})'.format(len(crashes)))
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Loop cycle
echo -n "Running loop cycle... "
if python3 -c "
from src.loop.cycle import run_cycle
r = run_cycle()
assert r['receipt_type'] == 'loop_cycle'
print('PASS')
" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=========================================="
echo "Gate T+48h Results: $PASS passed, $FAIL failed"
echo "=========================================="

if [ $FAIL -gt 0 ]; then
    echo "GATE FAILED - DO NOT SHIP"
    exit 1
else
    echo "GATE PASSED - READY TO SHIP"
    echo ""
    echo "=========================================="
    echo "SHIP IT"
    echo "=========================================="
    exit 0
fi
