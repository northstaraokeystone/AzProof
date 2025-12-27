#!/bin/bash
# Gate T+2h: SKELETON
#
# Validates that the skeleton structure is in place.

set -e

echo "=========================================="
echo "AzProof Gate T+2h: SKELETON"
echo "=========================================="

PASS=0
FAIL=0

# Check spec.md exists
echo -n "Checking spec.md exists... "
if [ -f "spec.md" ]; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check ledger_schema.json exists
echo -n "Checking ledger_schema.json exists... "
if [ -f "ledger_schema.json" ]; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check cli.py exists
echo -n "Checking cli.py exists... "
if [ -f "cli.py" ]; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check src/core.py exists
echo -n "Checking src/core.py exists... "
if [ -f "src/core.py" ]; then
    echo "PASS"
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check dual_hash function
echo -n "Checking dual_hash function... "
if python3 -c "from src.core import dual_hash; h=dual_hash('test'); assert ':' in h; print('PASS')" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check emit_receipt function
echo -n "Checking emit_receipt function... "
if python3 -c "from src.core import emit_receipt; r=emit_receipt('test', {'key':'value'}); assert r['receipt_type']=='test'; print('PASS')" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check merkle function
echo -n "Checking merkle function... "
if python3 -c "from src.core import merkle; m=merkle(['a','b','c']); assert ':' in m; print('PASS')" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

# Check StopRule class
echo -n "Checking StopRule class... "
if python3 -c "from src.core import StopRule; s=StopRule('test', 'message'); assert s.rule_name=='test'; print('PASS')" 2>/dev/null; then
    PASS=$((PASS + 1))
else
    echo "FAIL"
    FAIL=$((FAIL + 1))
fi

echo ""
echo "=========================================="
echo "Gate T+2h Results: $PASS passed, $FAIL failed"
echo "=========================================="

if [ $FAIL -gt 0 ]; then
    echo "GATE FAILED"
    exit 1
else
    echo "GATE PASSED"
    exit 0
fi
