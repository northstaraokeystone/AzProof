"""
AzProof Core Module

Foundation module providing:
- dual_hash: SHA256:BLAKE3 dual hashing
- emit_receipt: Receipt creation with timestamps and hashes
- merkle: Merkle root computation
- StopRule: Exception for stoprule violations
- Constants and configuration
"""

import hashlib
import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

# Try to import blake3, fall back to hashlib.sha256 for second hash if not available
try:
    import blake3
    HAS_BLAKE3 = True
except ImportError:
    HAS_BLAKE3 = False


# === TENANT CONFIGURATION ===
TENANT_ID = "azproof"

# === ARIZONA-SPECIFIC CONSTANTS ===

# Medicaid/AHCCCS
AHCCCS_FRAUD_SCALE = 2_800_000_000  # $2.8B documented
AHCCCS_RECOVERED = 125_000_000      # $125M recovered (~5%)
AIHP_CONCENTRATION_THRESHOLD = 0.80  # >80% AIHP claims = flag
MAX_PATIENTS_PER_PROVIDER_DAY = 30   # Physical impossibility
SHELL_MIN_CLUSTER = 5                # Minimum LLCs for shell detection
SHELL_BILLING_THRESHOLD = 10_000_000  # $10M combined = flag

# Key Actors (from Grok research)
ALI_FRAUD_AMOUNT = 564_000_000       # Farrukh Ali $564M
ANAGHO_FRAUD_AMOUNT = 70_000_000     # Rita Anagho $70M
KOLEOSHO_FRAUD_AMOUNT = 51_000_000   # Daud Koleosho $51M

# ESA Voucher
ESA_ANNUAL_SCALE = 1_000_000_000     # ~$1B/year
ESA_REVIEW_THRESHOLD = 2_000         # Auto-approve under $2K
ESA_EGREGIOUS_KEYWORDS = ["ski", "snowbowl", "piano", "trampoline", "ninja"]

# Fiscal
AZ_DEFICIT = 1_400_000_000           # $1.4B+ shortfall
FLAT_TAX_COST = 700_000_000          # $700M+ revenue drop

# Entropy Thresholds
COMPRESSION_BASELINE_MEDICAID = 0.65
COMPRESSION_BASELINE_VOUCHER = 0.70
COMPRESSION_FRAUD_THRESHOLD = 0.40
NETWORK_ENTROPY_BASELINE = 2.5       # Healthy network ~2.5 bits

# SLOs
DETECTION_PRECISION_MIN = 0.85
DETECTION_RECALL_MIN = 0.90
FALSE_POSITIVE_RATE_MAX = 0.15
INGEST_LATENCY_MS = 50
DETECTION_LATENCY_MS = 1000

# Receipts ledger path
RECEIPTS_LEDGER_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "receipts.jsonl")

# Receipt schema for autodocumentation
RECEIPT_SCHEMA = {
    "base_fields": ["receipt_type", "ts", "tenant_id", "payload_hash"],
    "hash_format": "sha256:blake3",
    "tenant_id": TENANT_ID
}


class StopRule(Exception):
    """
    Exception raised when a stoprule triggers.

    Stoprules indicate critical failures that must halt execution.
    Never catch silently - always log and re-raise or handle explicitly.
    """

    def __init__(self, rule_name: str, message: str, context: Optional[Dict] = None):
        self.rule_name = rule_name
        self.message = message
        self.context = context or {}
        super().__init__(f"STOPRULE [{rule_name}]: {message}")


def dual_hash(data: Union[bytes, str]) -> str:
    """
    Compute dual hash in SHA256:BLAKE3 format.

    Args:
        data: Input data as bytes or string

    Returns:
        Hash string in format "sha256_hex:blake3_hex"

    Pure function - no side effects.
    """
    if isinstance(data, str):
        data = data.encode('utf-8')

    # SHA256
    sha256_hash = hashlib.sha256(data).hexdigest()

    # BLAKE3 (or fallback to SHA256 with different prefix if blake3 not available)
    if HAS_BLAKE3:
        blake3_hash = blake3.blake3(data).hexdigest()
    else:
        # Fallback: use SHA256 with salt to differentiate
        blake3_hash = hashlib.sha256(b"blake3_fallback:" + data).hexdigest()

    return f"{sha256_hash}:{blake3_hash}"


def emit_receipt(receipt_type: str, data: Dict[str, Any], tenant_id: str = TENANT_ID) -> Dict[str, Any]:
    """
    Create a receipt with timestamp, tenant_id, and payload hash.

    Args:
        receipt_type: Type of receipt (e.g., "medicaid_ingest")
        data: Payload data to include in receipt
        tenant_id: Tenant identifier (default: "azproof")

    Returns:
        Complete receipt dict with ts, tenant_id, and payload_hash

    Also prints JSON to stdout for ledger capture.
    """
    # Create timestamp
    ts = datetime.now(timezone.utc).isoformat()

    # Compute payload hash
    payload_json = json.dumps(data, sort_keys=True, default=str)
    payload_hash = dual_hash(payload_json)

    # Build receipt
    receipt = {
        "receipt_type": receipt_type,
        "ts": ts,
        "tenant_id": tenant_id,
        **data,
        "payload_hash": payload_hash
    }

    # Append to ledger
    append_to_ledger(receipt)

    return receipt


def append_to_ledger(receipt: Dict[str, Any]) -> None:
    """
    Append a receipt to the receipts.jsonl ledger.

    Args:
        receipt: Receipt dict to append
    """
    try:
        with open(RECEIPTS_LEDGER_PATH, 'a') as f:
            f.write(json.dumps(receipt, default=str) + '\n')
    except IOError:
        # If we can't write to ledger, continue (for testing scenarios)
        pass


def load_receipts(ledger_path: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Load all receipts from the ledger.

    Args:
        ledger_path: Path to ledger file (default: RECEIPTS_LEDGER_PATH)

    Returns:
        List of receipt dicts
    """
    path = ledger_path or RECEIPTS_LEDGER_PATH
    receipts = []

    try:
        with open(path, 'r') as f:
            for line in f:
                line = line.strip()
                if line:
                    receipts.append(json.loads(line))
    except FileNotFoundError:
        pass

    return receipts


def merkle(items: List[Union[str, bytes, Dict]]) -> str:
    """
    Compute Merkle root using dual_hash.

    Args:
        items: List of items to hash (strings, bytes, or dicts)

    Returns:
        Merkle root as dual hash string

    Handles empty lists and odd counts per CLAUDEME spec.
    """
    if not items:
        # Empty list: hash of empty string
        return dual_hash("")

    # Convert items to hashes
    hashes = []
    for item in items:
        if isinstance(item, dict):
            item = json.dumps(item, sort_keys=True, default=str)
        if isinstance(item, str):
            item = item.encode('utf-8')
        hashes.append(dual_hash(item))

    # Build Merkle tree
    while len(hashes) > 1:
        if len(hashes) % 2 == 1:
            # Odd count: duplicate last hash
            hashes.append(hashes[-1])

        new_level = []
        for i in range(0, len(hashes), 2):
            combined = hashes[i] + hashes[i + 1]
            new_level.append(dual_hash(combined))
        hashes = new_level

    return hashes[0]


def stoprule_hash_mismatch(expected: str, actual: str, context: Optional[Dict] = None) -> None:
    """
    Stoprule: Hash mismatch detected.

    Emits anomaly receipt and raises StopRule exception.
    """
    emit_receipt("anomaly", {
        "anomaly_type": "hash_mismatch",
        "expected": expected,
        "actual": actual,
        "context": context or {}
    })
    raise StopRule("hash_mismatch", f"Expected {expected}, got {actual}", context)


def stoprule_invalid_receipt(reason: str, receipt: Optional[Dict] = None) -> None:
    """
    Stoprule: Invalid receipt structure.

    Emits anomaly receipt and raises StopRule exception.
    """
    emit_receipt("anomaly", {
        "anomaly_type": "invalid_receipt",
        "reason": reason,
        "receipt": receipt
    })
    raise StopRule("invalid_receipt", reason, {"receipt": receipt})


def stoprule_slo_violation(slo_name: str, threshold: float, actual: float, context: Optional[Dict] = None) -> None:
    """
    Stoprule: SLO threshold violation.

    Emits violation receipt and raises StopRule exception.
    """
    emit_receipt("violation", {
        "violation_type": "slo_breach",
        "slo_name": slo_name,
        "threshold": threshold,
        "actual": actual,
        "context": context or {}
    })
    raise StopRule("slo_violation", f"SLO {slo_name}: threshold={threshold}, actual={actual}", context)


def validate_receipt(receipt: Dict[str, Any]) -> tuple:
    """
    Validate a receipt has required fields and valid structure.

    Args:
        receipt: Receipt dict to validate

    Returns:
        Tuple of (is_valid: bool, reason: str)
    """
    required_fields = ["receipt_type", "ts", "tenant_id", "payload_hash"]

    for field in required_fields:
        if field not in receipt:
            return False, f"Missing required field: {field}"

    if receipt["tenant_id"] != TENANT_ID:
        return False, f"Invalid tenant_id: {receipt['tenant_id']}"

    # Validate hash format
    payload_hash = receipt.get("payload_hash", "")
    if ":" not in payload_hash:
        return False, f"Invalid hash format: {payload_hash}"

    parts = payload_hash.split(":")
    if len(parts) != 2 or len(parts[0]) != 64 or len(parts[1]) != 64:
        return False, f"Invalid hash lengths in: {payload_hash}"

    return True, "valid"


def get_risk_level(score: float) -> str:
    """
    Convert numeric risk score to level.

    Args:
        score: Risk score between 0 and 1

    Returns:
        Risk level: "low", "medium", "high", or "critical"
    """
    if score < 0.2:
        return "low"
    elif score < 0.5:
        return "medium"
    elif score < 0.8:
        return "high"
    else:
        return "critical"
