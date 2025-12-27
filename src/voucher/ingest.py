"""
ESA Voucher Transaction Ingestion Module

Ingests ESA transaction data into the receipts stream.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from ..core import emit_receipt, dual_hash, merkle, TENANT_ID


# Required fields for a valid transaction
REQUIRED_TXN_FIELDS = [
    "txn_id",
    "amount"
]

# Optional but recommended fields
OPTIONAL_TXN_FIELDS = [
    "account_id",
    "merchant_id",
    "merchant_name",
    "merchant_category_code",
    "txn_date",
    "description"
]


def validate_transaction(txn: Dict[str, Any]) -> Tuple[bool, str]:
    """
    Check required fields and format of a transaction.

    Args:
        txn: Transaction dictionary to validate

    Returns:
        Tuple of (valid: bool, reason: str)
    """
    for field in REQUIRED_TXN_FIELDS:
        if field not in txn:
            return False, f"Missing required field: {field}"

    # Validate txn_id is non-empty
    if not txn.get("txn_id"):
        return False, "txn_id cannot be empty"

    # Validate amount is numeric and non-negative
    amount = txn.get("amount")
    if not isinstance(amount, (int, float)):
        return False, "amount must be numeric"
    if amount < 0:
        return False, "amount cannot be negative"

    # Validate txn_date format if provided
    if "txn_date" in txn and txn["txn_date"]:
        try:
            datetime.fromisoformat(txn["txn_date"].replace('Z', '+00:00'))
        except (ValueError, AttributeError):
            return False, f"Invalid txn_date format: {txn['txn_date']}"

    return True, "valid"


def ingest_transaction(txn: Dict[str, Any], tenant_id: str = TENANT_ID) -> Dict[str, Any]:
    """
    Validate transaction and emit ingest_receipt.

    Args:
        txn: Transaction dictionary
        tenant_id: Tenant identifier

    Returns:
        Receipt dict with txn_hash

    Raises:
        ValueError: If transaction is invalid
    """
    valid, reason = validate_transaction(txn)
    if not valid:
        raise ValueError(f"Invalid transaction: {reason}")

    # Compute transaction hash
    txn_hash = dual_hash(str(txn))

    # Build receipt data
    receipt_data = {
        "txn_hash": txn_hash,
        "txn_id": txn.get("txn_id"),
        "account_id": txn.get("account_id"),
        "merchant_id": txn.get("merchant_id"),
        "merchant_name": txn.get("merchant_name"),
        "merchant_category_code": txn.get("merchant_category_code"),
        "amount": txn.get("amount"),
        "description": txn.get("description")
    }

    # Emit receipt
    receipt = emit_receipt("voucher_ingest", receipt_data, tenant_id)

    return receipt


def batch_ingest(txns: List[Dict[str, Any]], tenant_id: str = TENANT_ID) -> Dict[str, Any]:
    """
    Batch ingest transactions with merkle anchor.

    Args:
        txns: List of transaction dictionaries
        tenant_id: Tenant identifier

    Returns:
        Batch receipt with merkle_root and individual txn hashes
    """
    if not txns:
        return emit_receipt("voucher_batch_ingest", {
            "txn_count": 0,
            "merkle_root": merkle([]),
            "txns": []
        }, tenant_id)

    # Process each transaction
    receipts = []
    txn_hashes = []
    errors = []

    for i, txn in enumerate(txns):
        try:
            receipt = ingest_transaction(txn, tenant_id)
            receipts.append(receipt)
            txn_hashes.append(receipt["txn_hash"])
        except ValueError as e:
            errors.append({"index": i, "error": str(e)})

    # Compute merkle root
    merkle_root = merkle(txn_hashes) if txn_hashes else merkle([])

    # Calculate totals
    total_amount = sum(r.get("amount", 0) for r in receipts)

    # Build batch receipt
    batch_data = {
        "txn_count": len(receipts),
        "error_count": len(errors),
        "total_amount": total_amount,
        "merkle_root": merkle_root,
        "txn_hashes": txn_hashes,
        "errors": errors if errors else None
    }

    return emit_receipt("voucher_batch_ingest", batch_data, tenant_id)


def extract_txns_by_account(receipts: List[Dict], account_id: str) -> List[Dict]:
    """
    Extract all transactions for a specific account from receipts.

    Args:
        receipts: List of receipt dicts
        account_id: Account ID to filter by

    Returns:
        List of matching receipts
    """
    return [
        r for r in receipts
        if r.get("receipt_type") == "voucher_ingest"
        and r.get("account_id") == account_id
    ]


def extract_txns_by_merchant(receipts: List[Dict], merchant_id: str) -> List[Dict]:
    """
    Extract all transactions for a specific merchant from receipts.

    Args:
        receipts: List of receipt dicts
        merchant_id: Merchant ID to filter by

    Returns:
        List of matching receipts
    """
    return [
        r for r in receipts
        if r.get("receipt_type") == "voucher_ingest"
        and r.get("merchant_id") == merchant_id
    ]
