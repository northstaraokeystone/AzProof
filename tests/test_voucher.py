"""
Tests for Voucher module.
"""

import pytest
from src.voucher.ingest import ingest_transaction, batch_ingest, validate_transaction
from src.voucher.category import (
    classify_transaction,
    load_category_rules,
    compute_educational_ratio
)
from src.voucher.merchant import (
    build_merchant_index,
    flag_new_merchant,
    detect_merchant_front,
    compute_merchant_entropy
)
from src.voucher.patterns import (
    detect_threshold_gaming,
    detect_seasonal_spike,
    flag_egregious_items
)


class TestTransactionValidation:
    """Tests for transaction validation."""

    def test_validate_valid_transaction(self, sample_transaction):
        """Test validation of valid transaction."""
        valid, reason = validate_transaction(sample_transaction)
        assert valid is True
        assert reason == "valid"

    def test_validate_missing_txn_id(self):
        """Test validation with missing txn_id."""
        txn = {"amount": 100}
        valid, reason = validate_transaction(txn)
        assert valid is False
        assert "txn_id" in reason

    def test_validate_negative_amount(self):
        """Test validation with negative amount."""
        txn = {"txn_id": "TXN123", "amount": -100}
        valid, reason = validate_transaction(txn)
        assert valid is False
        assert "negative" in reason


class TestTransactionIngestion:
    """Tests for transaction ingestion."""

    def test_ingest_transaction(self, sample_transaction):
        """Test transaction ingestion."""
        receipt = ingest_transaction(sample_transaction)

        assert receipt["receipt_type"] == "voucher_ingest"
        assert "txn_hash" in receipt
        assert receipt["amount"] == sample_transaction["amount"]

    def test_batch_ingest(self, sample_transaction):
        """Test batch ingestion."""
        txns = [sample_transaction, sample_transaction.copy()]
        txns[1]["txn_id"] = "TXN_002"

        receipt = batch_ingest(txns)

        assert receipt["receipt_type"] == "voucher_batch_ingest"
        assert receipt["txn_count"] == 2
        assert "merkle_root" in receipt


class TestCategoryClassification:
    """Tests for transaction category classification."""

    def test_classify_educational(self, sample_transaction):
        """Test classification of educational transaction."""
        result = classify_transaction(sample_transaction)

        assert result["category"] == "educational"
        assert result["educational_flag"] is True
        assert result["confidence"] > 0.5

    def test_classify_non_educational(self, sample_egregious_transaction):
        """Test classification of non-educational transaction."""
        result = classify_transaction(sample_egregious_transaction)

        assert result["category"] == "non_educational"
        assert result["educational_flag"] is False
        assert result["confidence"] > 0.5

    def test_classify_ski_keywords(self):
        """Test classification catches ski keywords."""
        txn = {
            "txn_id": "SKI_TEST",
            "merchant_name": "Ski Shop",
            "amount": 500,
            "description": "ski equipment"
        }
        result = classify_transaction(txn)

        assert result["category"] == "non_educational"

    def test_load_category_rules(self):
        """Test loading category rules."""
        rules = load_category_rules()

        assert "educational" in rules
        assert "non_educational" in rules
        assert "egregious_keywords" in rules

    def test_compute_educational_ratio(self, sample_transaction, sample_egregious_transaction):
        """Test educational ratio computation."""
        txns = [sample_transaction, sample_egregious_transaction]

        ratio = compute_educational_ratio(sample_transaction["account_id"], txns)

        # Only one txn for this account, and it's educational
        assert ratio == 1.0


class TestMerchantAnalysis:
    """Tests for merchant analysis."""

    def test_build_merchant_index(self, sample_transaction):
        """Test building merchant index."""
        txns = [sample_transaction for _ in range(5)]

        index = build_merchant_index(txns)

        assert sample_transaction["merchant_id"] in index
        merchant = index[sample_transaction["merchant_id"]]
        assert merchant["txn_count"] == 5

    def test_flag_new_merchant_high_volume(self, sample_transaction):
        """Test flagging new high-volume merchant."""
        merchant = {
            "merchant_id": "NEW_MER",
            "total_spend": 15000,  # > threshold
            "unique_accounts": 5
        }

        flagged = flag_new_merchant(merchant)
        assert flagged is True

    def test_compute_merchant_entropy(self, sample_transaction):
        """Test merchant entropy computation."""
        # Create transactions with varying amounts
        txns = []
        for i in range(10):
            txn = sample_transaction.copy()
            txn["txn_id"] = f"TXN_{i}"
            txn["amount"] = 100 + i * 50
            txns.append(txn)

        entropy = compute_merchant_entropy(
            sample_transaction["merchant_id"],
            txns
        )

        assert entropy >= 0


class TestPatternDetection:
    """Tests for spending pattern detection."""

    def test_detect_threshold_gaming(self):
        """Test threshold gaming detection."""
        # Create transactions just under $2000
        txns = [
            {"txn_id": f"GAME_{i}", "account_id": "GAMER", "amount": 1950 + i}
            for i in range(5)
        ]

        gaming = detect_threshold_gaming("GAMER", txns)
        assert gaming is True

    def test_detect_threshold_gaming_normal(self, sample_transaction):
        """Test threshold gaming with normal transactions."""
        txns = [sample_transaction for _ in range(5)]

        gaming = detect_threshold_gaming(sample_transaction["account_id"], txns)
        assert gaming is False

    def test_flag_egregious_items(self, sample_egregious_transaction):
        """Test flagging egregious items."""
        txns = [sample_egregious_transaction]

        flagged = flag_egregious_items(txns)

        assert len(flagged) == 1
        assert "ski" in flagged[0].get("egregious_keyword", "").lower() or \
               "snowbowl" in flagged[0].get("egregious_keyword", "").lower()

    def test_flag_egregious_piano(self):
        """Test flagging piano purchases."""
        txn = {
            "txn_id": "PIANO_TEST",
            "merchant_name": "Piano World",
            "amount": 15000,
            "description": "Grand piano purchase"
        }

        flagged = flag_egregious_items([txn])
        assert len(flagged) == 1
        assert "piano" in flagged[0].get("egregious_keyword", "")

    def test_flag_egregious_ninja(self):
        """Test flagging ninja gym."""
        txn = {
            "txn_id": "NINJA_TEST",
            "merchant_name": "Ninja Warrior Gym",
            "amount": 500,
            "description": "Ninja gym membership"
        }

        flagged = flag_egregious_items([txn])
        assert len(flagged) == 1
        assert "ninja" in flagged[0].get("egregious_keyword", "")
