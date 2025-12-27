"""
Tests for core module.
"""

import pytest
from src.core import (
    dual_hash,
    emit_receipt,
    merkle,
    StopRule,
    validate_receipt,
    get_risk_level,
    TENANT_ID
)


class TestDualHash:
    """Tests for dual_hash function."""

    def test_dual_hash_string(self):
        """Test dual_hash with string input."""
        result = dual_hash("test")
        assert ":" in result
        parts = result.split(":")
        assert len(parts) == 2
        assert len(parts[0]) == 64  # SHA256 hex
        assert len(parts[1]) == 64  # BLAKE3 hex

    def test_dual_hash_bytes(self):
        """Test dual_hash with bytes input."""
        result = dual_hash(b"test")
        assert ":" in result
        parts = result.split(":")
        assert len(parts) == 2

    def test_dual_hash_deterministic(self):
        """Test that dual_hash is deterministic."""
        result1 = dual_hash("test")
        result2 = dual_hash("test")
        assert result1 == result2

    def test_dual_hash_different_inputs(self):
        """Test that different inputs produce different hashes."""
        result1 = dual_hash("test1")
        result2 = dual_hash("test2")
        assert result1 != result2

    def test_dual_hash_empty_string(self):
        """Test dual_hash with empty string."""
        result = dual_hash("")
        assert ":" in result
        parts = result.split(":")
        assert len(parts) == 2


class TestEmitReceipt:
    """Tests for emit_receipt function."""

    def test_emit_receipt_basic(self):
        """Test basic receipt emission."""
        receipt = emit_receipt("test", {"key": "value"})

        assert receipt["receipt_type"] == "test"
        assert receipt["tenant_id"] == TENANT_ID
        assert "ts" in receipt
        assert "payload_hash" in receipt
        assert receipt["key"] == "value"

    def test_emit_receipt_has_valid_hash(self):
        """Test that receipt has valid dual hash."""
        receipt = emit_receipt("test", {"key": "value"})

        assert ":" in receipt["payload_hash"]
        parts = receipt["payload_hash"].split(":")
        assert len(parts) == 2
        assert len(parts[0]) == 64
        assert len(parts[1]) == 64

    def test_emit_receipt_custom_tenant(self):
        """Test receipt with custom tenant."""
        receipt = emit_receipt("test", {"key": "value"}, tenant_id="custom")

        assert receipt["tenant_id"] == "custom"


class TestMerkle:
    """Tests for merkle function."""

    def test_merkle_empty(self):
        """Test merkle with empty list."""
        result = merkle([])
        assert ":" in result

    def test_merkle_single(self):
        """Test merkle with single item."""
        result = merkle(["item1"])
        assert ":" in result

    def test_merkle_multiple(self):
        """Test merkle with multiple items."""
        result = merkle(["item1", "item2", "item3"])
        assert ":" in result

    def test_merkle_deterministic(self):
        """Test that merkle is deterministic."""
        items = ["a", "b", "c"]
        result1 = merkle(items)
        result2 = merkle(items)
        assert result1 == result2

    def test_merkle_order_matters(self):
        """Test that order affects merkle root."""
        result1 = merkle(["a", "b"])
        result2 = merkle(["b", "a"])
        assert result1 != result2

    def test_merkle_with_dicts(self):
        """Test merkle with dict items."""
        items = [{"key": "value1"}, {"key": "value2"}]
        result = merkle(items)
        assert ":" in result


class TestStopRule:
    """Tests for StopRule exception."""

    def test_stoprule_creation(self):
        """Test StopRule exception creation."""
        exc = StopRule("test_rule", "Test message")
        assert exc.rule_name == "test_rule"
        assert exc.message == "Test message"
        assert "STOPRULE" in str(exc)

    def test_stoprule_with_context(self):
        """Test StopRule with context."""
        exc = StopRule("test_rule", "Test message", {"extra": "data"})
        assert exc.context == {"extra": "data"}

    def test_stoprule_raises(self):
        """Test that StopRule can be raised and caught."""
        with pytest.raises(StopRule) as exc_info:
            raise StopRule("test_rule", "Test message")

        assert exc_info.value.rule_name == "test_rule"


class TestValidateReceipt:
    """Tests for validate_receipt function."""

    def test_validate_valid_receipt(self):
        """Test validation of valid receipt."""
        receipt = emit_receipt("test", {"key": "value"})
        valid, reason = validate_receipt(receipt)
        assert valid is True
        assert reason == "valid"

    def test_validate_missing_field(self):
        """Test validation with missing field."""
        receipt = {"receipt_type": "test", "ts": "2024-01-01T00:00:00Z"}
        valid, reason = validate_receipt(receipt)
        assert valid is False
        assert "Missing" in reason

    def test_validate_invalid_tenant(self):
        """Test validation with invalid tenant."""
        receipt = {
            "receipt_type": "test",
            "ts": "2024-01-01T00:00:00Z",
            "tenant_id": "wrong",
            "payload_hash": "a" * 64 + ":" + "b" * 64
        }
        valid, reason = validate_receipt(receipt)
        assert valid is False
        assert "tenant_id" in reason


class TestGetRiskLevel:
    """Tests for get_risk_level function."""

    def test_risk_level_low(self):
        """Test low risk level."""
        assert get_risk_level(0.1) == "low"
        assert get_risk_level(0.19) == "low"

    def test_risk_level_medium(self):
        """Test medium risk level."""
        assert get_risk_level(0.3) == "medium"
        assert get_risk_level(0.49) == "medium"

    def test_risk_level_high(self):
        """Test high risk level."""
        assert get_risk_level(0.6) == "high"
        assert get_risk_level(0.79) == "high"

    def test_risk_level_critical(self):
        """Test critical risk level."""
        assert get_risk_level(0.8) == "critical"
        assert get_risk_level(1.0) == "critical"
