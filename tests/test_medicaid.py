"""
Tests for Medicaid module.
"""

import pytest
from src.medicaid.ingest import ingest_claim, batch_ingest, validate_claim
from src.medicaid.network import (
    build_provider_graph,
    detect_clusters,
    compute_network_entropy,
    flag_hub_providers
)
from src.medicaid.aihp import (
    flag_aihp_claims,
    compute_aihp_concentration,
    detect_geographic_mismatch
)
from src.medicaid.billing import (
    compute_billing_velocity,
    detect_impossible_volume,
    compression_ratio_billing,
    detect_upcoding
)
from src.medicaid.shell import (
    extract_principals,
    build_ownership_graph,
    detect_shell_clusters
)


class TestClaimValidation:
    """Tests for claim validation."""

    def test_validate_valid_claim(self, sample_claim):
        """Test validation of valid claim."""
        valid, reason = validate_claim(sample_claim)
        assert valid is True
        assert reason == "valid"

    def test_validate_missing_claim_id(self):
        """Test validation with missing claim_id."""
        claim = {"provider_id": "NPI123", "billed_amount": 100}
        valid, reason = validate_claim(claim)
        assert valid is False
        assert "claim_id" in reason

    def test_validate_missing_provider_id(self):
        """Test validation with missing provider_id."""
        claim = {"claim_id": "CLM123", "billed_amount": 100}
        valid, reason = validate_claim(claim)
        assert valid is False
        assert "provider_id" in reason

    def test_validate_negative_amount(self):
        """Test validation with negative amount."""
        claim = {"claim_id": "CLM123", "provider_id": "NPI123", "billed_amount": -100}
        valid, reason = validate_claim(claim)
        assert valid is False
        assert "negative" in reason


class TestClaimIngestion:
    """Tests for claim ingestion."""

    def test_ingest_claim(self, sample_claim):
        """Test claim ingestion."""
        receipt = ingest_claim(sample_claim)

        assert receipt["receipt_type"] == "medicaid_ingest"
        assert "claim_hash" in receipt
        assert receipt["provider_id"] == sample_claim["provider_id"]
        assert receipt["aihp_flag"] is False

    def test_ingest_aihp_claim(self, sample_aihp_claim):
        """Test AIHP claim ingestion."""
        receipt = ingest_claim(sample_aihp_claim)

        assert receipt["aihp_flag"] is True

    def test_batch_ingest(self, sample_claim):
        """Test batch ingestion."""
        claims = [sample_claim, sample_claim.copy()]
        claims[1]["claim_id"] = "CLM_002"

        receipt = batch_ingest(claims)

        assert receipt["receipt_type"] == "medicaid_batch_ingest"
        assert receipt["claim_count"] == 2
        assert "merkle_root" in receipt


class TestProviderNetwork:
    """Tests for provider network analysis."""

    def test_build_provider_graph(self, sample_claim):
        """Test building provider graph."""
        receipts = [ingest_claim(sample_claim)]

        graph = build_provider_graph(receipts)

        assert "nodes" in graph
        assert "edges" in graph
        assert graph["n_providers"] >= 1

    def test_detect_clusters_empty(self):
        """Test cluster detection with empty graph."""
        graph = {"nodes": [], "edges": []}
        clusters = detect_clusters(graph)
        assert clusters == []

    def test_compute_network_entropy_empty(self):
        """Test entropy with empty graph."""
        graph = {"nodes": [], "edges": []}
        entropy = compute_network_entropy(graph)
        assert entropy == 0.0

    def test_compute_network_entropy_with_edges(self):
        """Test entropy with edges."""
        graph = {
            "nodes": [{"provider_id": "A"}, {"provider_id": "B"}, {"provider_id": "C"}],
            "edges": [
                {"source": "A", "target": "B", "weight": 1},
                {"source": "B", "target": "C", "weight": 1}
            ]
        }
        entropy = compute_network_entropy(graph)
        assert entropy > 0


class TestAIHPDetection:
    """Tests for AIHP exploitation detection."""

    def test_flag_aihp_claims(self, sample_claim, sample_aihp_claim):
        """Test flagging AIHP claims."""
        receipts = [
            ingest_claim(sample_claim),
            ingest_claim(sample_aihp_claim)
        ]

        aihp_claims = flag_aihp_claims(receipts)
        assert len(aihp_claims) == 1
        assert aihp_claims[0]["aihp_flag"] is True

    def test_compute_aihp_concentration(self, sample_aihp_claim):
        """Test AIHP concentration calculation."""
        receipts = [ingest_claim(sample_aihp_claim) for _ in range(5)]

        concentration = compute_aihp_concentration(
            sample_aihp_claim["provider_id"],
            receipts
        )
        assert concentration == 1.0  # All AIHP

    def test_detect_geographic_mismatch(self, sample_aihp_claim):
        """Test geographic mismatch detection."""
        sample_aihp_claim["facility_address"] = "123 Main St, Phoenix, AZ"
        claims = [sample_aihp_claim]

        mismatches = detect_geographic_mismatch(claims)
        assert len(mismatches) == 1  # Phoenix is urban


class TestBillingAnomalies:
    """Tests for billing anomaly detection."""

    def test_compression_ratio_billing(self, sample_claim):
        """Test billing compression ratio."""
        claims = [sample_claim for _ in range(10)]
        ratio = compression_ratio_billing(claims)

        assert 0 < ratio < 1

    def test_compression_ratio_empty(self):
        """Test compression ratio with empty list."""
        ratio = compression_ratio_billing([])
        assert ratio == 1.0

    def test_detect_upcoding(self, sample_claim):
        """Test upcoding detection."""
        # Create claims with all high amounts
        claims = []
        for i in range(20):
            claim = sample_claim.copy()
            claim["claim_id"] = f"CLM_{i}"
            claim["provider_id"] = "NPI_UPCODER"
            claim["billed_amount"] = 9000 + i * 10  # All high amounts
            claims.append(claim)

        upcoding = detect_upcoding(claims)
        assert len(upcoding) >= 1


class TestShellDetection:
    """Tests for shell LLC detection."""

    def test_extract_principals(self):
        """Test principal extraction."""
        provider = {
            "provider_id": "TEST",
            "principals": ["OWNER_A", "OWNER_B"],
            "registered_agent": "AGENT_1"
        }
        principals = extract_principals(provider)

        assert len(principals) >= 2
        assert "OWNER_A" in principals or "owner_a" in [p.lower() for p in principals]

    def test_build_ownership_graph(self, sample_providers):
        """Test building ownership graph."""
        graph = build_ownership_graph(sample_providers)

        assert "nodes" in graph
        assert "edges" in graph
        assert graph["n_providers"] == len(sample_providers)

    def test_detect_shell_clusters(self, sample_providers):
        """Test shell cluster detection."""
        graph = build_ownership_graph(sample_providers)
        clusters = detect_shell_clusters(graph, min_shared=1)

        # Should detect cluster since all share SHARED_OWNER
        assert len(clusters) >= 1
        if clusters:
            assert clusters[0]["n_entities"] >= 5
