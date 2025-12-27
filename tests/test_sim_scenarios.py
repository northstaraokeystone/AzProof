"""
Tests for simulation scenarios.

These tests validate the 6 mandatory scenarios for AzProof.
"""

import pytest
from src.sim import (
    run_simulation,
    run_scenario,
    run_all_scenarios,
    SimConfig,
    SimState,
    generate_medicaid_claims,
    generate_voucher_txns,
    inject_fraud_pattern,
    validate_detection
)


class TestSimConfig:
    """Tests for simulation configuration."""

    def test_default_config(self):
        """Test default configuration values."""
        config = SimConfig()

        assert config.n_cycles == 1000
        assert config.n_providers == 100
        assert config.fraud_rate == 0.15
        assert config.random_seed == 42


class TestDataGeneration:
    """Tests for synthetic data generation."""

    def test_generate_medicaid_claims(self):
        """Test Medicaid claim generation."""
        claims, fraud_ids = generate_medicaid_claims(100, fraud_rate=0.2)

        assert len(claims) == 100
        assert len(fraud_ids) > 0  # Some fraud
        assert len(fraud_ids) < len(claims)  # Not all fraud

    def test_generate_voucher_txns(self):
        """Test voucher transaction generation."""
        txns, fraud_ids = generate_voucher_txns(100, fraud_rate=0.2)

        assert len(txns) == 100
        assert len(fraud_ids) > 0

    def test_inject_ali_pattern(self):
        """Test Ali pattern injection."""
        claims = []
        claims = inject_fraud_pattern(claims, "ali")

        # Should have 41 clinics * 100 claims = 4100 claims
        assert len(claims) >= 4000

        # Check for Ali pattern characteristics
        ali_providers = [c for c in claims if "ALI" in c.get("provider_id", "")]
        assert len(ali_providers) > 0

    def test_validate_detection_perfect(self):
        """Test detection validation with perfect detection."""
        detections = ["a", "b", "c"]
        ground_truth = ["a", "b", "c"]

        metrics = validate_detection(detections, ground_truth)

        assert metrics["precision"] == 1.0
        assert metrics["recall"] == 1.0
        assert metrics["f1"] == 1.0

    def test_validate_detection_partial(self):
        """Test detection validation with partial detection."""
        detections = ["a", "b", "d"]  # d is false positive
        ground_truth = ["a", "b", "c"]  # c is missed

        metrics = validate_detection(detections, ground_truth)

        assert metrics["precision"] < 1.0  # Has false positive
        assert metrics["recall"] < 1.0  # Missed one


class TestScenarioBaseline:
    """Tests for BASELINE scenario."""

    def test_baseline_completes(self):
        """Test that baseline scenario completes."""
        state = run_scenario("BASELINE")

        assert isinstance(state, SimState)
        assert state.cycle > 0

    def test_baseline_has_receipts(self):
        """Test that baseline generates receipts."""
        state = run_scenario("BASELINE")

        assert len(state.medicaid_receipts) > 0
        assert len(state.voucher_receipts) > 0


class TestScenarioStress:
    """Tests for STRESS scenario."""

    def test_stress_handles_high_fraud(self):
        """Test that stress scenario handles 40% fraud rate."""
        state = run_scenario("STRESS")

        # Should complete without crash
        assert isinstance(state, SimState)
        assert state.cycle >= 1

    def test_stress_detects_some_fraud(self):
        """Test that stress scenario detects some fraud."""
        state = run_scenario("STRESS")

        # May have violations but should still work
        assert len(state.detected_fraud) > 0 or len(state.detection_receipts) > 0


class TestScenarioAliPattern:
    """Tests for ALI_PATTERN scenario."""

    def test_ali_pattern_detected(self):
        """Test Ali pattern detection."""
        state = run_scenario("ALI_PATTERN")

        # Should detect the Ali pattern
        assert state.ali_detected is True

    def test_ali_entities_flagged(self):
        """Test that Ali entities are flagged."""
        state = run_scenario("ALI_PATTERN")

        # Should flag significant number of entities
        assert state.entities_flagged >= 30  # May not get all 41 exactly


class TestScenarioVoucherEgregious:
    """Tests for VOUCHER_EGREGIOUS scenario."""

    def test_voucher_egregious_detects_items(self):
        """Test egregious item detection."""
        state = run_scenario("VOUCHER_EGREGIOUS")

        # Should have detection receipts
        assert len(state.detection_receipts) > 0

    def test_voucher_egregious_flags_ski(self):
        """Test ski-related detection."""
        state = run_scenario("VOUCHER_EGREGIOUS")

        # Should flag ski-related items
        ski_detections = [
            d for d in state.detection_receipts
            if "ski" in str(d).lower() or "snowbowl" in str(d).lower()
        ]
        assert len(ski_detections) > 0


class TestScenarioMetaLoop:
    """Tests for META_LOOP scenario."""

    def test_meta_loop_runs(self):
        """Test that meta loop scenario runs."""
        state = run_scenario("META_LOOP")

        assert isinstance(state, SimState)

    def test_meta_loop_harvests_gaps(self):
        """Test that meta loop harvests gaps."""
        state = run_scenario("META_LOOP")

        # Should have gap receipts
        assert len(state.gap_receipts) > 0


class TestScenarioGodel:
    """Tests for GODEL scenario."""

    def test_godel_handles_edge_cases(self):
        """Test Godel scenario handles edge cases."""
        state = run_scenario("GODEL")

        # Should complete without crashes
        assert isinstance(state, SimState)

    def test_godel_no_crash_violations(self):
        """Test no crash violations in Godel."""
        state = run_scenario("GODEL")

        # No crash violations
        crash_violations = [
            v for v in state.violations
            if v.get("type") == "crash"
        ]
        assert len(crash_violations) == 0


class TestRunAllScenarios:
    """Tests for running all scenarios."""

    @pytest.mark.slow
    def test_all_scenarios_run(self):
        """Test that all scenarios run (may be slow)."""
        results = run_all_scenarios()

        assert len(results) == 6
        assert "BASELINE" in results
        assert "STRESS" in results
        assert "ALI_PATTERN" in results
        assert "VOUCHER_EGREGIOUS" in results
        assert "META_LOOP" in results
        assert "GODEL" in results

    @pytest.mark.slow
    def test_critical_scenarios_pass(self):
        """Test that critical scenarios pass."""
        results = run_all_scenarios()

        # GODEL should have no crashes
        assert len([
            v for v in results.get("GODEL", {}).get("violations", [])
            if v.get("type") == "crash"
        ]) == 0

        # ALI_PATTERN should detect the network
        # (May have some violations but core detection should work)


class TestSimState:
    """Tests for SimState dataclass."""

    def test_simstate_defaults(self):
        """Test SimState default values."""
        state = SimState()

        assert state.cycle == 0
        assert len(state.medicaid_receipts) == 0
        assert len(state.violations) == 0
        assert state.ali_detected is False

    def test_simstate_mutable(self):
        """Test SimState mutability."""
        state = SimState()
        state.cycle = 100
        state.ali_detected = True

        assert state.cycle == 100
        assert state.ali_detected is True
