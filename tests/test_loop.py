"""
Tests for Loop module.
"""

import pytest
from src.loop.sense import sense_receipts, summarize_activity, filter_by_type
from src.loop.harvest import harvest_gaps, rank_gaps, identify_patterns, emit_gap
from src.loop.genesis import synthesize_helper, validate_blueprint, estimate_savings
from src.loop.gate import (
    calculate_risk,
    request_approval,
    check_approval,
    auto_approve,
    clear_approvals
)
from src.loop.effectiveness import (
    register_helper,
    record_execution,
    measure_effectiveness,
    track_helper,
    retire_helper,
    clear_helpers
)
from src.loop.cycle import run_cycle, get_cycle_count, reset_cycle_count


class TestSense:
    """Tests for receipt sensing."""

    def test_sense_receipts(self):
        """Test sensing recent receipts."""
        receipts = sense_receipts(since_minutes=60)
        # May be empty in test environment
        assert isinstance(receipts, list)

    def test_summarize_activity(self):
        """Test activity summarization."""
        summary = summarize_activity(minutes=60)

        assert "period_minutes" in summary
        assert "total_receipts" in summary
        assert "by_type" in summary

    def test_filter_by_type(self):
        """Test filtering receipts by type."""
        receipts = [
            {"receipt_type": "medicaid_ingest"},
            {"receipt_type": "voucher_ingest"},
            {"receipt_type": "medicaid_ingest"}
        ]

        filtered = filter_by_type(receipts, "medicaid_ingest")
        assert len(filtered) == 2


class TestHarvest:
    """Tests for gap harvesting."""

    def test_emit_gap(self):
        """Test gap emission."""
        receipt = emit_gap(
            problem_type="test_problem",
            domain="medicaid",
            time_to_resolve_ms=5000,
            resolution_steps=["step1", "step2"],
            could_automate=True,
            automation_confidence=0.8
        )

        assert receipt["receipt_type"] == "gap"
        assert receipt["problem_type"] == "test_problem"

    def test_rank_gaps(self):
        """Test gap ranking."""
        gaps = [
            {
                "problem_type": "frequent",
                "time_to_resolve_ms": 10000
            },
            {
                "problem_type": "frequent",
                "time_to_resolve_ms": 15000
            },
            {
                "problem_type": "rare",
                "time_to_resolve_ms": 5000
            }
        ]

        ranked = rank_gaps(gaps)

        assert len(ranked) == 2  # Two problem types
        assert ranked[0]["problem_type"] == "frequent"  # Higher score

    def test_identify_patterns(self):
        """Test pattern identification."""
        gaps = [
            {"problem_type": "common", "domain": "medicaid", "resolution_steps": ["fix"], "could_automate": True}
            for _ in range(5)
        ]

        patterns = identify_patterns(gaps, min_count=3)

        assert len(patterns) >= 1
        assert patterns[0]["count"] >= 3


class TestGenesis:
    """Tests for helper blueprint creation."""

    def test_synthesize_helper(self):
        """Test helper synthesis from pattern."""
        pattern = {
            "problem_type": "test_problem",
            "domain": "medicaid",
            "count": 10,
            "resolution_steps": ["step1", "step2"],
            "automation_likelihood": 0.8,
            "avg_resolution_ms": 60000
        }

        blueprint = synthesize_helper(pattern)

        assert "blueprint_id" in blueprint
        assert "trigger" in blueprint
        assert "action" in blueprint
        assert blueprint["status"] == "proposed"

    def test_validate_blueprint(self):
        """Test blueprint validation."""
        pattern = {
            "problem_type": "test_problem",
            "domain": "medicaid",
            "count": 5,
            "resolution_steps": ["fix"],
            "automation_likelihood": 0.8
        }

        blueprint = synthesize_helper(pattern)
        historical = [
            {"problem_type": "test_problem", "domain": "medicaid", "automation_confidence": 0.9}
            for _ in range(5)
        ]

        validated = validate_blueprint(blueprint, historical)

        assert "validation" in validated
        assert validated["validation"]["backtested"] == 5

    def test_estimate_savings(self):
        """Test savings estimation."""
        blueprint = {
            "origin": {
                "gap_count": 20,
                "total_hours_saved": 10
            },
            "validation": {
                "success_rate": 0.9
            }
        }

        savings = estimate_savings(blueprint)
        assert savings > 0


class TestGate:
    """Tests for HITL approval management."""

    def setup_method(self):
        """Clear approvals before each test."""
        clear_approvals()

    def test_calculate_risk_low(self):
        """Test low risk calculation."""
        action = {
            "action": "alert:operator",
            "validation": {"success_rate": 0.95},
            "origin": {"gap_count": 200}
        }

        risk = calculate_risk(action)
        assert risk < 0.5

    def test_calculate_risk_high(self):
        """Test high risk calculation."""
        action = {
            "action": "delete_records",
            "validation": {"success_rate": 0.4},
            "origin": {"gap_count": 2}
        }

        risk = calculate_risk(action)
        assert risk > 0.5

    def test_request_approval(self):
        """Test approval request."""
        blueprint = {"blueprint_id": "TEST_BP", "risk_score": 0.3}

        approval_id = request_approval(blueprint)

        assert approval_id
        assert check_approval(approval_id) == "pending"

    def test_auto_approve_low_risk(self):
        """Test auto-approval for low risk."""
        blueprint = {
            "blueprint_id": "LOW_RISK",
            "action": "alert",
            "validation": {"success_rate": 0.95},
            "origin": {"gap_count": 100}
        }

        approved = auto_approve(blueprint)
        assert approved is True


class TestEffectiveness:
    """Tests for helper effectiveness measurement."""

    def setup_method(self):
        """Clear helpers before each test."""
        clear_helpers()

    def test_register_helper(self):
        """Test helper registration."""
        blueprint = {"blueprint_id": "TEST_HELPER"}

        helper_id = register_helper(blueprint)
        assert helper_id == "TEST_HELPER"

    def test_record_execution(self):
        """Test recording execution."""
        blueprint = {"blueprint_id": "TEST_HELPER"}
        register_helper(blueprint)

        record_execution("TEST_HELPER", success=True, time_saved_ms=5000)

        metrics = track_helper("TEST_HELPER")
        assert metrics["executions"] == 1
        assert metrics["successes"] == 1

    def test_measure_effectiveness(self):
        """Test effectiveness measurement."""
        blueprint = {"blueprint_id": "TEST_HELPER"}
        register_helper(blueprint)

        for _ in range(10):
            record_execution("TEST_HELPER", success=True, time_saved_ms=1000)

        effectiveness = measure_effectiveness("TEST_HELPER")
        assert effectiveness > 0

    def test_retire_helper(self):
        """Test helper retirement."""
        blueprint = {"blueprint_id": "RETIRE_TEST"}
        register_helper(blueprint)

        result = retire_helper("RETIRE_TEST", "test retirement")

        assert result["status"] == "retired"
        assert result["retirement_reason"] == "test retirement"


class TestCycle:
    """Tests for main loop cycle."""

    def setup_method(self):
        """Reset cycle count before each test."""
        reset_cycle_count()
        clear_helpers()
        clear_approvals()

    def test_run_cycle(self):
        """Test running a single cycle."""
        result = run_cycle(
            sense_minutes=60,
            harvest_days=7,
            min_pattern_count=3
        )

        assert result["receipt_type"] == "loop_cycle"
        assert "cycle_id" in result
        assert "receipts_processed" in result

    def test_cycle_count_increments(self):
        """Test that cycle count increments."""
        initial = get_cycle_count()

        run_cycle()

        assert get_cycle_count() == initial + 1

    def test_reset_cycle_count(self):
        """Test cycle count reset."""
        run_cycle()
        run_cycle()

        reset_cycle_count()

        assert get_cycle_count() == 0
