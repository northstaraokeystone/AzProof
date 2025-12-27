"""
Tests for Fiscal module.
"""

import pytest
from src.fiscal.revenue import (
    ingest_revenue_data,
    compute_yoy_change,
    attribute_policy_impact
)
from src.fiscal.policy import (
    ingest_policy_change,
    compute_policy_cost,
    detect_budget_stress
)
from src.fiscal.deficit import (
    compute_deficit,
    attribute_deficit,
    project_deficit
)
from src.core import AZ_DEFICIT


class TestRevenue:
    """Tests for revenue tracking."""

    def test_ingest_revenue_data(self):
        """Test revenue data ingestion."""
        data = {
            "period": "2024",
            "amount": 15000000000,
            "prior_amount": 14500000000
        }

        receipt = ingest_revenue_data(data, "income_tax")

        assert receipt["receipt_type"] == "revenue_ingest"
        assert receipt["source"] == "income_tax"
        assert receipt["amount"] == data["amount"]

    def test_compute_yoy_change(self):
        """Test year-over-year change calculation."""
        current = {"income_tax": 10000000, "sales_tax": 5000000}
        prior = {"income_tax": 11000000, "sales_tax": 4500000}

        changes = compute_yoy_change(current, prior)

        assert "by_category" in changes
        assert "total" in changes
        assert changes["by_category"]["income_tax"]["absolute_change"] == -1000000
        assert changes["by_category"]["sales_tax"]["absolute_change"] == 500000

    def test_attribute_policy_impact_known(self):
        """Test policy impact attribution for known policy."""
        attribution = attribute_policy_impact(-700000000, "flat_tax_2.5")

        assert attribution["policy"] == "flat_tax_2.5"
        assert attribution["estimated_impact"] == -700000000
        assert attribution["confidence"] > 0.5

    def test_attribute_policy_impact_unknown(self):
        """Test policy impact attribution for unknown policy."""
        attribution = attribute_policy_impact(-100000, "unknown_policy")

        assert attribution["policy"] == "unknown_policy"
        assert attribution["confidence"] < 0.5


class TestPolicy:
    """Tests for policy tracking."""

    def test_ingest_policy_change(self):
        """Test policy change ingestion."""
        policy = {
            "policy_id": "TEST_POLICY",
            "name": "Test Policy",
            "effective_date": "2024-01-01",
            "projected_cost": 1000000
        }

        receipt = ingest_policy_change(policy)

        assert receipt["receipt_type"] == "policy_ingest"
        assert receipt["policy_id"] == "TEST_POLICY"

    def test_compute_policy_cost(self):
        """Test policy cost computation."""
        fiscal_data = [
            {"policy_id": "TEST_POLICY", "amount": 500000},
            {"policy_id": "TEST_POLICY", "amount": 600000}
        ]

        cost = compute_policy_cost("TEST_POLICY", fiscal_data)
        assert cost == 1100000

    def test_detect_budget_stress_below(self):
        """Test budget stress detection below threshold."""
        stressed = detect_budget_stress(1000000000)  # $1B
        assert stressed is False

    def test_detect_budget_stress_above(self):
        """Test budget stress detection above threshold."""
        stressed = detect_budget_stress(AZ_DEFICIT)  # $1.4B
        assert stressed is True


class TestDeficit:
    """Tests for deficit analysis."""

    def test_compute_deficit(self):
        """Test deficit computation."""
        deficit = compute_deficit(10000000, 12000000)
        assert deficit == -2000000

    def test_compute_deficit_surplus(self):
        """Test surplus computation."""
        surplus = compute_deficit(12000000, 10000000)
        assert surplus == 2000000

    def test_attribute_deficit_known_factors(self):
        """Test deficit attribution with known factors."""
        attribution = attribute_deficit(
            -1400000000,  # $1.4B deficit
            ["flat_tax", "esa_voucher"]
        )

        assert attribution["deficit"] == -1400000000
        assert len(attribution["attributions"]) == 2
        assert attribution["total_explained"] > 0

    def test_project_deficit_flat(self):
        """Test deficit projection with no trend."""
        projected, projections = project_deficit(-1000000000, [], 5)

        assert projected == -1000000000
        assert len(projections) == 5

    def test_project_deficit_with_trend(self):
        """Test deficit projection with trend."""
        trend = [-900000000, -1000000000, -1100000000]
        projected, projections = project_deficit(-1200000000, trend, 5)

        assert len(projections) == 5
        # Should project increasing deficit based on trend
        assert projected < -1200000000
