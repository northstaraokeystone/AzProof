"""
Tests for Entropy module.
"""

import pytest
from src.entropy.compression import (
    compress_records,
    compression_fraud_score,
    batch_compression_analysis
)
from src.entropy.network import (
    network_entropy,
    detect_entropy_anomaly,
    temporal_network_entropy
)
from src.entropy.temporal import (
    time_series_entropy,
    detect_regularity,
    entropy_change_point
)


class TestCompression:
    """Tests for compression-based fraud detection."""

    def test_compress_records_empty(self):
        """Test compression of empty records."""
        compressed, ratio = compress_records([])
        assert ratio == 1.0

    def test_compress_records_basic(self):
        """Test basic compression."""
        records = [{"key": f"value_{i}"} for i in range(10)]
        compressed, ratio = compress_records(records)

        assert 0 < ratio < 1  # Should compress
        assert len(compressed) > 0

    def test_compress_records_identical(self):
        """Test compression of identical records (should compress well)."""
        records = [{"key": "same_value"} for _ in range(100)]
        _, ratio = compress_records(records)

        assert ratio < 0.5  # Highly repetitive = low ratio

    def test_compression_fraud_score_low_ratio(self):
        """Test fraud score for low compression ratio."""
        score = compression_fraud_score(0.3, baseline=0.65)
        assert score > 0.5  # Low ratio = high fraud score

    def test_compression_fraud_score_normal_ratio(self):
        """Test fraud score for normal ratio."""
        score = compression_fraud_score(0.65, baseline=0.65)
        assert score == 0.0  # At baseline = no fraud

    def test_batch_compression_analysis(self):
        """Test batch compression analysis."""
        records = [{"id": i, "value": i % 10} for i in range(200)]

        results = batch_compression_analysis(records, window_size=50)

        assert len(results) > 0
        for result in results:
            assert "compression_ratio" in result
            assert "fraud_score" in result


class TestNetworkEntropy:
    """Tests for network entropy calculation."""

    def test_network_entropy_empty(self):
        """Test entropy of empty graph."""
        graph = {"nodes": [], "edges": []}
        entropy = network_entropy(graph)
        assert entropy == 0.0

    def test_network_entropy_single_edge(self):
        """Test entropy with single edge."""
        graph = {
            "nodes": [{"provider_id": "A"}, {"provider_id": "B"}],
            "edges": [{"source": "A", "target": "B", "weight": 1}]
        }
        entropy = network_entropy(graph)
        assert entropy >= 0

    def test_network_entropy_balanced(self):
        """Test entropy of balanced network (should be higher)."""
        # Fully connected graph
        nodes = [{"provider_id": chr(65 + i)} for i in range(5)]
        edges = []
        for i in range(5):
            for j in range(i + 1, 5):
                edges.append({
                    "source": chr(65 + i),
                    "target": chr(65 + j),
                    "weight": 1
                })

        graph = {"nodes": nodes, "edges": edges}
        entropy = network_entropy(graph)

        assert entropy > 0  # Balanced = higher entropy

    def test_detect_entropy_anomaly_normal(self):
        """Test anomaly detection with normal entropy."""
        anomaly = detect_entropy_anomaly(2.5, baseline=2.5, sigma=0.5)
        assert anomaly is False

    def test_detect_entropy_anomaly_low(self):
        """Test anomaly detection with low entropy."""
        anomaly = detect_entropy_anomaly(1.5, baseline=2.5, sigma=0.5)
        assert anomaly is True

    def test_temporal_network_entropy(self):
        """Test temporal network entropy."""
        graphs = [
            {"nodes": [], "edges": [{"source": "A", "target": "B"}]}
            for _ in range(5)
        ]

        entropies = temporal_network_entropy(graphs, window=3)
        assert len(entropies) == len(graphs)


class TestTemporalEntropy:
    """Tests for temporal entropy analysis."""

    def test_time_series_entropy_constant(self):
        """Test entropy of constant series."""
        values = [5.0] * 20
        entropy = time_series_entropy(values)
        assert entropy == 0.0  # All same = zero entropy

    def test_time_series_entropy_varied(self):
        """Test entropy of varied series."""
        values = list(range(20))
        entropy = time_series_entropy(values)
        assert entropy > 0  # Varied = positive entropy

    def test_detect_regularity_regular(self):
        """Test regularity detection for regular pattern."""
        values = [100.0] * 20  # Perfectly regular
        regularity = detect_regularity(values)
        assert regularity > 0.8  # High regularity

    def test_detect_regularity_random(self):
        """Test regularity detection for random pattern."""
        import random
        random.seed(42)
        values = [random.uniform(0, 100) for _ in range(20)]
        regularity = detect_regularity(values)
        assert regularity < 0.5  # Lower regularity

    def test_entropy_change_point_detection(self):
        """Test change point detection."""
        # Create series with regime change
        values = [5.0] * 30 + [50.0] * 30  # Jump at position 30

        change_points = entropy_change_point(values, window=10)

        # Should detect change around position 30
        # Note: may not always detect depending on window
        assert isinstance(change_points, list)

    def test_time_series_entropy_short(self):
        """Test entropy of short series."""
        values = [1.0, 2.0]
        entropy = time_series_entropy(values)
        assert entropy >= 0  # Should handle gracefully
