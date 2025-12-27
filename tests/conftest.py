"""
Pytest configuration and fixtures for AzProof tests.
"""

import os
import tempfile
import pytest

# Set up test environment
os.environ.setdefault("AZPROOF_TEST", "1")


@pytest.fixture
def temp_ledger():
    """Create a temporary ledger file for testing."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        ledger_path = f.name

    yield ledger_path

    # Cleanup
    if os.path.exists(ledger_path):
        os.unlink(ledger_path)


@pytest.fixture
def sample_claim():
    """Sample Medicaid claim for testing."""
    return {
        "claim_id": "TEST_CLM_001",
        "provider_id": "NPI1234567890",
        "provider_name": "Test Provider LLC",
        "patient_id": "PAT_TEST_001",
        "patient_tribal_affiliation": None,
        "service_type": "behavioral",
        "service_date": "2024-01-15T10:00:00Z",
        "billed_amount": 500.00,
        "paid_amount": 450.00,
        "facility_address": "123 Test St, Phoenix, AZ",
        "facility_type": "outpatient"
    }


@pytest.fixture
def sample_aihp_claim():
    """Sample AIHP claim for testing."""
    return {
        "claim_id": "TEST_CLM_AIHP_001",
        "provider_id": "NPI9876543210",
        "provider_name": "Native Health Services LLC",
        "patient_id": "PAT_AIHP_001",
        "patient_tribal_affiliation": "Navajo Nation",
        "service_type": "addiction",
        "service_date": "2024-01-15T10:00:00Z",
        "billed_amount": 5000.00,
        "paid_amount": 4500.00,
        "facility_address": "456 Reservation Rd, Phoenix, AZ",
        "facility_type": "sober_living"
    }


@pytest.fixture
def sample_transaction():
    """Sample ESA voucher transaction for testing."""
    return {
        "txn_id": "TXN_TEST_001",
        "account_id": "ESA_TEST_001",
        "merchant_id": "MER_TEST_001",
        "merchant_name": "ABC Learning Center",
        "merchant_category_code": "8299",
        "amount": 150.00,
        "txn_date": "2024-01-15T10:00:00Z",
        "description": "Curriculum materials"
    }


@pytest.fixture
def sample_egregious_transaction():
    """Sample non-educational ESA transaction for testing."""
    return {
        "txn_id": "TXN_EGREGIOUS_001",
        "account_id": "ESA_TEST_002",
        "merchant_id": "MER_SNOWBOWL",
        "merchant_name": "Arizona Snowbowl",
        "merchant_category_code": "7999",
        "amount": 1695.00,
        "txn_date": "2024-01-15T10:00:00Z",
        "description": "Ski passes and equipment"
    }


@pytest.fixture
def sample_providers():
    """Sample provider list for shell detection testing."""
    shared_principal = "SHARED_OWNER"
    return [
        {
            "provider_id": f"SHELL_{i}",
            "provider_name": f"Shell Clinic {i} LLC",
            "principals": [shared_principal, f"OFFICER_{i}"],
            "total_billed": 1000000 + i * 100000,
            "registration_date": "2024-01-01T00:00:00Z"
        }
        for i in range(10)
    ]
