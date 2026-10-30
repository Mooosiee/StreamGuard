from consumer import validate_transaction


def test_validate_valid_transaction():
    valid_txn = {
        "account_id": "acc_001",
        "amount": 1250.50,
        "location": "Lahore",
        "merchant": "Daraz",
        "timestamp": "2026-10-30T10:00:00",
    }
    is_valid, error = validate_transaction(valid_txn)
    assert is_valid is True
    assert error is None


def test_validate_missing_field():
    invalid_txn = {
        "account_id": "acc_001",
        "amount": 1250.50,
        "merchant": "Daraz",
        # missing location and timestamp
    }
    is_valid, error = validate_transaction(invalid_txn)
    assert is_valid is False
    assert "Missing field" in error


def test_validate_non_numeric_amount():
    invalid_txn = {
        "account_id": "acc_001",
        "amount": "one thousand",
        "location": "Karachi",
        "merchant": "Foodpanda",
        "timestamp": "2026-10-30T10:00:00",
    }
    is_valid, error = validate_transaction(invalid_txn)
    assert is_valid is False
    assert "Amount must be a number" in error


def test_validate_negative_amount():
    invalid_txn = {
        "account_id": "acc_001",
        "amount": -50.00,
        "location": "Karachi",
        "merchant": "Foodpanda",
        "timestamp": "2026-10-30T10:00:00",
    }
    is_valid, error = validate_transaction(invalid_txn)
    assert is_valid is False
    assert "Amount cannot be negative" in error


def test_validate_malformed_timestamp():
    invalid_txn = {
        "account_id": "acc_001",
        "amount": 250.00,
        "location": "Islamabad",
        "merchant": "Careem",
        "timestamp": "yesterday afternoon",
    }
    is_valid, error = validate_transaction(invalid_txn)
    assert is_valid is False
    assert "Invalid timestamp" in error


def test_validate_non_dict_payload():
    is_valid, error = validate_transaction("not a json object")
    assert is_valid is False
    assert "Payload must be a JSON object" in error
