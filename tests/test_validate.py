import logging
import pytest
from etl import validate

VALID_RECORD = {
    "input_text":         "A reddit post about mental health",
    "predicted_disorder": "Depression",
    "probability":        0.87,
    "timestamp":          "2026-06-01 01:49:16",
}


class TestValidate:
    def test_valid_record(self):
        assert validate(VALID_RECORD, "abc123.json") is True

    def test_probability_as_int_accepted(self):
        # probability is typed as (int, float) — both must be valid
        assert validate({**VALID_RECORD, "probability": 85}, "abc123.json") is True

    @pytest.mark.parametrize("field", [
        "input_text", "predicted_disorder", "probability", "timestamp"
    ])
    def test_missing_field_returns_false(self, field, caplog):
        data = {k: v for k, v in VALID_RECORD.items() if k != field}
        with caplog.at_level(logging.WARNING):
            result = validate(data, "abc123.json")
        assert result is False
        assert field in caplog.text

    @pytest.mark.parametrize("field,bad_value", [
        ("input_text",         123),
        ("predicted_disorder", 456),
        ("probability",        "high"),
        ("timestamp",          20260601),
    ])
    def test_wrong_type_returns_false(self, field, bad_value, caplog):
        data = {**VALID_RECORD, field: bad_value}
        with caplog.at_level(logging.WARNING):
            result = validate(data, "abc123.json")
        assert result is False
        assert field in caplog.text
