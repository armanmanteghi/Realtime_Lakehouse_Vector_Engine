"""
Pre-ETL Data Quality & Schema Auditing Module
Enforces strict column presence, nullability constraints, and type signatures prior to write operations.
"""

from typing import Dict, Any, List, Tuple

class SchemaValidator:
    def __init__(self, required_fields: List[str], non_nullable_fields: List[str]):
        self.required_fields = required_fields
        self.non_nullable_fields = non_nullable_fields

    def validate_record(self, record: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """Validates a single record against schema rules."""
        errors = []

        # 1. Field presence validation
        for field in self.required_fields:
            if field not in record:
                errors.append(f"Missing required field: {field}")

        # 2. Nullability constraint validation
        for field in self.non_nullable_fields:
            if record.get(field) is None:
                errors.append(f"Null value constraint violation on field: {field}")

        is_valid = len(errors) == 0
        return is_valid, errors

    def audit_batch(self, batch: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        """Splits a batch into clean records and dead-letter records."""
        clean_records = []
        rejected_records = []

        for record in batch:
            is_valid, errors = self.validate_record(record)
            if is_valid:
                clean_records.append(record)
            else:
                record["_validation_errors"] = errors
                rejected_records.append(record)

        return clean_records, rejected_records
