"""DDS draft generation and local mock workflow helpers.

This package prepares local draft evidence contracts only. Any submission
helpers are deterministic mocks for tests and development, not TRACES/EUDR
network adapters.
"""

from .workflow import (
    DEFAULT_APPROVAL_TEXT,
    DDSWorkflowError,
    MockDDSService,
    assert_draft_ready_for_approval,
    blocking_validation_reasons,
    select_evidence_bundle,
    submit_approved_draft,
    write_operator_approval,
)

__all__ = [
    "DEFAULT_APPROVAL_TEXT",
    "DDSWorkflowError",
    "MockDDSService",
    "assert_draft_ready_for_approval",
    "blocking_validation_reasons",
    "select_evidence_bundle",
    "submit_approved_draft",
    "write_operator_approval",
]
