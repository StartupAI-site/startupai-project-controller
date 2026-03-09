"""Resolution-proof helpers — re-export shim.

Canonical implementation moved to domain/resolution_policy.py.
This file exists only for backward compatibility with external callers.
"""

from startupai_controller.domain.resolution_policy import (  # noqa: F401
    AUTO_CLOSE_RESOLUTION_KINDS,
    NON_AUTO_CLOSE_RESOLUTION_KINDS,
    ParsedResolutionComment,
    RESOLUTION_COMMENT_KIND,
    VALID_EQUIVALENCE_CLAIMS,
    VALID_RESOLUTION_ACTIONS,
    VALID_RESOLUTION_KINDS,
    VALID_VERIFICATION_CLASSES,
    build_resolution_comment,
    normalize_resolution_payload,
    parse_resolution_comment,
    resolution_allows_autoclose,
    resolution_has_meaningful_signal,
)
