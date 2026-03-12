"""GitHub transport helpers for CLI-backed adapters."""

from __future__ import annotations

import subprocess
import time
from collections.abc import Callable, Sequence

from startupai_controller.gh_cli_timeout import gh_command_timeout_seconds
from startupai_controller.github_http import GitHubTransportError, run_github_command
from startupai_controller.validate_critical_path_promotion import GhQueryError

_GH_RETRY_DELAYS_SECONDS = (1.0, 2.0, 4.0)
_GH_RETRYABLE_ERROR_MARKERS = (
    "error connecting to api.github.com",
    "connection reset by peer",
    "tls handshake timeout",
    "i/o timeout",
    "timeout awaiting response headers",
    "timed out after",
    "temporary failure in name resolution",
)
_GH_RATE_LIMIT_ERROR_MARKERS = (
    "api rate limit exceeded",
    "secondary rate limit",
)
_GH_COMMAND_TIMEOUT_SECONDS = gh_command_timeout_seconds()


class GhCommandError(GhQueryError):
    """Structured GitHub CLI failure with normalized operation and kind."""

    def __init__(
        self,
        *,
        operation_type: str,
        failure_kind: str,
        command_excerpt: str,
        detail: str,
        rate_limit_reset_at: int | None = None,
    ) -> None:
        self.operation_type = operation_type
        self.failure_kind = failure_kind
        self.command_excerpt = command_excerpt
        self.detail = detail
        self.rate_limit_reset_at = rate_limit_reset_at
        super().__init__(
            f"{operation_type}:{failure_kind}:Failed running gh "
            f"{command_excerpt}: {detail}"
        )


def _classify_gh_failure_kind(detail: str) -> str:
    """Normalize GitHub CLI failure text into a stable reason code."""
    text = detail.strip().lower()
    if any(marker in text for marker in _GH_RATE_LIMIT_ERROR_MARKERS):
        return "rate_limit"
    if any(marker in text for marker in _GH_RETRYABLE_ERROR_MARKERS):
        return "network"
    if (
        "authentication failed" in text
        or "http 401" in text
        or "must authenticate" in text
    ):
        return "auth"
    if "http 403" in text and "rate limit" not in text:
        return "auth"
    if "http 5" in text or "server error" in text or "bad gateway" in text:
        return "github_outage"
    if "invalid character" in text or "unexpected token" in text:
        return "invalid_response"
    return "query_failed"


def _gh_error(
    args: Sequence[str],
    *,
    operation_type: str,
    detail: str,
) -> GhCommandError:
    """Build a structured GitHub command error."""
    return GhCommandError(
        operation_type=operation_type,
        failure_kind=_classify_gh_failure_kind(detail),
        command_excerpt=" ".join(args[:3]),
        detail=detail.strip() or "unknown-gh-error",
        rate_limit_reset_at=None,
    )


def gh_reason_code(error: Exception) -> str:
    """Return a stable machine-readable reason code for GitHub failures."""
    if isinstance(error, GhCommandError):
        return error.failure_kind
    return _classify_gh_failure_kind(str(error))


def _run_gh(
    args: list[str],
    *,
    gh_runner: Callable[..., str] | None = None,
    operation_type: str = "query",
) -> str:
    """Run a gh CLI command and return stdout."""
    if gh_runner is not None:
        try:
            return gh_runner(args)
        except GhQueryError:
            raise
        except subprocess.CalledProcessError as error:
            detail = (error.output or "").strip() or str(error)
            raise _gh_error(
                args,
                operation_type=operation_type,
                detail=detail,
            ) from error

    try:
        http_output = run_github_command(
            args,
            operation_type=operation_type,
            timeout_seconds=_GH_COMMAND_TIMEOUT_SECONDS,
            retry_delays=_GH_RETRY_DELAYS_SECONDS,
        )
        if http_output is not None:
            return http_output
    except GitHubTransportError as error:
        raise GhCommandError(
            operation_type=error.operation_type,
            failure_kind=error.failure_kind,
            command_excerpt=error.command_excerpt,
            detail=error.detail,
            rate_limit_reset_at=error.rate_limit_reset_at,
        ) from error

    gh_command = ["gh"] + args
    for attempt in range(len(_GH_RETRY_DELAYS_SECONDS) + 1):
        try:
            return subprocess.check_output(
                gh_command,
                text=True,
                stderr=subprocess.STDOUT,
                timeout=_GH_COMMAND_TIMEOUT_SECONDS,
            )
        except OSError as error:
            raise GhCommandError(
                operation_type=operation_type,
                failure_kind="network",
                command_excerpt=" ".join(args[:3]),
                detail=str(error),
            ) from error
        except subprocess.TimeoutExpired as error:
            detail = f"timed out after {_GH_COMMAND_TIMEOUT_SECONDS:.1f}s"
            raise GhCommandError(
                operation_type=operation_type,
                failure_kind="network",
                command_excerpt=" ".join(args[:3]),
                detail=detail,
            ) from error
        except subprocess.CalledProcessError as error:
            output = error.output.strip()
            failure_kind = _classify_gh_failure_kind(output)
            is_retryable = failure_kind in {"network", "rate_limit", "github_outage"}
            if is_retryable and attempt < len(_GH_RETRY_DELAYS_SECONDS):
                time.sleep(_GH_RETRY_DELAYS_SECONDS[attempt])
                continue
            raise GhCommandError(
                operation_type=operation_type,
                failure_kind=failure_kind,
                command_excerpt=" ".join(args[:3]),
                detail=output,
            ) from error
