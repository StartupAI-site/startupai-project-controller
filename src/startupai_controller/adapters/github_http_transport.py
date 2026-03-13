from __future__ import annotations

from dataclasses import dataclass
from contextvars import ContextVar, Token
import json
import os
import re
import socket
import subprocess
import time
from typing import Any, Mapping, Sequence
from urllib import error as urllib_error
from urllib import parse as urllib_parse
from urllib import request as urllib_request

API_BASE_URL = "https://api.github.com"
GRAPHQL_URL = f"{API_BASE_URL}/graphql"
DEFAULT_API_VERSION = "2022-11-28"
USER_AGENT = "startupai-board-consumer"
RETRYABLE_FAILURE_KINDS = frozenset({"network", "rate_limit", "github_outage"})
_RATE_LIMIT_ERROR_MARKERS = (
    "api rate limit exceeded",
    "secondary rate limit",
    "rate limit",
)
_NETWORK_ERROR_MARKERS = (
    "error connecting to api.github.com",
    "connection reset by peer",
    "tls handshake timeout",
    "i/o timeout",
    "timeout awaiting response headers",
    "timed out after",
    "temporary failure in name resolution",
)
_TOKEN_CACHE: str | None = None
_REQUEST_STATS: ContextVar["GitHubRequestStats | None"] = ContextVar(
    "github_http_request_stats",
    default=None,
)
SUPPORTED_OUTPUT_FILTERS = frozenset(
    {
        ".[].body",
        ".body",
        ".updated_at",
        ".state",
        ".assignees[].login",
        "{state: .state, updated_at: .updated_at}",
        "{title: .title, body: .body, labels: [.labels[].name]}",
        "{title: .title, body: .body, labels: [.labels[].name], updated_at: .updated_at}",
    }
)


class GitHubTransportError(RuntimeError):

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
        super().__init__(detail)


@dataclass(frozen=True)
class _ApiCommand:
    endpoint: str
    method: str | None
    fields: dict[str, Any]
    q_expr: str | None
    jq_expr: str | None
    paginate: bool
    slurp: bool


@dataclass(frozen=True)
class _PullRequestCommand:
    selector: str | None
    repo: str
    state: str | None
    limit: int | None
    search: str | None
    json_fields: tuple[str, ...]
    head: str | None = None
    title: str | None = None
    body: str | None = None
    base: str | None = None
    comment: str | None = None
    delete_branch: bool = False
    auto_merge: bool = False
    squash: bool = False


@dataclass
class GitHubRequestStats:

    graphql: int = 0
    rest: int = 0


def begin_request_stats() -> Token:
    return _REQUEST_STATS.set(GitHubRequestStats())


def end_request_stats(token: Token) -> GitHubRequestStats:
    stats = _REQUEST_STATS.get() or GitHubRequestStats()
    _REQUEST_STATS.reset(token)
    return stats


def _record_request(url: str) -> None:
    stats = _REQUEST_STATS.get()
    if stats is None:
        return
    if url.endswith("/graphql"):
        stats.graphql += 1
    else:
        stats.rest += 1


def classify_failure_kind(detail: str, *, status_code: int | None = None) -> str:
    text = detail.strip().lower()
    if status_code == 429 or any(
        marker in text for marker in _RATE_LIMIT_ERROR_MARKERS
    ):
        return "rate_limit"
    if any(marker in text for marker in _NETWORK_ERROR_MARKERS):
        return "network"
    if status_code in {401, 403} or "must authenticate" in text:
        if "rate limit" not in text:
            return "auth"
    if status_code is not None and 500 <= status_code <= 599:
        return "github_outage"
    if "invalid character" in text or "unexpected token" in text:
        return "invalid_response"
    return "query_failed"


def run_github_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    """Run a supported gh command via direct HTTP.

    Returns `None` when the command is not implemented here and should fall back
    to subprocess `gh`.
    """
    normalized = list(args)
    if normalized and normalized[0] == "gh":
        normalized = normalized[1:]
    if not normalized:
        return None

    if normalized[0] == "api":
        return _run_api_command(
            normalized[1:],
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
        )

    if normalized[0] == "pr" and len(normalized) >= 2:
        subcommand = normalized[1]
        if subcommand == "list":
            return _run_pr_list_command(
                normalized[2:],
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        if subcommand == "view":
            return _run_pr_view_command(
                normalized[2:],
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        if subcommand == "create":
            return _run_pr_create_command(
                normalized[2:],
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        if subcommand == "edit":
            return _run_pr_edit_command(
                normalized[2:],
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        if subcommand == "close":
            return _run_pr_close_command(
                normalized[2:],
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        if subcommand == "update-branch":
            return _run_pr_update_branch_command(
                normalized[2:],
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        if subcommand == "merge":
            return _run_pr_merge_command(
                normalized[2:],
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        return None

    if normalized[0] == "run" and len(normalized) >= 2 and normalized[1] == "rerun":
        return _run_actions_rerun_command(
            normalized[2:],
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
        )

    return None


def _github_token() -> str:
    global _TOKEN_CACHE
    for env_name in ("GH_TOKEN", "GITHUB_TOKEN"):
        value = os.environ.get(env_name, "").strip()
        if value:
            return value

    if _TOKEN_CACHE is not None:
        return _TOKEN_CACHE

    try:
        token = subprocess.check_output(
            ["gh", "auth", "token"],
            text=True,
            stderr=subprocess.STDOUT,
            timeout=5,
        ).strip()
    except (OSError, subprocess.SubprocessError) as error:
        raise GitHubTransportError(
            operation_type="auth",
            failure_kind="auth",
            command_excerpt="gh auth token",
            detail=str(error),
        ) from error

    if not token:
        raise GitHubTransportError(
            operation_type="auth",
            failure_kind="auth",
            command_excerpt="gh auth token",
            detail="empty auth token",
        )

    _TOKEN_CACHE = token
    return token


def _headers(*, content_type_json: bool = False) -> dict[str, str]:
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {_github_token()}",
        "User-Agent": USER_AGENT,
        "X-GitHub-Api-Version": DEFAULT_API_VERSION,
    }
    if content_type_json:
        headers["Content-Type"] = "application/json"
    return headers


def _transport_error(
    *,
    operation_type: str,
    command_excerpt: str,
    detail: str,
    status_code: int | None = None,
    rate_limit_reset_at: int | None = None,
) -> GitHubTransportError:
    return GitHubTransportError(
        operation_type=operation_type,
        failure_kind=classify_failure_kind(detail, status_code=status_code),
        command_excerpt=command_excerpt,
        detail=detail.strip() or "unknown-github-error",
        rate_limit_reset_at=rate_limit_reset_at,
    )


def _rate_limit_reset_at(headers: Mapping[str, str] | None) -> int | None:
    if not headers:
        return None
    raw = headers.get("x-ratelimit-reset") or headers.get("X-RateLimit-Reset")
    if not raw:
        return None
    try:
        reset_at = int(str(raw).strip())
    except ValueError:
        return None
    return reset_at if reset_at > 0 else None


def _read_json_response(
    url: str,
    *,
    method: str,
    payload: dict[str, Any] | None,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
    command_excerpt: str,
) -> tuple[Any, Mapping[str, str]]:
    body = None
    if payload is not None:
        body = json.dumps(payload).encode("utf-8")

    for attempt in range(len(retry_delays) + 1):
        try:
            _record_request(url)
            request = urllib_request.Request(
                url,
                data=body,
                headers=_headers(content_type_json=payload is not None),
                method=method,
            )
            with urllib_request.urlopen(request, timeout=timeout_seconds) as response:
                raw = response.read().decode("utf-8")
                try:
                    parsed = json.loads(raw) if raw else {}
                except json.JSONDecodeError as error:
                    raise _transport_error(
                        operation_type=operation_type,
                        command_excerpt=command_excerpt,
                        detail="invalid JSON response",
                    ) from error
                return parsed, dict(response.headers.items())
        except urllib_error.HTTPError as error:
            raw = error.read().decode("utf-8", errors="replace")
            detail = _http_error_detail(raw, error.code)
            failure_kind = classify_failure_kind(detail, status_code=error.code)
            reset_at = _rate_limit_reset_at(dict(error.headers.items()))
            if failure_kind in RETRYABLE_FAILURE_KINDS and attempt < len(retry_delays):
                time.sleep(retry_delays[attempt])
                continue
            raise _transport_error(
                operation_type=operation_type,
                command_excerpt=command_excerpt,
                detail=detail,
                status_code=error.code,
                rate_limit_reset_at=reset_at,
            ) from error
        except urllib_error.URLError as error:
            reason = getattr(error, "reason", error)
            if isinstance(reason, (socket.timeout, TimeoutError)):
                raise _transport_error(
                    operation_type=operation_type,
                    command_excerpt=command_excerpt,
                    detail=f"timed out after {timeout_seconds:.1f}s",
                ) from error
            detail = str(reason)
            if attempt < len(retry_delays):
                time.sleep(retry_delays[attempt])
                continue
            raise _transport_error(
                operation_type=operation_type,
                command_excerpt=command_excerpt,
                detail=detail,
                status_code=None,
            ) from error
        except (TimeoutError, socket.timeout) as error:
            raise _transport_error(
                operation_type=operation_type,
                command_excerpt=command_excerpt,
                detail=f"timed out after {timeout_seconds:.1f}s",
                status_code=None,
            ) from error
        except OSError as error:
            detail = str(getattr(error, "reason", error))
            if attempt < len(retry_delays):
                time.sleep(retry_delays[attempt])
                continue
            raise _transport_error(
                operation_type=operation_type,
                command_excerpt=command_excerpt,
                detail=detail,
                status_code=None,
            ) from error
    raise AssertionError("transport retry loop exited without returning or raising")


def _http_error_detail(raw: str, status_code: int) -> str:
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        payload = None

    if isinstance(payload, dict):
        message = str(payload.get("message") or f"HTTP {status_code}")
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            parts = [message]
            for entry in errors:
                if isinstance(entry, dict):
                    parts.append(str(entry.get("message") or entry))
                else:
                    parts.append(str(entry))
            return "; ".join(part for part in parts if part)
        return message

    if raw.strip():
        return raw.strip()
    return f"HTTP {status_code}"


def _append_query(url: str, query: Mapping[str, Any] | None) -> str:
    if not query:
        return url

    existing = urllib_parse.urlsplit(url)
    existing_pairs = urllib_parse.parse_qsl(existing.query, keep_blank_values=True)
    new_pairs = existing_pairs + list(_query_pairs(query))
    encoded = urllib_parse.urlencode(new_pairs, doseq=True)
    return urllib_parse.urlunsplit(
        (existing.scheme, existing.netloc, existing.path, encoded, existing.fragment)
    )


def _query_pairs(query: Mapping[str, Any]) -> list[tuple[str, Any]]:
    pairs: list[tuple[str, Any]] = []
    for key, value in query.items():
        if isinstance(value, list):
            for item in value:
                pairs.append((key, item))
        else:
            pairs.append((key, value))
    return pairs


def _next_link(headers: Mapping[str, str]) -> str | None:
    link_header = headers.get("Link") or headers.get("link") or ""
    for entry in link_header.split(","):
        if 'rel="next"' not in entry:
            continue
        match = re.search(r"<([^>]+)>", entry)
        if match:
            return match.group(1)
    return None


def _graphql(
    query: str,
    variables: Mapping[str, Any],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
    command_excerpt: str,
) -> dict[str, Any]:
    payload, _headers = _read_json_response(
        GRAPHQL_URL,
        method="POST",
        payload={"query": query, "variables": dict(variables)},
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt=command_excerpt,
    )
    if not isinstance(payload, dict):
        raise _transport_error(
            operation_type=operation_type,
            command_excerpt=command_excerpt,
            detail="invalid GraphQL response",
        )
    return payload


def _rest_json(
    method: str,
    path_or_url: str,
    *,
    query: Mapping[str, Any] | None = None,
    payload: dict[str, Any] | None = None,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
    command_excerpt: str,
) -> tuple[Any, Mapping[str, str]]:
    if path_or_url.startswith("http://") or path_or_url.startswith("https://"):
        url = path_or_url
    else:
        url = f"{API_BASE_URL.rstrip('/')}/{path_or_url.lstrip('/')}"
    url = _append_query(url, query)
    return _read_json_response(
        url,
        method=method,
        payload=payload,
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt=command_excerpt,
    )


def _rest_paginated(
    path: str,
    *,
    query: Mapping[str, Any] | None,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
    command_excerpt: str,
) -> list[Any]:
    pages: list[Any] = []
    base_url = f"{API_BASE_URL.rstrip('/')}/{path.lstrip('/')}"
    next_url: str | None = _append_query(base_url, query)
    if query:
        next_url = _append_query(base_url, query)

    while next_url:
        page, headers = _rest_json(
            "GET",
            next_url,
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt=command_excerpt,
        )
        pages.append(page)
        next_url = _next_link(headers)

    return pages


def _run_api_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    parsed = _parse_api_command(args)
    if parsed is None:
        return None

    command_excerpt = (
        "api graphql" if parsed.endpoint == "graphql" else f"api {parsed.endpoint}"
    )

    if parsed.endpoint == "graphql":
        variables = dict(parsed.fields)
        graphql_query = str(variables.pop("query", ""))
        if not graphql_query:
            return None
        payload = _graphql(
            graphql_query,
            variables,
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt=command_excerpt,
        )
        return json.dumps(payload)

    method = (parsed.method or ("POST" if parsed.fields else "GET")).upper()
    if parsed.paginate:
        rest_query = dict(parsed.fields) if method == "GET" else None
        if rest_query is not None and "per_page" not in rest_query:
            rest_query["per_page"] = 100
        pages = _rest_paginated(
            parsed.endpoint,
            query=rest_query,
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt=command_excerpt,
        )
        return _render_api_output(
            payload=pages[-1] if pages else [],
            pages=pages,
            q_expr=parsed.q_expr,
            jq_expr=parsed.jq_expr,
            slurp=parsed.slurp,
        )

    rest_query = dict(parsed.fields) if method == "GET" else None
    body = dict(parsed.fields) if method != "GET" else None
    payload, _headers = _rest_json(
        method,
        parsed.endpoint,
        query=rest_query,
        payload=body,
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt=command_excerpt,
    )
    return _render_api_output(
        payload=payload,
        pages=None,
        q_expr=parsed.q_expr,
        jq_expr=parsed.jq_expr,
        slurp=parsed.slurp,
    )


def _render_api_output(
    *,
    payload: Any,
    pages: list[Any] | None,
    q_expr: str | None,
    jq_expr: str | None,
    slurp: bool,
) -> str:
    expr = q_expr or jq_expr
    if expr is not None:
        if pages is None:
            return _apply_output_filter(payload, expr)
        rendered = [_apply_output_filter(page, expr) for page in pages]
        return "\n".join(part for part in rendered if part)

    if pages is not None:
        if slurp:
            return json.dumps(pages)
        if len(pages) == 1:
            return json.dumps(pages[0])
        return "\n".join(json.dumps(page) for page in pages)
    return json.dumps(payload)


def _apply_output_filter(payload: Any, expr: str) -> str:
    if expr == ".[].body":
        if not isinstance(payload, list):
            return ""
        return "\n".join(
            str(item.get("body") or "") for item in payload if isinstance(item, dict)
        )

    if expr == ".body":
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("body") or "")

    if expr == ".updated_at":
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("updated_at") or "")

    if expr == ".state":
        if not isinstance(payload, dict):
            return ""
        return str(payload.get("state") or "")

    if expr == ".assignees[].login":
        if not isinstance(payload, dict):
            return ""
        assignees = payload.get("assignees") or []
        return "\n".join(
            str(item.get("login") or "")
            for item in assignees
            if isinstance(item, dict) and item.get("login")
        )

    if expr == "{state: .state, updated_at: .updated_at}":
        if not isinstance(payload, dict):
            return json.dumps({"state": "", "updated_at": ""})
        return json.dumps(
            {
                "state": payload.get("state"),
                "updated_at": payload.get("updated_at"),
            }
        )

    if expr == "{title: .title, body: .body, labels: [.labels[].name]}":
        if not isinstance(payload, dict):
            return json.dumps({"title": "", "body": "", "labels": []})
        labels = payload.get("labels") or []
        return json.dumps(
            {
                "title": payload.get("title"),
                "body": payload.get("body"),
                "labels": [
                    str(item.get("name") or "")
                    for item in labels
                    if isinstance(item, dict) and item.get("name")
                ],
            }
        )

    if (
        expr
        == "{title: .title, body: .body, labels: [.labels[].name], updated_at: .updated_at}"
    ):
        if not isinstance(payload, dict):
            return json.dumps({"title": "", "body": "", "labels": [], "updated_at": ""})
        labels = payload.get("labels") or []
        return json.dumps(
            {
                "title": payload.get("title"),
                "body": payload.get("body"),
                "labels": [
                    str(item.get("name") or "")
                    for item in labels
                    if isinstance(item, dict) and item.get("name")
                ],
                "updated_at": payload.get("updated_at"),
            }
        )

    raise _transport_error(
        operation_type="query",
        command_excerpt="api filter",
        detail=f"unsupported filter expression: {expr}",
    )


def _parse_api_command(args: Sequence[str]) -> _ApiCommand | None:
    if not args:
        return None

    endpoint = args[0]
    fields: dict[str, Any] = {}
    method: str | None = None
    q_expr: str | None = None
    jq_expr: str | None = None
    paginate = False
    slurp = False

    index = 1
    while index < len(args):
        arg = args[index]
        if arg in {"-f", "-F"}:
            if index + 1 >= len(args):
                return None
            raw = args[index + 1]
            key, value = _split_field(raw)
            _append_field(fields, key, _coerce_field_value(value, typed=arg == "-F"))
            index += 2
            continue
        if arg in {"-X", "--method"}:
            if index + 1 >= len(args):
                return None
            method = args[index + 1]
            index += 2
            continue
        if arg == "-q":
            if index + 1 >= len(args):
                return None
            q_expr = args[index + 1]
            index += 2
            continue
        if arg == "--jq":
            if index + 1 >= len(args):
                return None
            jq_expr = args[index + 1]
            index += 2
            continue
        if arg == "--paginate":
            paginate = True
            index += 1
            continue
        if arg == "--slurp":
            slurp = True
            index += 1
            continue
        return None

    return _ApiCommand(
        endpoint=endpoint,
        method=method,
        fields=fields,
        q_expr=q_expr,
        jq_expr=jq_expr,
        paginate=paginate,
        slurp=slurp,
    )


def _split_field(raw: str) -> tuple[str, str]:
    if "=" not in raw:
        return raw, ""
    key, value = raw.split("=", maxsplit=1)
    return key, value


def _append_field(target: dict[str, Any], key: str, value: Any) -> None:
    if key.endswith("[]"):
        normalized = key[:-2]
        existing = target.get(normalized)
        if not isinstance(existing, list):
            existing = []
            target[normalized] = existing
        existing.append(value)
        return
    target[key] = value


def _coerce_field_value(value: str, *, typed: bool) -> Any:
    if not typed:
        return value
    lowered = value.lower()
    if lowered == "null":
        return None
    if lowered == "true":
        return True
    if lowered == "false":
        return False
    if re.fullmatch(r"-?\d+", value):
        try:
            return int(value)
        except ValueError:
            return value
    return value


def _run_pr_list_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    command = _parse_pr_list_command(args)
    if command is None:
        return None

    owner, repo = _split_repo(command.repo)
    fields = set(command.json_fields)
    limit = min(command.limit or 30, 100)

    pulls: list[dict[str, Any]] = []
    if command.search:
        search_query = f"repo:{command.repo} is:pr"
        if command.state and command.state != "all":
            search_query += f" is:{command.state}"
        search_query += f" {command.search}"
        search_payload, _headers = _rest_json(
            "GET",
            "search/issues",
            query={"q": search_query, "per_page": limit},
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt="api search/issues",
        )
        numbers: list[int] = []
        for item in search_payload.get("items") or []:
            if not isinstance(item, dict):
                continue
            raw_number = item.get("number")
            if isinstance(raw_number, int):
                numbers.append(raw_number)
                continue
            if isinstance(raw_number, str):
                try:
                    numbers.append(int(raw_number))
                except ValueError:
                    continue
        for number in numbers[:limit]:
            pull, _headers = _rest_json(
                "GET",
                f"repos/{owner}/{repo}/pulls/{number}",
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
                command_excerpt="pr view",
            )
            if isinstance(pull, dict):
                pulls.append(pull)
    else:
        query = {"state": command.state or "open", "per_page": limit}
        pages = _rest_paginated(
            f"repos/{owner}/{repo}/pulls",
            query=query,
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt="pr list",
        )
        for page in pages:
            if isinstance(page, list):
                pulls.extend(item for item in page if isinstance(item, dict))
            elif isinstance(page, dict):
                pulls.append(page)
            if len(pulls) >= limit:
                break

    payload = [
        _project_pull_json(
            pull,
            owner=owner,
            repo=repo,
            fields=fields,
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
        )
        for pull in pulls[:limit]
    ]
    return json.dumps(payload)


def _run_pr_view_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    command = _parse_pr_view_command(args)
    if command is None:
        return None

    pull = _resolve_pull_request(
        command.repo,
        command.selector or "",
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
    )
    owner, repo = _split_repo(command.repo)
    payload = _project_pull_json(
        pull,
        owner=owner,
        repo=repo,
        fields=set(command.json_fields),
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
    )
    return json.dumps(payload)


def _run_pr_create_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    command = _parse_pr_create_or_edit_command(args, expect_selector=False)
    if (
        command is None
        or command.head is None
        or command.title is None
        or command.body is None
    ):
        return None

    owner, repo = _split_repo(command.repo)
    payload, _headers = _rest_json(
        "POST",
        f"repos/{owner}/{repo}/pulls",
        payload={
            "head": command.head,
            "base": command.base or "main",
            "title": command.title,
            "body": command.body,
        },
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="pr create",
    )
    if not isinstance(payload, dict):
        raise _transport_error(
            operation_type=operation_type,
            command_excerpt="pr create",
            detail="invalid PR create response",
        )
    return str(payload.get("html_url") or "")


def _run_pr_edit_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    command = _parse_pr_create_or_edit_command(args, expect_selector=True)
    if command is None or command.body is None or command.selector is None:
        return None

    pull = _resolve_pull_request(
        command.repo,
        command.selector,
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
    )
    owner, repo = _split_repo(command.repo)
    number = int(pull.get("number") or 0)
    payload, _headers = _rest_json(
        "PATCH",
        f"repos/{owner}/{repo}/pulls/{number}",
        payload={"body": command.body},
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="pr edit",
    )
    if not isinstance(payload, dict):
        raise _transport_error(
            operation_type=operation_type,
            command_excerpt="pr edit",
            detail="invalid PR edit response",
        )
    return str(payload.get("html_url") or "")


def _run_pr_close_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    command = _parse_pr_close_command(args)
    if command is None or command.selector is None:
        return None

    pull = _resolve_pull_request(
        command.repo,
        command.selector,
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
    )
    owner, repo = _split_repo(command.repo)
    number = int(pull.get("number") or 0)

    if command.comment:
        _rest_json(
            "POST",
            f"repos/{owner}/{repo}/issues/{number}/comments",
            payload={"body": command.comment},
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt="pr close comment",
        )

    payload, _headers = _rest_json(
        "PATCH",
        f"repos/{owner}/{repo}/issues/{number}",
        payload={"state": "closed"},
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="pr close",
    )
    if not isinstance(payload, dict):
        raise _transport_error(
            operation_type=operation_type,
            command_excerpt="pr close",
            detail="invalid PR close response",
        )
    return str(payload.get("html_url") or "")


def _run_pr_update_branch_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    command = _parse_pr_update_branch_command(args)
    if command is None or command.selector is None:
        return None

    pull = _resolve_pull_request(
        command.repo,
        command.selector,
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
    )
    owner, repo = _split_repo(command.repo)
    number = int(pull.get("number") or 0)
    payload, _headers = _rest_json(
        "PUT",
        f"repos/{owner}/{repo}/pulls/{number}/update-branch",
        payload={},
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="pr update-branch",
    )
    return json.dumps(payload)


def _run_pr_merge_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    command = _parse_pr_merge_command(args)
    if command is None or command.selector is None or not command.auto_merge:
        return None

    pull = _resolve_pull_request(
        command.repo,
        command.selector,
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
    )
    node_id = str(pull.get("node_id") or "")
    if not node_id:
        raise _transport_error(
            operation_type=operation_type,
            command_excerpt="pr merge",
            detail="missing pull request node id",
        )
    _graphql(
        """
mutation($pullRequestId: ID!, $mergeMethod: PullRequestMergeMethod!) {
  enablePullRequestAutoMerge(
    input: {
      pullRequestId: $pullRequestId
      mergeMethod: $mergeMethod
    }
  ) {
    pullRequest { number }
  }
}
""",
        {
            "pullRequestId": node_id,
            "mergeMethod": "SQUASH" if command.squash else "MERGE",
        },
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="pr merge",
    )
    return ""


def _run_actions_rerun_command(
    args: Sequence[str],
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> str | None:
    if not args:
        return None
    run_id = args[0]
    repo: str | None = None
    index = 1
    while index < len(args):
        arg = args[index]
        if arg == "--repo" and index + 1 < len(args):
            repo = args[index + 1]
            index += 2
            continue
        return None

    if repo is None:
        return None

    owner, repository = _split_repo(repo)
    payload, _headers = _rest_json(
        "POST",
        f"repos/{owner}/{repository}/actions/runs/{run_id}/rerun",
        payload={},
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="run rerun",
    )
    return json.dumps(payload)


def _resolve_pull_request(
    repo_slug: str,
    selector: str,
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> dict[str, Any]:
    owner, repo = _split_repo(repo_slug)
    if selector.isdigit():
        payload, _headers = _rest_json(
            "GET",
            f"repos/{owner}/{repo}/pulls/{selector}",
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt="pr view",
        )
        if isinstance(payload, dict):
            return payload
        raise _transport_error(
            operation_type=operation_type,
            command_excerpt="pr view",
            detail="invalid pull request response",
        )

    pages = _rest_paginated(
        f"repos/{owner}/{repo}/pulls",
        query={"state": "open", "head": f"{owner}:{selector}", "per_page": 1},
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="pr view",
    )
    for page in pages:
        if isinstance(page, list) and page:
            first = page[0]
            if isinstance(first, dict):
                return first

    raise _transport_error(
        operation_type=operation_type,
        command_excerpt="pr view",
        detail=f"pull request not found for selector '{selector}'",
    )


def _status_check_rollup(
    owner: str,
    repo: str,
    head_sha: str,
    *,
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> list[dict[str, Any]]:
    checks_payload, _headers = _rest_json(
        "GET",
        f"repos/{owner}/{repo}/commits/{head_sha}/check-runs",
        query={"per_page": 100},
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="check-runs",
    )
    statuses_payload, _headers = _rest_json(
        "GET",
        f"repos/{owner}/{repo}/commits/{head_sha}/status",
        operation_type=operation_type,
        timeout_seconds=timeout_seconds,
        retry_delays=retry_delays,
        command_excerpt="status contexts",
    )

    rollup: list[dict[str, Any]] = []
    for run in (
        checks_payload.get("check_runs", []) if isinstance(checks_payload, dict) else []
    ):
        if not isinstance(run, dict):
            continue
        rollup.append(
            {
                "__typename": "CheckRun",
                "name": str(run.get("name") or ""),
                "status": str(run.get("status") or "").lower(),
                "conclusion": str(run.get("conclusion") or "").lower(),
                "detailsUrl": str(run.get("details_url") or ""),
                "completedAt": str(run.get("completed_at") or ""),
                "startedAt": str(run.get("started_at") or ""),
                "workflowName": "",
            }
        )

    for status in (
        statuses_payload.get("statuses", [])
        if isinstance(statuses_payload, dict)
        else []
    ):
        if not isinstance(status, dict):
            continue
        rollup.append(
            {
                "__typename": "StatusContext",
                "context": str(status.get("context") or ""),
                "state": str(status.get("state") or "").lower(),
                "targetUrl": str(status.get("target_url") or ""),
                "startedAt": str(status.get("created_at") or ""),
            }
        )

    return rollup


def _project_pull_json(
    pull: dict[str, Any],
    *,
    owner: str,
    repo: str,
    fields: set[str],
    operation_type: str,
    timeout_seconds: float,
    retry_delays: Sequence[float],
) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    number = int(pull.get("number") or 0)

    if "number" in fields:
        payload["number"] = number
    if "url" in fields:
        payload["url"] = str(pull.get("html_url") or "")
    if "body" in fields:
        payload["body"] = str(pull.get("body") or "")
    if "author" in fields:
        payload["author"] = {"login": str((pull.get("user") or {}).get("login") or "")}
    if "headRefName" in fields:
        payload["headRefName"] = str(((pull.get("head") or {}).get("ref")) or "")
    if "isDraft" in fields:
        payload["isDraft"] = bool(pull.get("draft", False))
    if "state" in fields:
        payload["state"] = str(pull.get("state") or "")
    if "mergeStateStatus" in fields:
        payload["mergeStateStatus"] = str(pull.get("mergeable_state") or "").upper()
    if "mergeable" in fields:
        mergeable = pull.get("mergeable")
        if mergeable is True:
            payload["mergeable"] = "MERGEABLE"
        elif mergeable is False:
            payload["mergeable"] = "CONFLICTING"
        else:
            payload["mergeable"] = "UNKNOWN"
    if "baseRefName" in fields:
        payload["baseRefName"] = str(((pull.get("base") or {}).get("ref")) or "")
    if "autoMergeRequest" in fields:
        payload["autoMergeRequest"] = pull.get("auto_merge")

    if "comments" in fields:
        pages = _rest_paginated(
            f"repos/{owner}/{repo}/issues/{number}/comments",
            query={"per_page": 100},
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt="pr comments",
        )
        comments: list[dict[str, Any]] = []
        for page in pages:
            if not isinstance(page, list):
                continue
            for item in page:
                if not isinstance(item, dict):
                    continue
                comments.append(
                    {
                        "body": str(item.get("body") or ""),
                        "createdAt": str(item.get("created_at") or ""),
                        "author": {
                            "login": str((item.get("user") or {}).get("login") or "")
                        },
                    }
                )
        payload["comments"] = comments

    if "reviews" in fields:
        pages = _rest_paginated(
            f"repos/{owner}/{repo}/pulls/{number}/reviews",
            query={"per_page": 100},
            operation_type=operation_type,
            timeout_seconds=timeout_seconds,
            retry_delays=retry_delays,
            command_excerpt="pr reviews",
        )
        reviews: list[dict[str, Any]] = []
        for page in pages:
            if not isinstance(page, list):
                continue
            for item in page:
                if not isinstance(item, dict):
                    continue
                reviews.append(
                    {
                        "body": str(item.get("body") or ""),
                        "submittedAt": str(item.get("submitted_at") or ""),
                        "state": str(item.get("state") or ""),
                        "author": {
                            "login": str((item.get("user") or {}).get("login") or "")
                        },
                    }
                )
        payload["reviews"] = reviews

    if "statusCheckRollup" in fields:
        head_sha = str(((pull.get("head") or {}).get("sha")) or "")
        if head_sha:
            payload["statusCheckRollup"] = _status_check_rollup(
                owner,
                repo,
                head_sha,
                operation_type=operation_type,
                timeout_seconds=timeout_seconds,
                retry_delays=retry_delays,
            )
        else:
            payload["statusCheckRollup"] = []

    return payload


def _parse_pr_list_command(args: Sequence[str]) -> _PullRequestCommand | None:
    repo: str | None = None
    state = "open"
    limit = 30
    search: str | None = None
    json_fields: tuple[str, ...] = ()

    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--repo" and index + 1 < len(args):
            repo = args[index + 1]
            index += 2
            continue
        if arg == "--state" and index + 1 < len(args):
            state = args[index + 1]
            index += 2
            continue
        if arg == "--limit" and index + 1 < len(args):
            try:
                limit = int(args[index + 1])
            except ValueError:
                return None
            index += 2
            continue
        if arg == "--search" and index + 1 < len(args):
            search = args[index + 1]
            index += 2
            continue
        if arg == "--json" and index + 1 < len(args):
            json_fields = tuple(
                field.strip() for field in args[index + 1].split(",") if field.strip()
            )
            index += 2
            continue
        return None

    if repo is None or not json_fields:
        return None
    return _PullRequestCommand(
        selector=None,
        repo=repo,
        state=state,
        limit=limit,
        search=search,
        json_fields=json_fields,
    )


def _parse_pr_view_command(args: Sequence[str]) -> _PullRequestCommand | None:
    if not args:
        return None
    selector = args[0]
    repo: str | None = None
    json_fields: tuple[str, ...] = ()
    index = 1
    while index < len(args):
        arg = args[index]
        if arg == "--repo" and index + 1 < len(args):
            repo = args[index + 1]
            index += 2
            continue
        if arg == "--json" and index + 1 < len(args):
            json_fields = tuple(
                field.strip() for field in args[index + 1].split(",") if field.strip()
            )
            index += 2
            continue
        return None

    if repo is None or not json_fields:
        return None
    return _PullRequestCommand(
        selector=selector,
        repo=repo,
        state=None,
        limit=None,
        search=None,
        json_fields=json_fields,
    )


def _parse_pr_create_or_edit_command(
    args: Sequence[str],
    *,
    expect_selector: bool,
) -> _PullRequestCommand | None:
    selector: str | None = None
    if expect_selector:
        if not args:
            return None
        selector = args[0]
        args = args[1:]

    repo: str | None = None
    head: str | None = None
    title: str | None = None
    body: str | None = None
    base: str | None = None

    index = 0
    while index < len(args):
        arg = args[index]
        if arg == "--repo" and index + 1 < len(args):
            repo = args[index + 1]
            index += 2
            continue
        if arg == "--head" and index + 1 < len(args):
            head = args[index + 1]
            index += 2
            continue
        if arg == "--title" and index + 1 < len(args):
            title = args[index + 1]
            index += 2
            continue
        if arg == "--body" and index + 1 < len(args):
            body = args[index + 1]
            index += 2
            continue
        if arg == "--base" and index + 1 < len(args):
            base = args[index + 1]
            index += 2
            continue
        return None

    if repo is None:
        return None
    return _PullRequestCommand(
        selector=selector,
        repo=repo,
        state=None,
        limit=None,
        search=None,
        json_fields=(),
        head=head,
        title=title,
        body=body,
        base=base,
    )


def _parse_pr_close_command(args: Sequence[str]) -> _PullRequestCommand | None:
    if not args:
        return None
    selector = args[0]
    repo: str | None = None
    comment: str | None = None
    index = 1
    while index < len(args):
        arg = args[index]
        if arg == "--repo" and index + 1 < len(args):
            repo = args[index + 1]
            index += 2
            continue
        if arg == "--comment" and index + 1 < len(args):
            comment = args[index + 1]
            index += 2
            continue
        return None

    if repo is None:
        return None
    return _PullRequestCommand(
        selector=selector,
        repo=repo,
        state=None,
        limit=None,
        search=None,
        json_fields=(),
        comment=comment,
    )


def _parse_pr_update_branch_command(args: Sequence[str]) -> _PullRequestCommand | None:
    if not args:
        return None
    selector = args[0]
    repo: str | None = None
    index = 1
    while index < len(args):
        arg = args[index]
        if arg == "--repo" and index + 1 < len(args):
            repo = args[index + 1]
            index += 2
            continue
        return None
    if repo is None:
        return None
    return _PullRequestCommand(
        selector=selector,
        repo=repo,
        state=None,
        limit=None,
        search=None,
        json_fields=(),
    )


def _parse_pr_merge_command(args: Sequence[str]) -> _PullRequestCommand | None:
    if not args:
        return None
    selector = args[0]
    repo: str | None = None
    auto_merge = False
    squash = False
    delete_branch = False
    index = 1
    while index < len(args):
        arg = args[index]
        if arg == "--repo" and index + 1 < len(args):
            repo = args[index + 1]
            index += 2
            continue
        if arg == "--auto":
            auto_merge = True
            index += 1
            continue
        if arg == "--squash":
            squash = True
            index += 1
            continue
        if arg == "--delete-branch":
            delete_branch = True
            index += 1
            continue
        return None
    if repo is None:
        return None
    return _PullRequestCommand(
        selector=selector,
        repo=repo,
        state=None,
        limit=None,
        search=None,
        json_fields=(),
        auto_merge=auto_merge,
        squash=squash,
        delete_branch=delete_branch,
    )


def _split_repo(repo_slug: str) -> tuple[str, str]:
    if "/" not in repo_slug:
        raise _transport_error(
            operation_type="query",
            command_excerpt="repo split",
            detail=f"invalid repo slug '{repo_slug}'",
        )
    owner, repo = repo_slug.split("/", maxsplit=1)
    return owner, repo
