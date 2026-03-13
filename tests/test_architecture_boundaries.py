"""Architecture boundary checks for the hexagonal-lite controller."""

from __future__ import annotations

import ast
from pathlib import Path
import re

from startupai_controller.adapters.board_mutation import GitHubBoardMutationAdapter
from startupai_controller.adapters.pull_requests import GitHubPullRequestAdapter
from startupai_controller.adapters.review_state import GitHubReviewStateAdapter

REPO_ROOT = Path(__file__).resolve().parent.parent
SRC_ROOT = REPO_ROOT / "src" / "startupai_controller"

ORCHESTRATOR_MODULES = (
    SRC_ROOT / "board_consumer.py",
    SRC_ROOT / "board_automation.py",
    SRC_ROOT / "board_control_plane.py",
)
GRAPH_MODULE = SRC_ROOT / "board_graph.py"
APPLICATION_ROOT = SRC_ROOT / "application"
DOMAIN_ROOT = SRC_ROOT / "domain"
PORTS_ROOT = SRC_ROOT / "ports"
HOTSPOT_MODULES = {
    "startupai_controller.adapters.pull_requests": SRC_ROOT
    / "adapters"
    / "pull_requests.py",
    "startupai_controller.consumer_review_queue_helpers": SRC_ROOT
    / "consumer_review_queue_helpers.py",
    "startupai_controller.consumer_operational_wiring": SRC_ROOT
    / "consumer_operational_wiring.py",
    "startupai_controller.application.automation.ready_wiring": APPLICATION_ROOT
    / "automation"
    / "ready_wiring.py",
    "startupai_controller.project_field_sync": SRC_ROOT / "project_field_sync.py",
}

SHIM_MODULES = {
    "startupai_controller.board_io",
    "startupai_controller.consumer_db",
    "startupai_controller.github_http",
}
ADAPTER_PREFIX = "startupai_controller.adapters"
PORTS_PREFIX = "startupai_controller.ports"
RUNTIME_PREFIX = "startupai_controller.runtime"
ENTRYPOINT_MODULES = {
    "startupai_controller.board_consumer",
    "startupai_controller.board_automation",
    "startupai_controller.board_control_plane",
}
DIRECT_MECHANISM_MODULES = {"sqlite3", "subprocess"}
THIN_ENTRY_MODULES = (
    SRC_ROOT / "board_automation.py",
    SRC_ROOT / "board_control_plane.py",
)


def _is_type_checking_test(node: ast.expr) -> bool:
    return isinstance(node, ast.Name) and node.id == "TYPE_CHECKING"


class _RuntimeImportCollector(ast.NodeVisitor):
    """Collect import statements while skipping TYPE_CHECKING-only blocks."""

    def __init__(self) -> None:
        self.imports: set[str] = set()

    def visit_If(self, node: ast.If) -> None:  # noqa: N802
        if _is_type_checking_test(node.test):
            for stmt in node.orelse:
                self.visit(stmt)
            return
        self.generic_visit(node)

    def visit_Import(self, node: ast.Import) -> None:  # noqa: N802
        self.imports.update(alias.name for alias in node.names)

    def visit_ImportFrom(self, node: ast.ImportFrom) -> None:  # noqa: N802
        if node.module:
            self.imports.add(node.module)


def _runtime_imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    collector = _RuntimeImportCollector()
    collector.visit(tree)
    return collector.imports


def _controller_runtime_imports(path: Path) -> set[str]:
    return {
        module
        for module in _runtime_imported_modules(path)
        if module.startswith("startupai_controller")
    }


def _source_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _hotspot_import_graph() -> dict[str, set[str]]:
    graph: dict[str, set[str]] = {}
    for module_name, path in HOTSPOT_MODULES.items():
        imports = _runtime_imported_modules(path)
        graph[module_name] = {
            candidate
            for candidate in HOTSPOT_MODULES
            if candidate != module_name and candidate in imports
        }
    return graph


def _function_parameter_names(path: Path, function_name: str) -> set[str]:
    tree = ast.parse(_source_text(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            names = {arg.arg for arg in node.args.args}
            names.update(arg.arg for arg in node.args.kwonlyargs)
            if node.args.vararg is not None:
                names.add(node.args.vararg.arg)
            if node.args.kwarg is not None:
                names.add(node.args.kwarg.arg)
            return names
    raise AssertionError(f"Function {function_name} not found in {path}")


def _function_length(path: Path, function_name: str) -> int:
    tree = ast.parse(_source_text(path))
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            assert node.end_lineno is not None
            return node.end_lineno - node.lineno + 1
    raise AssertionError(f"Function {function_name} not found in {path}")


def test_orchestrators_use_canonical_runtime_boundaries() -> None:
    for path in ORCHESTRATOR_MODULES:
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module
            for module in imported
            if module in SHIM_MODULES or module.startswith(ADAPTER_PREFIX)
        )
        assert (
            offending == []
        ), f"{path.name} imports concrete adapter/shim modules at runtime: {offending}"
        owns_runtime_boundary = any(
            module.startswith(PORTS_PREFIX)
            or module.startswith(RUNTIME_PREFIX)
            or module.startswith("startupai_controller.control_plane_")
            or module.startswith("startupai_controller.application.control_plane.")
            for module in imported
        )
        assert owns_runtime_boundary, (
            f"{path.name} does not import canonical ports/runtime or "
            "control-plane composition wiring"
        )


def test_board_graph_has_no_adapter_or_shim_imports() -> None:
    imported = _runtime_imported_modules(GRAPH_MODULE)
    offending = sorted(
        module
        for module in imported
        if module in SHIM_MODULES or module.startswith(ADAPTER_PREFIX)
    )
    assert offending == [], f"board_graph.py leaks outer dependencies: {offending}"


def test_domain_has_no_port_adapter_or_shim_imports() -> None:
    for path in DOMAIN_ROOT.glob("*.py"):
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module
            for module in imported
            if module in SHIM_MODULES
            or module.startswith(ADAPTER_PREFIX)
            or module.startswith(PORTS_PREFIX)
        )
        assert offending == [], f"{path.name} imports outer layers: {offending}"


def test_application_has_no_entrypoint_adapter_or_shim_imports() -> None:
    for path in APPLICATION_ROOT.rglob("*.py"):
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module
            for module in imported
            if module in SHIM_MODULES
            or module.startswith(ADAPTER_PREFIX)
            or module.startswith(RUNTIME_PREFIX)
            or module in ENTRYPOINT_MODULES
            or module in {"sqlite3", "subprocess"}
        )
        assert offending == [], f"{path.name} imports outer layers: {offending}"


def test_board_control_plane_does_not_import_private_consumer_helpers() -> None:
    imported = _runtime_imported_modules(SRC_ROOT / "board_control_plane.py")
    assert (
        "startupai_controller.board_consumer" not in imported
    ), "board_control_plane.py still imports board_consumer.py at runtime"
    assert (
        "startupai_controller.board_automation" not in imported
    ), "board_control_plane.py still imports board_automation.py at runtime"


def test_board_control_plane_imports_stay_within_control_plane_stack() -> None:
    imported = _controller_runtime_imports(SRC_ROOT / "board_control_plane.py")
    offending = sorted(
        module
        for module in imported
        if module
        not in {
            "startupai_controller.board_automation_config",
            "startupai_controller.consumer_config",
            "startupai_controller.consumer_workflow",
            "startupai_controller.runtime.wiring",
            "startupai_controller.validate_critical_path_promotion",
        }
        and not module.startswith("startupai_controller.application.control_plane.")
        and not module.startswith("startupai_controller.control_plane_")
    )
    assert offending == [], (
        "board_control_plane.py imports modules outside the control-plane shell stack: "
        f"{offending}"
    )


def test_control_plane_rescue_routes_review_queue_through_wiring_module() -> None:
    imported = _controller_runtime_imports(SRC_ROOT / "control_plane_rescue.py")
    assert (
        "startupai_controller.consumer_review_queue_helpers" not in imported
    ), "control_plane_rescue.py still imports review-queue helpers directly"
    assert (
        "startupai_controller.consumer_review_queue_wiring" in imported
    ), "control_plane_rescue.py should depend on review-queue wiring"


def test_control_plane_rescue_routes_deferred_actions_through_wiring_module() -> None:
    imported = _controller_runtime_imports(SRC_ROOT / "control_plane_rescue.py")
    assert (
        "startupai_controller.consumer_deferred_action_helpers" not in imported
    ), "control_plane_rescue.py still imports deferred-action helpers directly"
    assert (
        "startupai_controller.consumer_deferred_action_wiring" in imported
    ), "control_plane_rescue.py should depend on deferred-action wiring"


def test_consumer_deferred_action_helpers_stay_shell_agnostic() -> None:
    imported = _controller_runtime_imports(
        SRC_ROOT / "consumer_deferred_action_helpers.py"
    )
    offending = sorted(
        module
        for module in imported
        if module
        in {
            "startupai_controller.consumer_automation_bridge",
            "startupai_controller.consumer_board_state_helpers",
            "startupai_controller.consumer_codex_comment_wiring",
            "startupai_controller.board_graph",
        }
    )
    assert offending == [], (
        "consumer_deferred_action_helpers.py still owns shell wiring imports: "
        f"{offending}"
    )


def test_board_automation_does_not_cross_into_consumer_or_control_plane_stacks() -> (
    None
):
    imported = _controller_runtime_imports(SRC_ROOT / "board_automation.py")
    offending = sorted(
        module
        for module in imported
        if module
        in {
            "startupai_controller.board_consumer",
            "startupai_controller.board_control_plane",
        }
        or module.startswith("startupai_controller.application.consumer.")
        or module.startswith("startupai_controller.consumer_")
        or module.startswith("startupai_controller.control_plane_")
    )
    assert offending == [], (
        "board_automation.py crosses into other shell stacks at runtime: "
        f"{offending}"
    )


def test_board_consumer_endgame_shell_avoids_lower_level_execution_mechanisms() -> None:
    imported = _controller_runtime_imports(SRC_ROOT / "board_consumer.py")
    assert (
        "startupai_controller.board_automation" not in imported
    ), "board_consumer.py still imports board_automation.py at runtime"
    offending = sorted(
        module
        for module in imported
        if module
        in {
            "startupai_controller.application.consumer.execution",
            "startupai_controller.application.consumer.launch",
            "startupai_controller.consumer_review_handoff_helpers",
        }
    )
    assert offending == [], (
        "board_consumer.py bypasses outer execution/launch wiring modules: "
        f"{offending}"
    )


def test_only_board_consumer_cli_imports_the_consumer_shell() -> None:
    direct_importers: list[str] = []
    direct_import_patterns = (
        re.compile(r"\bfrom startupai_controller\.board_consumer import\b"),
        re.compile(r"\bfrom startupai_controller import board_consumer(?:\s|$|,)"),
    )
    for path in sorted(SRC_ROOT.glob("*.py")):
        if path.name == "board_consumer.py":
            continue
        source = _source_text(path)
        if any(pattern.search(source) for pattern in direct_import_patterns):
            direct_importers.append(path.name)
    assert direct_importers == ["board_consumer_cli.py"], (
        "board_consumer.py should only be imported directly by its CLI shell: "
        f"{direct_importers}"
    )


def test_consumer_stack_has_no_compat_or_shell_service_locator_patterns() -> None:
    offending_compat_imports: list[str] = []
    offending_service_locator_usage: list[str] = []
    for path in sorted(SRC_ROOT.glob("consumer*.py")):
        source = _source_text(path)
        if "board_consumer_compat" in source:
            offending_compat_imports.append(path.name)
        if "_shell_module(" in source or "shell=sys.modules[__name__]" in source:
            offending_service_locator_usage.append(path.name)
    assert offending_compat_imports == [], (
        "consumer modules still depend on board_consumer_compat: "
        f"{offending_compat_imports}"
    )
    assert offending_service_locator_usage == [], (
        "consumer modules still use shell service-locator patterns: "
        f"{offending_service_locator_usage}"
    )


def test_consumer_prepared_cycle_use_case_has_no_legacy_board_or_comment_callables() -> (
    None
):
    params = _function_parameter_names(
        APPLICATION_ROOT / "consumer" / "cycle.py",
        "run_prepared_cycle",
    )
    forbidden = {
        "status_resolver",
        "board_info_resolver",
        "board_mutator",
        "comment_checker",
        "comment_poster",
    }
    offending = sorted(params & forbidden)
    assert offending == [], (
        "run_prepared_cycle() regained legacy board/comment callable seams: "
        f"{offending}"
    )


def test_consumer_reconciliation_use_case_has_no_legacy_board_or_gh_callables() -> None:
    params = _function_parameter_names(
        APPLICATION_ROOT / "consumer" / "preflight.py",
        "reconcile_board_truth",
    )
    forbidden = {
        "board_info_resolver",
        "board_mutator",
        "gh_runner",
    }
    offending = sorted(params & forbidden)
    assert offending == [], (
        "reconcile_board_truth() regained legacy board/gh callable seams: "
        f"{offending}"
    )


def test_consumer_launch_use_case_has_no_legacy_board_or_status_callables() -> None:
    params = _function_parameter_names(
        APPLICATION_ROOT / "consumer" / "launch.py",
        "prepare_launch_candidate",
    )
    forbidden = {
        "status_resolver",
        "board_info_resolver",
        "board_mutator",
    }
    offending = sorted(params & forbidden)
    assert offending == [], (
        "prepare_launch_candidate() regained legacy board/status callable seams: "
        f"{offending}"
    )


def test_consumer_recovery_use_case_has_no_legacy_board_or_gh_callables() -> None:
    params = _function_parameter_names(
        APPLICATION_ROOT / "consumer" / "recovery.py",
        "recover_interrupted_sessions",
    )
    forbidden = {
        "board_info_resolver",
        "board_mutator",
        "gh_runner",
    }
    offending = sorted(params & forbidden)
    assert offending == [], (
        "recover_interrupted_sessions() regained legacy board/gh callable seams: "
        f"{offending}"
    )


def test_consumer_recovery_shell_has_no_dead_board_callable_kwargs() -> None:
    params = _function_parameter_names(
        SRC_ROOT / "consumer_operational_wiring.py",
        "recover_interrupted_sessions",
    )
    forbidden = {
        "board_info_resolver",
        "board_mutator",
    }
    offending = sorted(params & forbidden)
    assert offending == [], (
        "consumer_operational_wiring.recover_interrupted_sessions() regained "
        f"dead legacy board kwargs: {offending}"
    )


def test_consumer_operational_wiring_routes_recovery_reconciliation_through_wiring_module() -> (
    None
):
    imported = _controller_runtime_imports(SRC_ROOT / "consumer_operational_wiring.py")
    assert (
        "startupai_controller.consumer_reconciliation_wiring" in imported
    ), "consumer_operational_wiring.py should depend on consumer_reconciliation_wiring"
    offending = sorted(
        module
        for module in imported
        if module
        in {
            "startupai_controller.consumer_recovery_helpers",
            "startupai_controller.application.consumer.reconciliation",
            "startupai_controller.application.consumer.recovery",
        }
    )
    assert offending == [], (
        "consumer_operational_wiring.py still owns recovery/reconciliation wiring: "
        f"{offending}"
    )


def test_consumer_preflight_runtime_module_has_no_port_builder_fields() -> None:
    source = _source_text(APPLICATION_ROOT / "consumer" / "preflight_runtime.py")
    forbidden = [
        "build_session_store:",
        "build_github_port_bundle:",
        "build_ready_flow_port:",
        "build_gh_runner_port:",
        "cycle_github_memo_factory:",
    ]
    offending = [token for token in forbidden if token in source]
    assert offending == [], (
        "application/consumer/preflight_runtime.py regained port-builder seams: "
        f"{offending}"
    )


def test_consumer_status_module_has_no_db_or_github_query_builder_fields() -> None:
    source = _source_text(APPLICATION_ROOT / "consumer" / "status.py")
    forbidden = [
        "open_consumer_db:",
        "list_project_items_by_status:",
    ]
    offending = [token for token in forbidden if token in source]
    assert offending == [], (
        "application/consumer/status.py regained DB/GitHub builder seams: "
        f"{offending}"
    )


def test_consumer_daemon_module_has_no_db_builder_fields() -> None:
    source = _source_text(APPLICATION_ROOT / "consumer" / "daemon.py")
    forbidden = ["open_consumer_db"]
    offending = [token for token in forbidden if token in source]
    assert offending == [], (
        "application/consumer/daemon.py regained DB builder seams: " f"{offending}"
    )


def test_consumer_launch_module_has_no_legacy_status_resolver_usage() -> None:
    source = _source_text(APPLICATION_ROOT / "consumer" / "launch.py")
    forbidden = ["status_resolver"]
    offending = [token for token in forbidden if token in source]
    assert offending == [], (
        "application/consumer/launch.py regained legacy status-resolver seams: "
        f"{offending}"
    )


def test_rebalance_application_module_no_longer_contains_outer_wiring_entry_points() -> (
    None
):
    source = _source_text(APPLICATION_ROOT / "automation" / "rebalance.py")
    assert (
        "def load_rebalance_in_progress_items(" not in source
    ), "application/automation/rebalance.py regained outer port-materialization helpers"
    assert (
        "def wire_rebalance_wip(" not in source
    ), "application/automation/rebalance.py regained outer rebalance wiring"


def test_automation_non_wiring_modules_have_no_default_port_or_wire_entrypoints() -> (
    None
):
    forbidden = [
        "default_pr_port_fn",
        "default_review_state_port_fn",
        "default_board_mutation_port_fn",
        "list_project_items_by_status",
        "def wire_",
    ]
    offending: dict[str, list[str]] = {}
    for path in sorted((APPLICATION_ROOT / "automation").glob("*.py")):
        if path.name.endswith("wiring.py"):
            continue
        source = _source_text(path)
        hits = [token for token in forbidden if token in source]
        if hits:
            offending[path.name] = hits
    assert offending == {}, (
        "Non-wiring automation application modules regained outer composition seams: "
        f"{offending}"
    )


def test_board_automation_does_not_construct_github_port_bundles_inline() -> None:
    source = _source_text(SRC_ROOT / "board_automation.py")
    assert (
        "build_github_port_bundle(" not in source
    ), "board_automation.py still constructs GitHub port bundles inline"
    forbidden_defs = [
        "def _ensure_github_bundle(",
        "def _default_pr_port(",
        "def _default_review_state_port(",
        "def _default_board_mutation_port(",
        "def _default_issue_context_port(",
    ]
    offending = [token for token in forbidden_defs if token in source]
    assert offending == [], (
        "board_automation.py still owns default port factory helpers: " f"{offending}"
    )


def test_board_control_plane_tick_does_not_construct_tickdeps_inline() -> None:
    source = _source_text(SRC_ROOT / "board_control_plane.py")
    assert (
        "TickDeps(" not in source
    ), "board_control_plane._tick still constructs TickDeps inline"


def test_ready_and_review_wiring_do_not_define_inline_compat_wrappers() -> None:
    offending: dict[str, list[str]] = {}
    for path in (
        APPLICATION_ROOT / "automation" / "ready_wiring.py",
        APPLICATION_ROOT / "automation" / "review_wiring.py",
    ):
        source = _source_text(path)
        hits = [
            token
            for token in ("def _wrap_", "class _DelegatingPort")
            if token in source
        ]
        if hits:
            offending[path.name] = hits
    assert offending == {}, (
        "major wiring modules still define inline compatibility wrappers: "
        f"{offending}"
    )


def test_outer_wiring_hotspots_stay_under_refined_size_ceiling() -> None:
    limits = {
        (
            APPLICATION_ROOT / "automation" / "ready_wiring.py",
            "auto_promote_successors",
        ): 70,
        (APPLICATION_ROOT / "automation" / "review_wiring.py", "sync_review_state"): 60,
        (SRC_ROOT / "board_control_plane.py", "_tick"): 20,
    }
    offending: dict[str, int] = {}
    for (path, function_name), limit in limits.items():
        length = _function_length(path, function_name)
        if length > limit:
            offending[f"{path.name}:{function_name}"] = length
    assert offending == {}, (
        "outer wiring hotspots regressed in size/complexity: " f"{offending}"
    )


def test_thin_entrypoints_do_not_import_direct_mechanism_modules() -> None:
    for path in THIN_ENTRY_MODULES:
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module for module in imported if module in DIRECT_MECHANISM_MODULES
        )
        assert (
            offending == []
        ), f"{path.name} imports direct mechanism modules: {offending}"


def test_ports_do_not_import_adapters() -> None:
    for path in PORTS_ROOT.glob("*.py"):
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module for module in imported if module.startswith(ADAPTER_PREFIX)
        )
        assert offending == [], f"{path.name} imports adapters: {offending}"


def test_capability_adapters_do_not_inherit_each_other() -> None:
    assert not issubclass(GitHubReviewStateAdapter, GitHubBoardMutationAdapter)
    assert not issubclass(GitHubPullRequestAdapter, GitHubReviewStateAdapter)


def test_official_hotspot_set_has_no_import_cycles() -> None:
    graph = _hotspot_import_graph()
    visiting: set[str] = set()
    visited: set[str] = set()

    def _walk(node: str, trail: tuple[str, ...]) -> None:
        if node in visiting:
            raise AssertionError(
                "Hotspot import cycle detected: " + " -> ".join((*trail, node))
            )
        if node in visited:
            return
        visiting.add(node)
        for neighbor in sorted(graph[node]):
            _walk(neighbor, (*trail, node))
        visiting.remove(node)
        visited.add(node)

    for module_name in sorted(graph):
        _walk(module_name, ())


# --- Shim governance tests ---

# Frozen allowlist: only these files may import shim modules at runtime.
# When a PR removes a shim caller, remove it from this dict in the same PR.
KNOWN_SHIM_IMPORTERS = {}

# Baselines: current line counts for each shim file.
# When a PR shrinks a shim, update the baseline downward in the same PR.
SHIM_SIZE_BASELINES = {
    "board_io.py": 1461,
    "consumer_db.py": 11,
    "github_http.py": 11,
}

SHIM_FILENAMES = {m.rsplit(".", 1)[-1] + ".py" for m in SHIM_MODULES}


def test_no_new_shim_importers() -> None:
    """No file outside the frozen allowlist may import a shim module at runtime."""
    violations: dict[str, set[str]] = {}
    for path in sorted(SRC_ROOT.rglob("*.py")):
        if path.name in SHIM_FILENAMES or path.name == "__init__.py":
            continue
        imported = _runtime_imported_modules(path)
        shim_hits = imported & SHIM_MODULES
        if not shim_hits:
            continue
        allowed = KNOWN_SHIM_IMPORTERS.get(path.name, set())
        unexpected = shim_hits - allowed
        if unexpected:
            violations[path.name] = unexpected
    assert violations == {}, (
        "New shim importers detected (not in KNOWN_SHIM_IMPORTERS allowlist). "
        "Migrate to ports/adapters instead of adding shim dependencies: "
        f"{violations}"
    )


def test_shim_size_ratchet() -> None:
    """Shim files may not grow beyond their recorded baselines."""
    for filename, baseline in SHIM_SIZE_BASELINES.items():
        path = SRC_ROOT / filename
        actual = len(path.read_text(encoding="utf-8").splitlines())
        assert actual <= baseline, (
            f"{filename} has {actual} lines, exceeding baseline of {baseline}. "
            "Shims must shrink monotonically. If you shrank it, update "
            "SHIM_SIZE_BASELINES in this test."
        )
