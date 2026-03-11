"""Architecture boundary checks for the hexagonal-lite controller."""

from __future__ import annotations

import ast
from pathlib import Path

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


def test_orchestrators_use_canonical_runtime_boundaries() -> None:
    for path in ORCHESTRATOR_MODULES:
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module
            for module in imported
            if module in SHIM_MODULES or module.startswith(ADAPTER_PREFIX)
        )
        assert offending == [], (
            f"{path.name} imports concrete adapter/shim modules at runtime: {offending}"
        )
        assert any(
            module.startswith(PORTS_PREFIX) or module.startswith(RUNTIME_PREFIX)
            for module in imported
        ), f"{path.name} does not import canonical ports/runtime wiring"


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
    assert "startupai_controller.board_consumer" not in imported, (
        "board_control_plane.py still imports board_consumer.py at runtime"
    )
    assert "startupai_controller.board_automation" not in imported, (
        "board_control_plane.py still imports board_automation.py at runtime"
    )


def test_board_control_plane_imports_stay_within_control_plane_stack() -> None:
    imported = _controller_runtime_imports(SRC_ROOT / "board_control_plane.py")
    offending = sorted(
        module
        for module in imported
        if module not in {
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


def test_board_automation_does_not_cross_into_consumer_or_control_plane_stacks() -> None:
    imported = _controller_runtime_imports(SRC_ROOT / "board_automation.py")
    offending = sorted(
        module
        for module in imported
        if module in {
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
    offending = sorted(
        module
        for module in imported
        if module in {
            "startupai_controller.application.consumer.execution",
            "startupai_controller.application.consumer.launch",
            "startupai_controller.consumer_review_handoff_helpers",
        }
    )
    assert offending == [], (
        "board_consumer.py bypasses outer execution/launch wiring modules: "
        f"{offending}"
    )


def test_thin_entrypoints_do_not_import_direct_mechanism_modules() -> None:
    for path in THIN_ENTRY_MODULES:
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module for module in imported if module in DIRECT_MECHANISM_MODULES
        )
        assert offending == [], (
            f"{path.name} imports direct mechanism modules: {offending}"
        )


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
