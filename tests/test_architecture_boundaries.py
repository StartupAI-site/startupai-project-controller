"""Architecture boundary checks for the hexagonal-lite controller."""

from __future__ import annotations

import ast
from pathlib import Path


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
        )
        assert offending == [], f"{path.name} imports outer layers: {offending}"


def test_ports_do_not_import_adapters() -> None:
    for path in PORTS_ROOT.glob("*.py"):
        imported = _runtime_imported_modules(path)
        offending = sorted(
            module for module in imported if module.startswith(ADAPTER_PREFIX)
        )
        assert offending == [], f"{path.name} imports adapters: {offending}"
