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
DOMAIN_ROOT = SRC_ROOT / "domain"
PORTS_ROOT = SRC_ROOT / "ports"

SHIM_MODULES = {
    "startupai_controller.board_io",
    "startupai_controller.consumer_db",
    "startupai_controller.github_http",
}
ADAPTER_PREFIX = "startupai_controller.adapters"
PORTS_PREFIX = "startupai_controller.ports"


def _imported_modules(path: Path) -> set[str]:
    tree = ast.parse(path.read_text(encoding="utf-8"))
    imports: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            imports.update(alias.name for alias in node.names)
        elif isinstance(node, ast.ImportFrom) and node.module:
            imports.add(node.module)
    return imports


def test_orchestrators_do_not_import_shim_modules() -> None:
    for path in ORCHESTRATOR_MODULES:
        imported = _imported_modules(path)
        offending = sorted(imported & SHIM_MODULES)
        assert offending == [], f"{path.name} imports shim modules: {offending}"


def test_board_graph_has_no_adapter_or_shim_imports() -> None:
    imported = _imported_modules(GRAPH_MODULE)
    offending = sorted(
        module
        for module in imported
        if module in SHIM_MODULES or module.startswith(ADAPTER_PREFIX)
    )
    assert offending == [], f"board_graph.py leaks outer dependencies: {offending}"


def test_domain_has_no_port_adapter_or_shim_imports() -> None:
    for path in DOMAIN_ROOT.glob("*.py"):
        imported = _imported_modules(path)
        offending = sorted(
            module
            for module in imported
            if module in SHIM_MODULES
            or module.startswith(ADAPTER_PREFIX)
            or module.startswith(PORTS_PREFIX)
        )
        assert offending == [], f"{path.name} imports outer layers: {offending}"


def test_ports_do_not_import_adapters() -> None:
    for path in PORTS_ROOT.glob("*.py"):
        imported = _imported_modules(path)
        offending = sorted(
            module for module in imported if module.startswith(ADAPTER_PREFIX)
        )
        assert offending == [], f"{path.name} imports adapters: {offending}"
