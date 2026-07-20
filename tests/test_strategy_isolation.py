"""The strategy must NEVER import from api/ or data/live.

This is enforced two ways:
  1. import-linter (configured in pyproject.toml) — runs via `lint-imports`
  2. A static check here that the strategy module's top-level imports don't
     reach into forbidden namespaces. Faster than booting the linter.
"""
from __future__ import annotations

import ast
from pathlib import Path


SRC = Path(__file__).resolve().parents[1] / "src" / "lnmarkets_bot" / "strategy"


def _iter_imports(tree: ast.Module) -> list[tuple[str, int]]:
    out: list[tuple[str, int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.append((alias.name, node.lineno))
        elif isinstance(node, ast.ImportFrom):
            if node.module is None:
                continue
            # PEP 660 implicit relative imports aren't used here; module always absolute.
            for alias in node.names:
                full = f"{node.module}.{alias.name}"
                out.append((full, node.lineno))
    return out


def test_strategy_does_not_import_api() -> None:
    for py in sorted(SRC.rglob("*.py")):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for name, lineno in _iter_imports(tree):
            assert not name.startswith("lnmarkets_bot.api"), (
                f"{py.relative_to(SRC)}:{lineno} imports from api: {name}"
            )


def test_strategy_does_not_import_data_live_or_engine_live() -> None:
    forbidden = ("lnmarkets_bot.data.live", "lnmarkets_bot.engine.live")
    for py in sorted(SRC.rglob("*.py")):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for name, lineno in _iter_imports(tree):
            assert not any(name.startswith(f) for f in forbidden), (
                f"{py.relative_to(SRC)}:{lineno} imports {name}"
            )


def test_engine_does_not_reach_api_trades_directly() -> None:
    """Engine must go through RiskGuard -> Executor, never import api.trades directly."""
    src_dir = Path(__file__).resolve().parents[1] / "src" / "lnmarkets_bot" / "engine"
    for py in sorted(src_dir.rglob("*.py")):
        tree = ast.parse(py.read_text(encoding="utf-8"))
        for name, lineno in _iter_imports(tree):
            assert not name.startswith("lnmarkets_bot.api.trades"), (
                f"engine cannot import api.trades directly ({py.name}:{lineno})"
            )
