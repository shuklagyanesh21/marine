"""Project configuration access.

Single source of truth for paths/params is ``config/config.yaml`` (see AGENTS.md).
Code reads values from here instead of hardcoding paths.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml


def project_root() -> Path:
    """Return the repository root (the directory containing ``config/``)."""
    # src/marine_peptides/config.py -> parents[2] == repo root
    return Path(__file__).resolve().parents[2]


@lru_cache(maxsize=8)
def load_config(path: str | Path | None = None) -> dict[str, Any]:
    """Load ``config/config.yaml`` (or an explicit path) as a dict."""
    cfg_path = Path(path) if path is not None else project_root() / "config" / "config.yaml"
    with open(cfg_path) as fh:
        return yaml.safe_load(fh)


def resolve_path(relative: str | Path) -> Path:
    """Resolve a config-relative path against the project root.

    Absolute paths are returned unchanged.
    """
    p = Path(relative)
    return p if p.is_absolute() else project_root() / p
