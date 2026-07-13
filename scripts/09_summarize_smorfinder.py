#!/usr/bin/env python3
"""Summarize SmORFinder outputs into a processed TSV manifest."""

from __future__ import annotations

from marine_peptides.config import load_config
from marine_peptides.orf_prediction.smorfinder import summarize_run_manifest


def main() -> None:
    cfg = load_config()
    manifest_path = summarize_run_manifest(cfg)
    print(f"SmORFinder run manifest: {manifest_path}")


if __name__ == "__main__":
    main()
