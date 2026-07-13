#!/usr/bin/env python3
"""Create/update the SmORFinder env and verify bundled assets."""

from __future__ import annotations

from marine_peptides.config import load_config
from marine_peptides.orf_prediction.smorfinder import ensure_smorfinder_environment


def main() -> None:
    cfg = load_config()
    payload = ensure_smorfinder_environment(cfg)
    print(f"SmORFinder preflight stamp: {cfg['smorfinder']['preflight_stamp']}")
    print(f"Asset manifest sha256: {payload['asset_manifest_sha256']}")


if __name__ == "__main__":
    main()
