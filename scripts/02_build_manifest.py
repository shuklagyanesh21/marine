#!/usr/bin/env python3
"""Merge Tier 1 + Tier 2 marine candidates into the committed manifest TSV.

This decides WHICH genomes to download later; it does not download anything.
"""

from __future__ import annotations

import pandas as pd

from marine_peptides.config import load_config, resolve_path
from marine_peptides.download.manifest import build_manifest, write_manifest


def _read(path) -> pd.DataFrame:
    p = resolve_path(path)
    if not p.exists():
        return pd.DataFrame()
    return pd.read_csv(p, sep="\t", dtype=str, na_filter=False)


def main() -> None:
    cfg = load_config()
    m = cfg["marine"]

    t1 = _read(m["candidates_tier1"])
    t2 = _read(m["candidates_tier2"])
    print(f"Tier 1 candidates: {len(t1):,} | Tier 2 candidates: {len(t2):,}")

    combined = pd.concat([t1, t2], ignore_index=True)
    if combined.empty:
        raise SystemExit("No candidates found; run scripts 00 (and 01) first.")

    manifest = build_manifest(combined)
    out = write_manifest(manifest, resolve_path(m["manifest"]))

    n_rep = (manifest["is_gtdb_representative"].astype(str).str.lower() == "true").sum()
    n_host = (manifest["is_host_associated"].astype(str).str.lower() == "true").sum()
    by_tier = manifest["marine_tier"].value_counts().to_dict()
    by_domain = manifest["domain"].value_counts().to_dict()
    print(f"Manifest: {len(manifest):,} genomes -> {out}")
    print(f"  by tier: {by_tier}")
    print(f"  by domain: {by_domain}")
    print(f"  representatives: {n_rep:,} | host-associated (tagged): {n_host:,}")


if __name__ == "__main__":
    main()
