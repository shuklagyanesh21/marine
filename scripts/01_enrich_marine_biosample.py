#!/usr/bin/env python3
"""Tier 2: enrich the query pool with NCBI BioSample env attributes (no downloads).

Fetches BioSample ``env_*``/isolation/geo attributes for the Tier 2 pool,
classifies them as marine, and writes the Tier 2 marine candidates. Network
results are cached, so this is safe to re-run / resume.
"""

from __future__ import annotations

import pandas as pd

from marine_peptides.config import load_config, resolve_path
from marine_peptides.download.biosample import EXTRACT_FIELDS, fetch_biosample_table
from marine_peptides.download.marine_filter import classify_dataframe


def main() -> None:
    cfg = load_config()
    m = cfg["marine"]
    t2cfg = m["tier2"]

    if not t2cfg.get("enabled", True):
        print("Tier 2 disabled in config; nothing to do.")
        return

    pool = pd.read_csv(resolve_path(m["tier2_pool"]), sep="\t", dtype=str, na_filter=False)
    ids = sorted(pool["ncbi_biosample"].unique())
    print(f"Tier 2 pool: {len(pool):,} genomes / {len(ids):,} BioSamples to resolve")

    bs = fetch_biosample_table(
        ids,
        cfg_tier2=t2cfg,
        cache_path=resolve_path(m["biosample_cache"]),
        logger=print,
    )
    print(f"Fetched attributes for {len(bs):,} BioSamples")

    bs = classify_dataframe(
        bs,
        fields=t2cfg["match_fields"],
        include_keywords=m["include_keywords"],
        exclude_keywords=m["exclude_keywords"],
        host_keywords=m["host_keywords"],
        prefix="marine_tier2",
    )
    marine_bs = bs[bs["marine_tier2"]].copy()
    print(f"Marine-positive BioSamples: {len(marine_bs):,}")

    # Map BioSample env data back onto pool genomes (prefix bs_ for the manifest).
    bs_cols = {f: f"bs_{f}" for f in EXTRACT_FIELDS}
    marine_bs = marine_bs.rename(columns={**bs_cols, "marine_tier2_evidence": "marine_evidence"})
    keep = ["biosample", *bs_cols.values(), "marine_evidence", "is_host_associated"]
    merged = pool.merge(
        marine_bs[keep], left_on="ncbi_biosample", right_on="biosample", how="inner"
    )
    merged["marine_tier"] = 2

    out_path = resolve_path(m["candidates_tier2"])
    merged.to_csv(out_path, sep="\t", index=False)
    print(f"Tier 2 marine candidates: {len(merged):,} genomes "
          f"({(merged['is_gtdb_representative'].astype(str).str.lower()=='true').sum():,} reps) -> {out_path}")


if __name__ == "__main__":
    main()
