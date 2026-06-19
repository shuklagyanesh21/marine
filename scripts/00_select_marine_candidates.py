#!/usr/bin/env python3
"""Tier 1: select marine candidates from GTDB metadata (offline, no downloads).

Thin wrapper around :mod:`marine_peptides.download`. Writes:
  * Tier 1 marine candidates (keyword hits in GTDB metadata)
  * the Tier 2 query pool (environmental genomes with a BioSample, not yet
    flagged marine) and its unique BioSample IDs for the enrichment step.
"""

from __future__ import annotations

from pathlib import Path

from marine_peptides.config import load_config, resolve_path
from marine_peptides.download.gtdb_metadata import load_gtdb_metadata
from marine_peptides.download.marine_filter import classify_dataframe


def main() -> None:
    cfg = load_config()
    m = cfg["marine"]

    df = load_gtdb_metadata(
        resolve_path(cfg["gtdb"]["bac120_metadata"]),
        resolve_path(cfg["gtdb"]["ar122_metadata"]),
    )
    print(f"Loaded {len(df):,} GTDB genomes ({(df.domain=='Bacteria').sum():,} bac / "
          f"{(df.domain=='Archaea').sum():,} arc)")

    df = classify_dataframe(
        df,
        fields=m["tier1"]["match_fields"],
        include_keywords=m["include_keywords"],
        exclude_keywords=m["exclude_keywords"],
        host_keywords=m["host_keywords"],
        prefix="marine_tier1",
    )

    # --- Tier 1 candidates ------------------------------------------------ #
    t1 = df[df["marine_tier1"]].copy()
    t1["marine_tier"] = 1
    t1["marine_evidence"] = t1["marine_tier1_evidence"]
    t1_path = resolve_path(m["candidates_tier1"])
    t1_path.parent.mkdir(parents=True, exist_ok=True)
    t1.to_csv(t1_path, sep="\t", index=False)
    print(f"Tier 1 marine candidates: {len(t1):,} "
          f"({t1['is_gtdb_representative'].sum():,} representatives) -> {t1_path}")

    # --- Tier 2 query pool ------------------------------------------------ #
    cats = set(m["tier2"]["genome_categories"])
    has_biosample = ~df["ncbi_biosample"].str.strip().str.lower().isin({"", "none", "na"})
    pool = df[(~df["marine_tier1"]) & df["ncbi_genome_category"].isin(cats) & has_biosample].copy()
    pool_path = resolve_path(m["tier2_pool"])
    pool.to_csv(pool_path, sep="\t", index=False)

    ids = sorted(pool["ncbi_biosample"].unique())
    ids_path = resolve_path(m["tier2_query_ids"])
    Path(ids_path).write_text("\n".join(ids) + "\n")
    print(f"Tier 2 query pool: {len(pool):,} genomes / {len(ids):,} unique BioSamples -> {pool_path}")


if __name__ == "__main__":
    main()
