"""Build the committed marine genome provenance manifest.

The manifest is the single source of truth for *which* genomes we will download
and *why* they are considered marine. It deliberately contains no sequence data
(see AGENTS.md: commit the manifest, not the genomes).
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from marine_peptides.download.marine_filter import parse_depth_m, parse_lat_lon

# Final manifest column order.
MANIFEST_COLUMNS = [
    "ncbi_accession",
    "gtdb_accession",
    "source_db",
    "domain",
    "gtdb_taxonomy",
    "ncbi_organism_name",
    "ncbi_genome_category",
    "is_gtdb_representative",
    "gtdb_genome_representative",
    "checkm_completeness",
    "checkm_contamination",
    "genome_size",
    "ncbi_assembly_level",
    "mimag_high_quality",
    "ncbi_biosample",
    "ncbi_bioproject",
    "ncbi_country",
    "geo_loc_name",
    "lat",
    "lon",
    "depth_m",
    "isolation_source",
    "env_broad_scale",
    "env_local_scale",
    "env_medium",
    "is_host_associated",
    "marine_tier",
    "marine_evidence",
]

# (gtdb column, biosample column) coalesce pairs: prefer biosample (Tier 2) value.
_GTDB_DEFAULT = ""


def _coalesce(df: pd.DataFrame, primary: str, fallback: str) -> pd.Series:
    """Return ``primary`` where non-empty, else ``fallback`` (missing-safe)."""
    p = df[primary] if primary in df.columns else pd.Series(_GTDB_DEFAULT, index=df.index)
    f = df[fallback] if fallback in df.columns else pd.Series(_GTDB_DEFAULT, index=df.index)
    p = p.fillna("").astype(str)
    f = f.fillna("").astype(str)
    return p.where(p.str.strip() != "", f)


def build_manifest(df: pd.DataFrame) -> pd.DataFrame:
    """Assemble the final manifest from a combined Tier 1 + Tier 2 candidate set.

    The input may contain GTDB columns plus optional BioSample columns
    (``bs_*``). Missing columns are tolerated.
    """
    out = pd.DataFrame(index=df.index)

    out["ncbi_accession"] = df.get("ncbi_accession", "")
    out["gtdb_accession"] = df.get("accession", "")
    out["source_db"] = df.get("source_db", "")
    out["domain"] = df.get("domain", "")
    out["gtdb_taxonomy"] = df.get("gtdb_taxonomy", "")
    out["ncbi_organism_name"] = df.get("ncbi_organism_name", "")
    out["ncbi_genome_category"] = df.get("ncbi_genome_category", "")
    out["is_gtdb_representative"] = df.get("is_gtdb_representative", False)
    out["gtdb_genome_representative"] = df.get("gtdb_genome_representative", "")
    out["checkm_completeness"] = df.get("checkm_completeness", "")
    out["checkm_contamination"] = df.get("checkm_contamination", "")
    out["genome_size"] = df.get("genome_size", "")
    out["ncbi_assembly_level"] = df.get("ncbi_assembly_level", "")
    out["mimag_high_quality"] = df.get("mimag_high_quality", "")
    out["ncbi_biosample"] = df.get("ncbi_biosample", "")
    out["ncbi_bioproject"] = df.get("ncbi_bioproject", "")
    out["ncbi_country"] = df.get("ncbi_country", "")

    # BioSample-derived environmental context (Tier 2 only; empty for Tier 1).
    out["geo_loc_name"] = df.get("bs_geo_loc_name", "")
    out["env_broad_scale"] = df.get("bs_env_broad_scale", "")
    out["env_local_scale"] = df.get("bs_env_local_scale", "")
    out["env_medium"] = df.get("bs_env_medium", "")

    # Coalesced fields: prefer BioSample value, fall back to GTDB metadata.
    isolation = _coalesce(df, "bs_isolation_source", "ncbi_isolation_source")
    out["isolation_source"] = isolation

    lat_lon_src = _coalesce(df, "bs_lat_lon", "ncbi_lat_lon")
    latlon = lat_lon_src.map(parse_lat_lon)
    out["lat"] = latlon.map(lambda t: t[0])
    out["lon"] = latlon.map(lambda t: t[1])
    depth_src = df.get("bs_depth", pd.Series("", index=df.index)).fillna("").astype(str)
    out["depth_m"] = depth_src.map(parse_depth_m)

    out["is_host_associated"] = df.get("is_host_associated", False)
    out["marine_tier"] = df.get("marine_tier", "")
    out["marine_evidence"] = df.get("marine_evidence", "")

    out = out[MANIFEST_COLUMNS]
    out = out.drop_duplicates(subset="ncbi_accession")
    out = out.sort_values(["domain", "gtdb_taxonomy", "ncbi_accession"]).reset_index(drop=True)
    return out


def write_manifest(df: pd.DataFrame, path: str | Path) -> Path:
    """Write the manifest TSV, creating parent directories as needed."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, sep="\t", index=False)
    return path
