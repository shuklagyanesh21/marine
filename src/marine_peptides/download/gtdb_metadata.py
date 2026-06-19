"""Load and normalize GTDB metadata TSVs (bacteria + archaea).

GTDB has no native habitat/environment field, so marine selection works off the
embedded NCBI metadata columns (isolation source, country, lat/lon, biosample).
This module just loads the relevant columns and harmonizes accessions; the
marine logic lives in :mod:`marine_peptides.download.marine_filter`.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd

# Columns we keep from the (110-column) GTDB metadata files.
KEEP_COLUMNS = [
    "accession",
    "gtdb_taxonomy",
    "gtdb_representative",
    "gtdb_genome_representative",
    "ncbi_taxonomy",
    "ncbi_organism_name",
    "ncbi_genbank_assembly_accession",
    "ncbi_biosample",
    "ncbi_bioproject",
    "ncbi_genome_category",
    "ncbi_assembly_level",
    "ncbi_country",
    "ncbi_lat_lon",
    "ncbi_isolation_source",
    "checkm_completeness",
    "checkm_contamination",
    "genome_size",
    "mimag_high_quality",
    "mimag_medium_quality",
]


def gtdb_to_ncbi_accession(accession: str) -> str:
    """Strip the GTDB ``GB_``/``RS_`` prefix to get the NCBI assembly accession.

    ``GB_GCA_000013845.2`` -> ``GCA_000013845.2`` (GenBank)
    ``RS_GCF_000013285.1`` -> ``GCF_000013285.1`` (RefSeq)
    """
    if accession.startswith(("GB_", "RS_")):
        return accession[3:]
    return accession


def load_metadata_file(path: str | Path, domain: str) -> pd.DataFrame:
    """Load one GTDB metadata TSV, keeping the marine-relevant columns."""
    df = pd.read_csv(
        path,
        sep="\t",
        usecols=lambda c: c in KEEP_COLUMNS,
        dtype=str,
        na_filter=False,
    )
    df["domain"] = domain
    return df


def load_gtdb_metadata(bac120_path: str | Path, ar122_path: str | Path) -> pd.DataFrame:
    """Load bacteria + archaea metadata into one normalized DataFrame.

    Adds ``ncbi_accession`` (download-ready) and ``source_db`` (GenBank/RefSeq).
    """
    bac = load_metadata_file(bac120_path, "Bacteria")
    arc = load_metadata_file(ar122_path, "Archaea")
    df = pd.concat([bac, arc], ignore_index=True)

    df["ncbi_accession"] = df["accession"].map(gtdb_to_ncbi_accession)
    df["source_db"] = df["accession"].str[:2].map({"RS": "RefSeq", "GB": "GenBank"})
    df["is_gtdb_representative"] = df["gtdb_representative"].str.lower().eq("t")
    return df
