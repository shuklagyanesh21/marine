"""Canonical genome identifiers and filenames for the standardized layer."""

from __future__ import annotations

import re
from pathlib import Path

ACCESSION_RE = re.compile(r"^(GC[AF]_\d+)\.(\d+)$")
OCEANDNA_PREFIX = "OceanDNA-"


def rewrite_version(identifier: str) -> str:
    """Rewrite ``.N`` accession suffixes to ``vN`` for filename safety."""
    match = ACCESSION_RE.match(identifier)
    if not match:
        return identifier
    return f"{match.group(1)}v{match.group(2)}"


def normalize_oceandna_id(source_id: str) -> str:
    """Strip the OceanDNA filename prefix used in raw downloads."""
    return source_id[len(OCEANDNA_PREFIX):] if source_id.startswith(OCEANDNA_PREFIX) else source_id


def canonical_source_token(source: str, source_id: str, ncbi_accession: str = "") -> str:
    """Return the source-specific identifier token used in canonical IDs."""
    if source in {"ncbi", "marref", "mardb"}:
        token = ncbi_accession or source_id
        if not token:
            raise ValueError(f"{source} records require an accession or source_id")
        return rewrite_version(token)
    if source == "oceandna":
        return normalize_oceandna_id(source_id)
    return source_id


def make_canonical_id(source: str, source_id: str, ncbi_accession: str = "") -> str:
    """Build the semantic-prefix canonical identifier (Option B)."""
    token = canonical_source_token(source, source_id, ncbi_accession=ncbi_accession)
    return f"{source}_{token}"


def canonical_filename(canonical_id: str, raw_path: str | Path) -> str:
    """Return the standardized FASTA filename while preserving compression."""
    name = Path(raw_path).name
    if name.endswith(".fna.gz"):
        return f"{canonical_id}.fna.gz"
    if name.endswith(".fasta.gz"):
        return f"{canonical_id}.fasta.gz"
    if name.endswith(".fa.gz"):
        return f"{canonical_id}.fa.gz"
    if name.endswith(".fna"):
        return f"{canonical_id}.fna"
    if name.endswith(".fasta"):
        return f"{canonical_id}.fasta"
    if name.endswith(".fa"):
        return f"{canonical_id}.fa"
    return f"{canonical_id}{Path(raw_path).suffix}"
