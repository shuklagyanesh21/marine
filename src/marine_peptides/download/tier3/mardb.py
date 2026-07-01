"""MarDB Tier 3 download wrapper."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from marine_peptides.download.tier3.common import Logger
from marine_peptides.download.tier3.marref import download_ncbi_catalog


def download_mardb(
    metadata_tsv: str | Path,
    accession_col: str,
    out_dir: str | Path,
    interim_dir: str | Path,
    tier12_root: str | Path,
    missing_path: str | Path,
    logger: Logger = None,
) -> tuple[pd.DataFrame, dict[str, Path]]:
    """Download or symlink MarDB genomes."""
    return download_ncbi_catalog(
        metadata_tsv=metadata_tsv,
        accession_col=accession_col,
        catalog="mardb",
        out_dir=out_dir,
        interim_dir=interim_dir,
        tier12_root=tier12_root,
        logger=logger,
        missing_path=missing_path,
    )
