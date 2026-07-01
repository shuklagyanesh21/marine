#!/usr/bin/env python3
"""Fetch Tier 3 catalog metadata and prepare accession lists."""

from __future__ import annotations

from pathlib import Path

import requests

from marine_peptides.config import load_config, resolve_path
from marine_peptides.download.tier3.common import download_file
from marine_peptides.download.tier3.gorg import fetch_gorg_metadata
from marine_peptides.download.tier3.marref import (
    load_catalog_table,
    write_accession_list,
    write_metadata_snapshot,
)
from marine_peptides.download.tier3.oceandna import figshare_url


def _fetch_text(url: str, dest: Path) -> None:
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(resp.text)


def _prepare_ncbi_catalog(name: str, cfg: dict) -> None:
    df = load_catalog_table(cfg["metadata_tsv"], accession_col=cfg["accession_col"], catalog=name)
    write_accession_list(df, resolve_path(cfg["accession_list"]))
    write_metadata_snapshot(df, resolve_path(cfg["metadata_snapshot"]))
    print(f"{name}: {len(df):,} accession(s) -> {resolve_path(cfg['accession_list'])}")


def main() -> None:
    cfg = load_config()
    t3 = cfg["tier3"]

    _prepare_ncbi_catalog("marref", t3["marref"])
    _prepare_ncbi_catalog("mardb", t3["mardb"])

    gorg_cfg = t3["gorg"]
    gorg_df = fetch_gorg_metadata(
        project=gorg_cfg["ena_project"],
        metadata_tsv=resolve_path(gorg_cfg["metadata_tsv"]),
        base_url=gorg_cfg["filereport_url"],
        logger=print,
    )
    print(f"gorg: {len(gorg_df):,} WGS-set rows -> {resolve_path(gorg_cfg['metadata_tsv'])}")

    oceandna_cfg = t3["oceandna"]
    supp = oceandna_cfg["files"]["supp"]
    supp_path = resolve_path(oceandna_cfg["metadata_dir"]) / supp["name"]
    if not supp_path.exists():
        download_file(figshare_url(supp["id"]), supp_path, logger=print)
    print(f"oceandna: metadata archive ready -> {supp_path}")

    gomc_cfg = t3["gomc"]
    md5_path = resolve_path(gomc_cfg["metadata_dir"]) / "md5.txt"
    if not md5_path.exists():
        download_file(f"{gomc_cfg['cngb_base'].rstrip('/')}/md5.txt", md5_path, logger=print)
    listing_path = resolve_path(gomc_cfg["metadata_dir"]) / "ftp_listing.html"
    if not listing_path.exists():
        _fetch_text(gomc_cfg["cngb_base"], listing_path)
    print(f"gomc: md5 + listing ready -> {resolve_path(gomc_cfg['metadata_dir'])}")

    broken = resolve_path("data/metadata/tier3/MarDB/MarDB_1.6.tgz")
    if broken.exists() and broken.stat().st_size < 1024:
        broken.unlink()
        print(f"Deleted truncated file: {broken}")


if __name__ == "__main__":
    main()
