"""Tier 2: fetch NCBI BioSample environmental attributes via E-utilities.

Recovers marine MAGs/SAGs whose habitat is recorded in BioSample
(``env_broad_scale``/``env_medium``/...) but never propagated into GTDB metadata.

Design notes:
* Raw HTTP to E-utilities (the EDirect ``efetch`` wrapper is not required).
* Results are cached to a TSV keyed by BioSample accession, so the job is
  fully resumable and re-runs are cheap.
* Rate limited to be a good NCBI citizen (3 req/s anonymous, 10 with an
  ``NCBI_API_KEY`` environment variable).
"""

from __future__ import annotations

import json
import os
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Callable, Iterable

import pandas as pd
import requests

# Harmonized BioSample attribute names we extract (classification + provenance).
EXTRACT_FIELDS = [
    "env_broad_scale",
    "env_local_scale",
    "env_medium",
    "isolation_source",
    "geo_loc_name",
    "lat_lon",
    "depth",
    "host",
    "collection_date",
]

_CACHE_COLUMNS = ["biosample", *EXTRACT_FIELDS, "fetch_status"]


def _log(msg: str, logger: Callable[[str], None] | None) -> None:
    if logger is not None:
        logger(msg)


def parse_biosample_xml(xml_text: str) -> dict[str, dict[str, str]]:
    """Parse an E-utilities BioSample XML payload.

    Returns ``{id: {harmonized_name: value}}`` indexed by the primary accession
    *and* every ``<Id>`` value (e.g. SAMN/SAMEA/SAMD + ERS/DRS aliases), so a
    requested accession resolves regardless of which alias we queried by. Both
    harmonized and raw attribute names are stored.
    """
    out: dict[str, dict[str, str]] = {}
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return out

    for bs in root.iter("BioSample"):
        attrs: dict[str, str] = {}
        for attr in bs.iter("Attribute"):
            value = (attr.text or "").strip()
            if not value:
                continue
            for key in (attr.get("harmonized_name"), attr.get("attribute_name")):
                if key:
                    attrs.setdefault(key, value)

        keys = {bs.get("accession")}
        keys.update(i.text for i in bs.iter("Id"))
        for key in keys:
            if key:
                out[key] = attrs
    return out


def _row_from_attrs(biosample: str, attrs: dict[str, str], status: str) -> dict[str, str]:
    row = {"biosample": biosample, "fetch_status": status}
    for f in EXTRACT_FIELDS:
        row[f] = attrs.get(f, "")
    return row


def _load_cache(cache_path: Path) -> pd.DataFrame:
    if cache_path.exists():
        return pd.read_csv(cache_path, sep="\t", dtype=str, na_filter=False)
    return pd.DataFrame(columns=_CACHE_COLUMNS)


def _chunks(seq: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _post(session: requests.Session, url: str, params: dict, max_retries: int,
          logger: Callable[[str], None] | None, label: str) -> str:
    """POST to an E-utilities endpoint with retry/backoff; return body text."""
    for attempt in range(1, max_retries + 1):
        try:
            resp = session.post(url, data=params, timeout=120)
            if resp.status_code == 200 and resp.text:
                return resp.text
            raise requests.HTTPError(f"status {resp.status_code}")
        except requests.RequestException as exc:
            wait = min(2 ** attempt, 30)
            _log(f"  {label} attempt {attempt} failed: {exc}; retry in {wait}s", logger)
            time.sleep(wait)
    return ""


def _esearch_uids(session, esearch_url, accessions, api_key, max_retries, logger) -> list[str]:
    """Resolve a batch of BioSample accessions to UIDs (handles SAMN/SAMEA/SAMD)."""
    params = {
        "db": "biosample",
        "term": " OR ".join(accessions),
        "retmax": str(len(accessions) + 10),
        "retmode": "json",
    }
    if api_key:
        params["api_key"] = api_key
    text = _post(session, esearch_url, params, max_retries, logger, "esearch")
    if not text:
        return []
    try:
        return json.loads(text)["esearchresult"].get("idlist", [])
    except (json.JSONDecodeError, KeyError):
        return []


def fetch_biosample_table(
    biosample_ids: Iterable[str],
    cfg_tier2: dict,
    cache_path: str | Path,
    logger: Callable[[str], None] | None = None,
    max_retries: int = 4,
) -> pd.DataFrame:
    """Fetch (with caching) BioSample attributes for the given accessions.

    Returns a DataFrame with columns ``biosample`` + :data:`EXTRACT_FIELDS`.
    """
    cache_path = Path(cache_path)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    wanted = sorted({b.strip() for b in biosample_ids if b and b.strip()})
    cache = _load_cache(cache_path)
    done = set(cache["biosample"]) if not cache.empty else set()
    todo = [b for b in wanted if b not in done]

    _log(f"BioSample cache: {len(done)} cached, {len(todo)} to fetch", logger)

    api_key = os.environ.get("NCBI_API_KEY", "").strip()
    rps = float(cfg_tier2.get("requests_per_second", 3))
    if api_key:
        rps = max(rps, 10.0)
    delay = 1.0 / rps if rps > 0 else 0.0
    batch_size = int(cfg_tier2.get("batch_size", 200))
    base = cfg_tier2["eutils_base"].rstrip("/")
    esearch_url = f"{base}/esearch.fcgi"
    efetch_url = f"{base}/efetch.fcgi"

    session = requests.Session()
    new_rows: list[dict[str, str]] = []
    n_batches = (len(todo) + batch_size - 1) // batch_size if todo else 0

    for bi, batch in enumerate(_chunks(todo, batch_size), start=1):
        # Step 1: accessions -> UIDs (needed for non-NCBI SAMEA/SAMD accessions).
        uids = _esearch_uids(session, esearch_url, batch, api_key, max_retries, logger)
        time.sleep(delay)

        # Step 2: efetch full records by UID; back-map via embedded accessions.
        xml_text = ""
        if uids:
            params = {"db": "biosample", "id": ",".join(uids), "rettype": "full", "retmode": "xml"}
            if api_key:
                params["api_key"] = api_key
            xml_text = _post(session, efetch_url, params, max_retries, logger, f"efetch {bi}/{n_batches}")

        parsed = parse_biosample_xml(xml_text) if xml_text else {}
        n_ok = 0
        for b in batch:
            if b in parsed:
                new_rows.append(_row_from_attrs(b, parsed[b], "ok"))
                n_ok += 1
            else:
                new_rows.append(_row_from_attrs(b, {}, "not_found"))

        # Flush incrementally so the run stays resumable on interruption.
        pd.DataFrame(new_rows, columns=_CACHE_COLUMNS).to_csv(
            cache_path,
            sep="\t",
            index=False,
            mode="a",
            header=not cache_path.exists(),
        )
        done.update(batch)
        new_rows.clear()
        _log(f"  batch {bi}/{n_batches}: {n_ok}/{len(batch)} resolved", logger)
        time.sleep(delay)

    full = _load_cache(cache_path)
    return full[full["biosample"].isin(wanted)].reset_index(drop=True)
