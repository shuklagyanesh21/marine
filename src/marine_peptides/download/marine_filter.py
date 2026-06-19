"""Marine classification logic (keyword matching) shared by Tier 1 and Tier 2.

Pure, testable functions: given text fields and configured keyword lists, decide
whether a record looks marine, why (evidence string), and whether the only marine
signal is host association.
"""

from __future__ import annotations

import re
from typing import Iterable

import pandas as pd

_MISSING = {"", "none", "na", "n/a", "not applicable", "missing", "not collected", "unknown"}


def compile_keywords(keywords: Iterable[str]) -> re.Pattern:
    """Compile a case-insensitive, whole-word alternation regex from keywords."""
    parts = sorted({k.strip() for k in keywords if k and k.strip()}, key=len, reverse=True)
    if not parts:
        return re.compile(r"(?!x)x")  # matches nothing
    alt = "|".join(re.escape(p) for p in parts)
    return re.compile(rf"(?<![A-Za-z0-9]){alt}(?![A-Za-z0-9])", re.IGNORECASE)


def first_match(text: str, pattern: re.Pattern) -> str | None:
    """Return the first keyword matched in ``text`` (lowercased), else None."""
    if not isinstance(text, str) or not text or text.strip().lower() in _MISSING:
        return None
    m = pattern.search(text)
    return m.group(0).lower() if m else None


def classify_record(
    fields: dict[str, str],
    include: re.Pattern,
    exclude: re.Pattern,
    host: re.Pattern,
) -> tuple[bool, str, bool]:
    """Classify a single record from its named text fields.

    Returns ``(is_marine, evidence, is_host_associated)`` where evidence is a
    ``;``-joined list of ``field:keyword`` hits. A record is marine when at least
    one include term matches and no exclude term matches in any field.
    """
    excluded = any(first_match(v, exclude) for v in fields.values())
    if excluded:
        return False, "", False

    evidence: list[str] = []
    host_hit = False
    for name, value in fields.items():
        kw = first_match(value, include)
        if kw:
            evidence.append(f"{name}:{kw}")
        if first_match(value, host):
            host_hit = True

    is_marine = len(evidence) > 0
    return is_marine, ";".join(evidence), (is_marine and host_hit)


def classify_dataframe(
    df: pd.DataFrame,
    fields: list[str],
    include_keywords: Iterable[str],
    exclude_keywords: Iterable[str],
    host_keywords: Iterable[str],
    prefix: str = "marine",
) -> pd.DataFrame:
    """Add ``{prefix}``, ``{prefix}_evidence`` and ``is_host_associated`` columns.

    Only ``fields`` present in ``df`` are scanned.
    """
    include = compile_keywords(include_keywords)
    exclude = compile_keywords(exclude_keywords)
    host = compile_keywords(host_keywords)

    present = [f for f in fields if f in df.columns]
    results = df[present].apply(
        lambda row: classify_record(row.to_dict(), include, exclude, host),
        axis=1,
        result_type="expand",
    )
    out = df.copy()
    out[prefix] = results[0].astype(bool)
    out[f"{prefix}_evidence"] = results[1]
    out["is_host_associated"] = results[2].astype(bool)
    return out


# --------------------------------------------------------------------------- #
# lat/lon + depth parsing (best-effort; for the manifest provenance columns)
# --------------------------------------------------------------------------- #

_LATLON_RE = re.compile(
    r"(?P<lat>\d+(?:\.\d+)?)\s*(?P<latd>[NS])[ ,]+(?P<lon>\d+(?:\.\d+)?)\s*(?P<lond>[EW])",
    re.IGNORECASE,
)


def parse_lat_lon(value: str) -> tuple[float | None, float | None]:
    """Parse NCBI ``lat_lon`` (e.g. ``31.40 N 64.10 W``) into signed decimals."""
    if not isinstance(value, str) or not value or value.strip().lower() in _MISSING:
        return None, None
    m = _LATLON_RE.search(value)
    if not m:
        return None, None
    lat = float(m.group("lat")) * (-1 if m.group("latd").upper() == "S" else 1)
    lon = float(m.group("lon")) * (-1 if m.group("lond").upper() == "W" else 1)
    return lat, lon


_DEPTH_RE = re.compile(r"(?P<val>\d+(?:\.\d+)?)\s*(?P<unit>m|meter|metre|km|cm)?", re.IGNORECASE)


def parse_depth_m(value: str) -> float | None:
    """Parse a BioSample ``depth`` string into meters (best effort)."""
    if not isinstance(value, str) or not value or value.strip().lower() in _MISSING:
        return None
    m = _DEPTH_RE.search(value)
    if not m:
        return None
    val = float(m.group("val"))
    unit = (m.group("unit") or "m").lower()
    if unit == "km":
        return val * 1000.0
    if unit == "cm":
        return val / 100.0
    return val
