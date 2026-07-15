"""Manifest-driven SmORFinder helpers and run bookkeeping."""

from __future__ import annotations

import csv
import gzip
import hashlib
import json
import os
import re
import shutil
import subprocess
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from marine_peptides.config import load_config, project_root, resolve_path
from marine_peptides.standardize.inventory import FASTA_SUFFIXES

INPUT_COLUMNS = ["sample_id", "mode", "fasta_path"]
STATUS_COLUMNS = [
    "sample_id",
    "mode",
    "fasta_path",
    "resolved_fasta_path",
    "fasta_size_bytes",
    "fasta_mtime_ns",
    "input_signature",
    "parameter_signature",
    "action",
    "reason",
    "output_dir",
    "success_marker",
]
RUN_MANIFEST_COLUMNS = [
    "sample_id",
    "mode",
    "fasta_path",
    "status",
    "reason",
    "output_dir",
    "success_marker",
    "prediction_count",
    "faa_path",
    "ffn_path",
    "gff_path",
    "tsv_path",
    "input_signature",
    "parameter_signature",
    "asset_manifest_sha256",
]
SAFE_SAMPLE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
VALID_MODES = {"single", "meta"}


@dataclass(frozen=True)
class SmorfinderInput:
    """One user-selected assembly to process with SmORFinder."""

    sample_id: str
    mode: str
    fasta_path: str

    @property
    def fasta_abs_path(self) -> Path:
        return resolve_path(self.fasta_path)

    @property
    def output_dir(self) -> Path:
        cfg = load_config()
        return expected_output_dir(cfg, self.sample_id, self.mode)

    def as_dict(self) -> dict[str, str]:
        return asdict(self)


def load_input_manifest(path: str | Path | None = None) -> list[SmorfinderInput]:
    """Read and validate the committed SmORFinder input TSV."""
    cfg = load_config()
    manifest_path = resolve_path(path or cfg["smorfinder"]["input_tsv"])
    if not manifest_path.exists():
        raise FileNotFoundError(f"Missing SmORFinder input manifest: {manifest_path}")

    with manifest_path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        if reader.fieldnames is None:
            raise ValueError(f"SmORFinder input manifest is empty: {manifest_path}")
        missing = [col for col in INPUT_COLUMNS if col not in reader.fieldnames]
        if missing:
            raise ValueError(f"Missing required columns in {manifest_path}: {', '.join(missing)}")

        seen_ids: set[str] = set()
        rows: list[SmorfinderInput] = []
        for line_num, raw in enumerate(reader, start=2):
            sample_id = (raw.get("sample_id") or "").strip()
            mode = (raw.get("mode") or "").strip().lower()
            fasta_path = (raw.get("fasta_path") or "").strip()
            if not sample_id:
                raise ValueError(f"{manifest_path}:{line_num} has blank sample_id")
            if not SAFE_SAMPLE_RE.match(sample_id):
                raise ValueError(
                    f"{manifest_path}:{line_num} sample_id={sample_id!r} must match {SAFE_SAMPLE_RE.pattern}"
                )
            if sample_id in seen_ids:
                raise ValueError(f"{manifest_path}:{line_num} reuses sample_id={sample_id!r}")
            seen_ids.add(sample_id)
            if mode not in VALID_MODES:
                raise ValueError(
                    f"{manifest_path}:{line_num} mode={mode!r} must be one of {sorted(VALID_MODES)}"
                )
            if not fasta_path:
                raise ValueError(f"{manifest_path}:{line_num} has blank fasta_path")

            fasta_rel = Path(fasta_path)
            if fasta_rel.is_absolute():
                raise ValueError(f"{manifest_path}:{line_num} fasta_path must be project-relative: {fasta_path}")
            if ".." in fasta_rel.parts:
                raise ValueError(f"{manifest_path}:{line_num} fasta_path must stay inside the repo: {fasta_path}")
            if not str(fasta_rel).endswith(FASTA_SUFFIXES):
                raise ValueError(
                    f"{manifest_path}:{line_num} fasta_path={fasta_path!r} must end with one of {FASTA_SUFFIXES}"
                )

            fasta_abs = resolve_path(fasta_rel)
            if not fasta_abs.exists() or not fasta_abs.is_file():
                raise FileNotFoundError(f"{manifest_path}:{line_num} FASTA not found: {fasta_abs}")
            rows.append(SmorfinderInput(sample_id=sample_id, mode=mode, fasta_path=str(fasta_rel)))
    return rows


def expected_output_dir(cfg: dict[str, Any], sample_id: str, mode: str) -> Path:
    """Return the published output directory for one sample."""
    return resolve_path(cfg["smorfinder"]["out_dir"]) / mode / sample_id


def expected_output_files(output_dir: str | Path, sample_id: str) -> dict[str, Path]:
    """Return the expected final SmORFinder result files."""
    out = Path(output_dir)
    prefix = out / sample_id
    return {
        "faa": prefix.with_suffix(".faa"),
        "ffn": prefix.with_suffix(".ffn"),
        "gff": prefix.with_suffix(".gff"),
        "tsv": prefix.with_suffix(".tsv"),
        "success": out / "_SUCCESS.json",
    }


def input_signature(fasta_path: str | Path) -> dict[str, Any]:
    """Build a cheap input fingerprint from the resolved path and stat metadata."""
    fasta_abs = resolve_path(fasta_path)
    stat = fasta_abs.stat()
    payload = {
        "resolved_fasta_path": str(fasta_abs.resolve(strict=True)),
        "size_bytes": stat.st_size,
        "mtime_ns": stat.st_mtime_ns,
    }
    payload["signature"] = _hash_json(payload)
    return payload


def asset_manifest_sha256(cfg: dict[str, Any]) -> str:
    """Hash the committed asset manifest file itself."""
    path = resolve_path(cfg["smorfinder"]["assets_manifest"])
    return sha256_file(path)


def parameter_signature(cfg: dict[str, Any], mode: str) -> str:
    """Hash the parameters that materially change SmORFinder output semantics."""
    sm_cfg = cfg["smorfinder"]
    payload = {
        "mode": mode,
        "cutoffs": sm_cfg["cutoffs"],
        "cleanup_tmp": bool(sm_cfg["cleanup_tmp"]),
        "asset_manifest_sha256": asset_manifest_sha256(cfg),
    }
    return _hash_json(payload)


def select_pending_inputs(rows: list[SmorfinderInput], cfg: dict[str, Any]) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    """Classify inputs into pending and already-complete rows."""
    pending: list[dict[str, str]] = []
    statuses: list[dict[str, str]] = []
    for row in rows:
        sig = input_signature(row.fasta_path)
        output_dir = expected_output_dir(cfg, row.sample_id, row.mode)
        success_path = expected_output_files(output_dir, row.sample_id)["success"]
        action = "run"
        reason = "missing_success_marker"
        if success_path.exists():
            if success_marker_is_valid(cfg, row, output_dir=output_dir):
                action = "skip_existing"
                reason = "matching_success_marker"
            else:
                action = "run"
                reason = "stale_or_invalid_success_marker"

        status_row = {
            "sample_id": row.sample_id,
            "mode": row.mode,
            "fasta_path": row.fasta_path,
            "resolved_fasta_path": sig["resolved_fasta_path"],
            "fasta_size_bytes": str(sig["size_bytes"]),
            "fasta_mtime_ns": str(sig["mtime_ns"]),
            "input_signature": sig["signature"],
            "parameter_signature": parameter_signature(cfg, row.mode),
            "action": action,
            "reason": reason,
            "output_dir": str(project_relative(output_dir)),
            "success_marker": str(project_relative(success_path)),
        }
        statuses.append(status_row)
        if action == "run":
            pending.append(row.as_dict())
    return pending, statuses


def write_prepared_manifests(cfg: dict[str, Any], rows: list[SmorfinderInput]) -> dict[str, Any]:
    """Validate input selections and write normalized, status, and pending TSVs."""
    sm_cfg = cfg["smorfinder"]
    normalized_path = resolve_path(sm_cfg["normalized_tsv"])
    pending_path = resolve_path(sm_cfg["pending_tsv"])
    status_path = resolve_path(sm_cfg["input_status_tsv"])
    pending_rows, statuses = select_pending_inputs(rows, cfg)
    _write_tsv_atomic(normalized_path, [row.as_dict() for row in rows], INPUT_COLUMNS)
    _write_tsv_atomic(status_path, statuses, STATUS_COLUMNS)
    _write_tsv_atomic(pending_path, pending_rows, INPUT_COLUMNS)
    return {
        "normalized_tsv": normalized_path,
        "status_tsv": status_path,
        "pending_tsv": pending_path,
        "total_rows": len(rows),
        "pending_rows": len(pending_rows),
        "skipped_rows": len(rows) - len(pending_rows),
    }


def _resolve_existing_relpath(*candidates: str | None) -> str | None:
    """Return the first project-relative candidate that exists, trying a .gz twin.

    The genome index stores raw paths without the ``.gz`` suffix even though the
    files on disk are gzipped, so each candidate is checked both as-is and with
    the compression suffix toggled.
    """
    for candidate in candidates:
        if not candidate:
            continue
        base = str(candidate)
        variants = [base[:-3]] if base.endswith(".gz") else [base + ".gz"]
        for variant in (base, *variants):
            if not variant.endswith(FASTA_SUFFIXES):
                continue
            if resolve_path(variant).is_file():
                return variant
    return None


def build_inputs_from_genome_index(
    cfg: dict[str, Any],
    output_path: str | Path | None = None,
    limit: int | None = None,
    representatives_only: bool = False,
) -> dict[str, Any]:
    """Expand the standardized genome index into a SmORFinder input TSV.

    Every canonical genome with ``status == ok`` and a resolvable FASTA becomes a
    ``single``-mode row (``sample_id`` = canonical id). Rows whose FASTA cannot be
    located on disk are skipped and reported so the run never fails validation.
    """
    sm_cfg = cfg["smorfinder"]
    index_path = resolve_path(sm_cfg["input_index"])
    out_path = resolve_path(output_path or sm_cfg["input_tsv"])
    if not index_path.exists():
        raise FileNotFoundError(f"Missing genome index: {index_path}")

    rows: list[dict[str, str]] = []
    skipped: list[str] = []
    truthy = {"1", "true", "t", "yes", "y"}
    with index_path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        for record in reader:
            if (record.get("status") or "").strip().lower() != "ok":
                continue
            if representatives_only and str(record.get("is_gtdb_representative", "")).strip().lower() not in truthy:
                continue
            canonical_id = (record.get("canonical_id") or "").strip()
            if not canonical_id:
                continue
            fasta_rel = _resolve_existing_relpath(
                record.get("fasta_path"), record.get("canonical_raw_path")
            )
            if fasta_rel is None:
                skipped.append(canonical_id)
                continue
            rows.append({"sample_id": canonical_id, "mode": "single", "fasta_path": fasta_rel})
            if limit is not None and len(rows) >= limit:
                break

    _write_tsv_atomic(out_path, rows, INPUT_COLUMNS)
    return {
        "output": out_path,
        "index": index_path,
        "written": len(rows),
        "skipped": len(skipped),
        "skipped_ids": skipped,
        "representatives_only": representatives_only,
    }


def promote_failures_to_meta(
    cfg: dict[str, Any], input_tsv: str | Path | None = None
) -> dict[str, Any]:
    """Flip ``single`` rows that lack a valid success marker to ``meta`` mode.

    Intended to run between the two phases of the hybrid strategy: after the
    single-mode pass, any genome that did not produce a valid
    ``single/<id>/_SUCCESS.json`` (e.g. because the bundled Prodigal segfaulted
    during single-mode training) is promoted to ``meta`` mode so a subsequent
    ``-resume`` re-runs only those genomes with the crash-free meta path.

    The rewrite is idempotent: genomes that already succeeded in single stay
    single, and rows already in ``meta`` are left untouched.
    """
    manifest_path = resolve_path(input_tsv or cfg["smorfinder"]["input_tsv"])
    rows = load_input_manifest(manifest_path)
    new_rows: list[dict[str, str]] = []
    promoted: list[str] = []
    kept_single = 0
    already_meta = 0
    for row in rows:
        if row.mode != "single":
            already_meta += 1
            new_rows.append(row.as_dict())
            continue
        if success_marker_is_valid(cfg, row):
            kept_single += 1
            new_rows.append(row.as_dict())
        else:
            promoted.append(row.sample_id)
            new_rows.append(
                {"sample_id": row.sample_id, "mode": "meta", "fasta_path": row.fasta_path}
            )
    _write_tsv_atomic(manifest_path, new_rows, INPUT_COLUMNS)
    return {
        "output": manifest_path,
        "total": len(rows),
        "kept_single": kept_single,
        "promoted_to_meta": len(promoted),
        "already_meta": already_meta,
        "promoted_ids": promoted,
    }


def read_asset_manifest(cfg: dict[str, Any]) -> list[tuple[str, str]]:
    """Read the committed SmORFinder asset checksum manifest."""
    path = resolve_path(cfg["smorfinder"]["assets_manifest"])
    rows: list[tuple[str, str]] = []
    with path.open() as handle:
        for line_num, line in enumerate(handle, start=1):
            raw = line.strip()
            if not raw:
                continue
            try:
                digest, rel_path = raw.split(None, 1)
            except ValueError as exc:
                raise ValueError(f"{path}:{line_num} is not a valid sha256 manifest row") from exc
            rows.append((digest, rel_path))
    if not rows:
        raise ValueError(f"SmORFinder asset manifest has no entries: {path}")
    return rows


def verify_smorfinder_assets(cfg: dict[str, Any]) -> list[dict[str, str]]:
    """Verify that all committed SmORFinder assets exist under the configured env prefix."""
    prefix = resolve_path(cfg["smorfinder"]["conda_prefix"])
    package_root = _smorfinder_package_root(prefix)
    verified: list[dict[str, str]] = []
    for expected_hash, rel_path in read_asset_manifest(cfg):
        asset_path = package_root / rel_path
        if not asset_path.exists():
            raise FileNotFoundError(f"Missing SmORFinder asset: {asset_path}")
        observed = sha256_file(asset_path)
        if observed != expected_hash:
            raise ValueError(
                f"SmORFinder asset hash mismatch for {asset_path}: expected {expected_hash}, observed {observed}"
            )
        verified.append({"path": rel_path, "sha256": observed})
    return verified


def ensure_smorfinder_environment(cfg: dict[str, Any], logger: Any = print) -> dict[str, Any]:
    """Create/update the dedicated env, trigger model download, and verify asset hashes."""
    sm_cfg = cfg["smorfinder"]
    env_yaml = resolve_path(sm_cfg["env_yaml"])
    prefix = resolve_path(sm_cfg["conda_prefix"])
    prefix.parent.mkdir(parents=True, exist_ok=True)

    create_args = ["conda", "env", "create", "--prefix", str(prefix), "-f", str(env_yaml)]
    update_args = ["conda", "env", "update", "--prefix", str(prefix), "-f", str(env_yaml)]
    command = create_args if not prefix.exists() else update_args
    logger(f"SmORFinder env: {'creating' if command is create_args else 'updating'} {prefix}")
    subprocess.run(command, check=True)

    smorf_bin = prefix / "bin" / "smorf"
    if not smorf_bin.exists():
        raise FileNotFoundError(f"Missing SmORFinder launcher after env setup: {smorf_bin}")

    env = os.environ.copy()
    env["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
    subprocess.run([str(smorf_bin), "--help"], check=True, env=env)
    verified = verify_smorfinder_assets(cfg)

    stamp_path = resolve_path(sm_cfg["preflight_stamp"])
    payload = {
        "timestamp_epoch_s": int(time.time()),
        "env_prefix": str(prefix),
        "smorf_bin": str(smorf_bin),
        "asset_manifest_sha256": asset_manifest_sha256(cfg),
        "assets": verified,
    }
    _write_json_atomic(stamp_path, payload)
    return payload


def build_smorf_command(
    cfg: dict[str, Any],
    sample_id: str,
    mode: str,
    fasta_path: str | Path,
    output_dir: str | Path,
    cpus: int,
) -> list[str]:
    """Build the SmORFinder CLI invocation for one assembly."""
    if mode not in VALID_MODES:
        raise ValueError(f"Unsupported SmORFinder mode: {mode}")
    sm_cfg = cfg["smorfinder"]
    smorf_bin = resolve_path(sm_cfg["conda_prefix"]).resolve(strict=False) / "bin" / "smorf"
    cutoffs = sm_cfg["cutoffs"]
    command = [
        str(smorf_bin),
        mode,
        str(fasta_path),
        "--outdir",
        str(output_dir),
        "--force",
        "--dsn1-indiv-cutoff",
        str(cutoffs["dsn1_indiv"]),
        "--dsn2-indiv-cutoff",
        str(cutoffs["dsn2_indiv"]),
        "--phmm-indiv-cutoff",
        str(cutoffs["phmm_indiv"]),
        "--dsn1-overlap-cutoff",
        str(cutoffs["dsn1_overlap"]),
        "--dsn2-overlap-cutoff",
        str(cutoffs["dsn2_overlap"]),
        "--phmm-overlap-cutoff",
        str(cutoffs["phmm_overlap"]),
    ]
    if mode == "meta":
        command.extend(["--threads", str(cpus)])
    return command


def stage_fasta_for_smorf(src: str | Path, dest: str | Path) -> Path:
    """Copy or gunzip a FASTA into an uncompressed task-local file."""
    src_path = Path(src)
    dest_path = Path(dest)
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    if src_path.name.endswith(".gz"):
        with gzip.open(src_path, "rt") as reader, dest_path.open("w") as writer:
            shutil.copyfileobj(reader, writer)
    else:
        shutil.copyfile(src_path, dest_path)
    return dest_path


def run_smorfinder_task(
    cfg: dict[str, Any],
    sample_id: str,
    mode: str,
    fasta_path: str | Path,
    work_dir: str | Path,
    cpus: int,
) -> Path:
    """Run SmORFinder in one task work directory and finalize outputs."""
    work_path = Path(work_dir)
    staged_fasta = stage_fasta_for_smorf(resolve_path(fasta_path), work_path / "input.fna")
    output_dir = work_path / sample_id

    env = os.environ.copy()
    env["PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION"] = "python"
    env["TF_CPP_MIN_LOG_LEVEL"] = "3"
    env["OMP_NUM_THREADS"] = "1"
    env["OPENBLAS_NUM_THREADS"] = "1"
    env["MKL_NUM_THREADS"] = "1"
    env["NUMEXPR_NUM_THREADS"] = "1"

    command = build_smorf_command(cfg, sample_id, mode, staged_fasta, output_dir, cpus=cpus)
    subprocess.run(command, check=True, cwd=work_path, env=env)

    files = expected_output_files(output_dir, sample_id)
    missing = [str(path) for key, path in files.items() if key != "success" and not path.exists()]
    if missing:
        raise FileNotFoundError(f"Missing SmORFinder outputs for {sample_id}: {missing}")

    if bool(cfg["smorfinder"]["cleanup_tmp"]):
        tmp_dir = output_dir / "tmp"
        if tmp_dir.exists():
            shutil.rmtree(tmp_dir)

    write_success_marker(cfg, sample_id, mode, fasta_path, output_dir)
    return output_dir


def success_marker_is_valid(
    cfg: dict[str, Any],
    row: SmorfinderInput,
    output_dir: str | Path | None = None,
) -> bool:
    """Return True if the success marker matches current inputs and files exist."""
    out_dir = Path(output_dir) if output_dir is not None else expected_output_dir(cfg, row.sample_id, row.mode)
    files = expected_output_files(out_dir, row.sample_id)
    success_path = files["success"]
    if not success_path.exists():
        return False
    try:
        payload = json.loads(success_path.read_text())
    except json.JSONDecodeError:
        return False
    required = {"sample_id", "mode", "input_signature", "parameter_signature"}
    if not required.issubset(payload):
        return False
    current_sig = input_signature(row.fasta_path)["signature"]
    current_params = parameter_signature(cfg, row.mode)
    if payload.get("sample_id") != row.sample_id or payload.get("mode") != row.mode:
        return False
    if payload.get("input_signature") != current_sig:
        return False
    if payload.get("parameter_signature") != current_params:
        return False
    return all(path.exists() for key, path in files.items() if key != "success")


def write_success_marker(
    cfg: dict[str, Any],
    sample_id: str,
    mode: str,
    fasta_path: str | Path,
    output_dir: str | Path,
) -> Path:
    """Write the final success marker after output validation passes."""
    out_dir = Path(output_dir)
    files = expected_output_files(out_dir, sample_id)
    missing = [str(path) for key, path in files.items() if key != "success" and not path.exists()]
    if missing:
        raise FileNotFoundError(f"Cannot write success marker; missing outputs: {missing}")
    payload = {
        "sample_id": sample_id,
        "mode": mode,
        "input_signature": input_signature(fasta_path)["signature"],
        "parameter_signature": parameter_signature(cfg, mode),
        "prediction_count": count_predictions(files["tsv"]),
        "timestamp_epoch_s": int(time.time()),
    }
    _write_json_atomic(files["success"], payload)
    return files["success"]


def summarize_run_manifest(cfg: dict[str, Any]) -> Path:
    """Build the processed TSV that summarizes current SmORFinder run outcomes."""
    rows = load_input_manifest(cfg["smorfinder"]["input_tsv"])
    status_by_id = _read_status_rows(cfg)
    out_rows: list[dict[str, str]] = []
    for row in rows:
        output_dir = expected_output_dir(cfg, row.sample_id, row.mode)
        files = expected_output_files(output_dir, row.sample_id)
        status_row = status_by_id.get(row.sample_id, {})
        marker_valid = success_marker_is_valid(cfg, row, output_dir=output_dir)
        if marker_valid:
            status = "completed" if status_row.get("action") != "skip_existing" else "skipped"
            reason = status_row.get("reason", "matching_success_marker")
        elif output_dir.exists():
            status = "failed"
            reason = "missing_or_invalid_success_marker"
        else:
            status = "pending"
            reason = status_row.get("reason", "not_started")

        prediction_count = ""
        if files["tsv"].exists():
            prediction_count = str(count_predictions(files["tsv"]))

        out_rows.append(
            {
                "sample_id": row.sample_id,
                "mode": row.mode,
                "fasta_path": row.fasta_path,
                "status": status,
                "reason": reason,
                "output_dir": str(project_relative(output_dir)),
                "success_marker": str(project_relative(files["success"])),
                "prediction_count": prediction_count,
                "faa_path": str(project_relative(files["faa"])),
                "ffn_path": str(project_relative(files["ffn"])),
                "gff_path": str(project_relative(files["gff"])),
                "tsv_path": str(project_relative(files["tsv"])),
                "input_signature": input_signature(row.fasta_path)["signature"],
                "parameter_signature": parameter_signature(cfg, row.mode),
                "asset_manifest_sha256": asset_manifest_sha256(cfg),
            }
        )
    manifest_path = resolve_path(cfg["smorfinder"]["processed_manifest"])
    _write_tsv_atomic(manifest_path, out_rows, RUN_MANIFEST_COLUMNS)
    return manifest_path


def count_predictions(tsv_path: str | Path) -> int:
    """Count the number of predicted smORFs in a result TSV."""
    path = Path(tsv_path)
    if not path.exists():
        raise FileNotFoundError(path)
    with path.open() as handle:
        next(handle, None)
        return sum(1 for _ in handle)


def project_relative(path: str | Path) -> Path:
    """Return a project-relative version of *path* where possible."""
    p = Path(path)
    try:
        return p.resolve(strict=False).relative_to(project_root())
    except ValueError:
        return p


def sha256_file(path: str | Path) -> str:
    """Hash a file without loading it fully into memory."""
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def _read_status_rows(cfg: dict[str, Any]) -> dict[str, dict[str, str]]:
    path = resolve_path(cfg["smorfinder"]["input_status_tsv"])
    if not path.exists():
        return {}
    with path.open(newline="") as handle:
        reader = csv.DictReader(handle, delimiter="\t")
        return {row["sample_id"]: dict(row) for row in reader}


def _write_tsv_atomic(path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, delimiter="\t", fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})
        handle.flush()
        os.fsync(handle.fileno())
    temp_path.replace(path)
    return path


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = path.with_suffix(path.suffix + ".tmp")
    with temp_path.open("w") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    temp_path.replace(path)
    return path


def _hash_json(payload: dict[str, Any]) -> str:
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode("utf-8")).hexdigest()


def _smorfinder_package_root(prefix: Path) -> Path:
    candidates = sorted(prefix.glob("lib/python*/site-packages/smorfinder"))
    if not candidates:
        raise FileNotFoundError(f"Could not locate installed smorfinder package under {prefix}")
    return candidates[0]
