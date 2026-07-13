from __future__ import annotations

from pathlib import Path

import pytest

from marine_peptides.orf_prediction import smorfinder as sm


def _patch_repo(monkeypatch: pytest.MonkeyPatch, root: Path, cfg: dict) -> None:
    monkeypatch.setattr(sm, "project_root", lambda: root)
    monkeypatch.setattr(sm, "resolve_path", lambda p: Path(p) if Path(p).is_absolute() else root / p)
    monkeypatch.setattr(sm, "load_config", lambda path=None: cfg)


def _write_text(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    return path


def _base_cfg(root: Path) -> dict:
    return {
        "smorfinder": {
            "assets_manifest": "env/smorfinder-assets.sha256",
            "conda_prefix": "../envs/smorfinder",
            "input_tsv": "config/smorfinder_inputs.tsv",
            "input_index": "data/processed/genome_index.tsv",
            "normalized_tsv": "data/interim/smorfinder/normalized_inputs.tsv",
            "input_status_tsv": "data/interim/smorfinder/input_status.tsv",
            "pending_tsv": "data/interim/smorfinder/pending_inputs.tsv",
            "preflight_stamp": "data/interim/smorfinder/preflight.ok.json",
            "out_dir": "data/interim/smorfinder",
            "processed_manifest": "data/processed/smorfinder_run_manifest.tsv",
            "cleanup_tmp": True,
            "cutoffs": {
                "dsn1_indiv": 0.9999,
                "dsn2_indiv": 0.9999,
                "phmm_indiv": 1.0e-6,
                "dsn1_overlap": 0.5,
                "dsn2_overlap": 0.5,
                "phmm_overlap": 1,
            },
        }
    }


def test_load_input_manifest_rejects_invalid_mode(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "repo"
    cfg = _base_cfg(root)
    _patch_repo(monkeypatch, root, cfg)
    _write_text(root / "data" / "example.fna", ">c1\nATGAAATAG\n")
    manifest = _write_text(
        root / "config" / "bad.tsv",
        "sample_id\tmode\tfasta_path\nsample1\twrong\tdata/example.fna\n",
    )

    with pytest.raises(ValueError, match="must be one of"):
        sm.load_input_manifest(manifest)


def test_build_smorf_command_handles_single_and_meta(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "repo"
    cfg = _base_cfg(root)
    _patch_repo(monkeypatch, root, cfg)

    single_cmd = sm.build_smorf_command(cfg, "iso1", "single", "input.fna", "iso1", cpus=1)
    meta_cmd = sm.build_smorf_command(cfg, "meta1", "meta", "input.fna", "meta1", cpus=8)

    assert single_cmd[:3] == [str(root.parent / "envs" / "smorfinder" / "bin" / "smorf"), "single", "input.fna"]
    assert "--threads" not in single_cmd
    assert meta_cmd[:3] == [str(root.parent / "envs" / "smorfinder" / "bin" / "smorf"), "meta", "input.fna"]
    assert meta_cmd[-2:] == ["--threads", "8"]


def test_stage_fasta_for_smorf_decompresses_gzip(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import gzip

    root = tmp_path / "repo"
    cfg = _base_cfg(root)
    _patch_repo(monkeypatch, root, cfg)
    src = root / "data" / "reads.fna.gz"
    src.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(src, "wt") as handle:
        handle.write(">c1\nATGC\n")

    dest = root / "work" / "input.fna"
    sm.stage_fasta_for_smorf(src, dest)

    assert dest.read_text() == ">c1\nATGC\n"


def test_success_marker_accepts_empty_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "repo"
    cfg = _base_cfg(root)
    _patch_repo(monkeypatch, root, cfg)
    _write_text(root / "env" / "smorfinder-assets.sha256", "abc  fake\n")
    fasta = _write_text(root / "data" / "example.fna", ">c1\nATGAAATAG\n")
    out_dir = root / "data" / "interim" / "smorfinder" / "single" / "iso1"
    out_dir.mkdir(parents=True, exist_ok=True)
    for suffix in (".faa", ".ffn", ".gff"):
        _write_text(out_dir / f"iso1{suffix}", "")
    _write_text(out_dir / "iso1.tsv", "seqid\tcontig\tstart\tend\torient\tsmorfam\thmm_smorfam_evalue\tdsn1_prob_smorf\tdsn2_prob_smorf\t5p_seq\torf\t3p_seq\n")

    row = sm.SmorfinderInput(sample_id="iso1", mode="single", fasta_path="data/example.fna")
    sm.write_success_marker(cfg, "iso1", "single", row.fasta_path, out_dir)

    assert sm.count_predictions(out_dir / "iso1.tsv") == 0
    assert sm.success_marker_is_valid(cfg, row, output_dir=out_dir)


def test_success_marker_invalidates_on_input_change(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "repo"
    cfg = _base_cfg(root)
    _patch_repo(monkeypatch, root, cfg)
    _write_text(root / "env" / "smorfinder-assets.sha256", "abc  fake\n")
    fasta = _write_text(root / "data" / "example.fna", ">c1\nATGAAATAG\n")
    out_dir = root / "data" / "interim" / "smorfinder" / "single" / "iso1"
    out_dir.mkdir(parents=True, exist_ok=True)
    for suffix in (".faa", ".ffn", ".gff"):
        _write_text(out_dir / f"iso1{suffix}", "")
    _write_text(out_dir / "iso1.tsv", "seqid\tcontig\tstart\tend\torient\tsmorfam\thmm_smorfam_evalue\tdsn1_prob_smorf\tdsn2_prob_smorf\t5p_seq\torf\t3p_seq\n")

    row = sm.SmorfinderInput(sample_id="iso1", mode="single", fasta_path="data/example.fna")
    sm.write_success_marker(cfg, "iso1", "single", row.fasta_path, out_dir)
    fasta.write_text(">c1\nATGAAAAAATAG\n")

    assert not sm.success_marker_is_valid(cfg, row, output_dir=out_dir)


def test_select_pending_inputs_skips_matching_success(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = tmp_path / "repo"
    cfg = _base_cfg(root)
    _patch_repo(monkeypatch, root, cfg)
    _write_text(root / "env" / "smorfinder-assets.sha256", "abc  fake\n")
    _write_text(root / "data" / "iso.fna", ">c1\nATGAAATAG\n")
    _write_text(root / "data" / "meta.fna", ">c2\nATGCCCCCTAG\n")

    out_dir = root / "data" / "interim" / "smorfinder" / "single" / "iso1"
    out_dir.mkdir(parents=True, exist_ok=True)
    for suffix in (".faa", ".ffn", ".gff"):
        _write_text(out_dir / f"iso1{suffix}", "")
    _write_text(out_dir / "iso1.tsv", "seqid\tcontig\tstart\tend\torient\tsmorfam\thmm_smorfam_evalue\tdsn1_prob_smorf\tdsn2_prob_smorf\t5p_seq\torf\t3p_seq\n")
    sm.write_success_marker(cfg, "iso1", "single", "data/iso.fna", out_dir)

    rows = [
        sm.SmorfinderInput(sample_id="iso1", mode="single", fasta_path="data/iso.fna"),
        sm.SmorfinderInput(sample_id="meta1", mode="meta", fasta_path="data/meta.fna"),
    ]
    pending, statuses = sm.select_pending_inputs(rows, cfg)

    assert [row["sample_id"] for row in pending] == ["meta1"]
    assert {row["sample_id"]: row["action"] for row in statuses} == {
        "iso1": "skip_existing",
        "meta1": "run",
    }


def test_build_inputs_from_genome_index_resolves_and_skips(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import gzip

    root = tmp_path / "repo"
    cfg = _base_cfg(root)
    _patch_repo(monkeypatch, root, cfg)

    # g1: standardized fasta_path exists and is preferred
    _write_text(root / "data" / "interim" / "genomes" / "fasta" / "g1.fna", ">c\nATG\n")
    # g2: fasta_path missing, raw path exists only as .gz (extension toggled)
    raw_gz = root / "data" / "raw" / "g2.fna.gz"
    raw_gz.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(raw_gz, "wt") as handle:
        handle.write(">c\nATG\n")

    header = "canonical_id\twinning_source\tfasta_path\tcanonical_raw_path\tstatus\tis_gtdb_representative\n"
    rows = [
        "g1\tncbi\tdata/interim/genomes/fasta/g1.fna\tdata/raw/g1.fna\tok\t1\n",
        "g2\ttier3\tdata/interim/genomes/fasta/g2.fna\tdata/raw/g2.fna\tok\t0\n",
        "g3\tncbi\tdata/interim/genomes/fasta/g3.fna\tdata/raw/g3.fna\tok\t0\n",  # unresolvable
        "g4\tncbi\tdata/interim/genomes/fasta/g4.fna\tdata/raw/g4.fna\tmissing\t0\n",  # status filtered
    ]
    _write_text(root / "data" / "processed" / "genome_index.tsv", header + "".join(rows))

    result = sm.build_inputs_from_genome_index(cfg)
    assert result["written"] == 2
    assert result["skipped"] == 1
    assert result["skipped_ids"] == ["g3"]

    emitted = sm.load_input_manifest(cfg["smorfinder"]["input_tsv"])
    by_id = {row.sample_id: row for row in emitted}
    assert set(by_id) == {"g1", "g2"}
    assert by_id["g1"].fasta_path == "data/interim/genomes/fasta/g1.fna"
    assert by_id["g2"].fasta_path == "data/raw/g2.fna.gz"
    assert all(row.mode == "single" for row in emitted)

    reps = sm.build_inputs_from_genome_index(cfg, representatives_only=True)
    assert reps["written"] == 1
