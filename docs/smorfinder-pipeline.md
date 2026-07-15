# SmORFinder Pipeline

This workflow runs [SmORFinder](https://github.com/bhattlab/SmORFinder) on an explicit list of selected assemblies. It supports two assembly modes:

- `single` for isolate, draft single-organism, MAG, or SAG assemblies
- `meta` for true unbinned metagenome assemblies

The pipeline is intentionally separate from `main.nf`. Run `smorfinder.nf` directly.

For the current repository state, the manifest-backed inputs are assembled genomes/MAGs/SAGs.
If you do not have separate true community metagenome assemblies, keep those rows in `single`
mode and do not force `meta`.

## Inputs

Populate `config/smorfinder_inputs.tsv` with one row per selected assembly:

```tsv
sample_id	mode	fasta_path
isolate_GCA_000153145v1	single	data/raw/ncbi_dataset/data/GCA_000153145.1/GCA_000153145.1_ASM15314v1_genomic.fna.gz
marine_metagenome_01	meta	data/interim/metagenomes/marine_metagenome_01.fna.gz
```

Rules:

- `sample_id` must be unique and match `^[A-Za-z0-9][A-Za-z0-9._-]*$`
- `mode` must be `single` or `meta`
- `fasta_path` must be project-relative and point to an existing FASTA or gzipped FASTA
- The workflow does not auto-discover metagenomes; you must list them explicitly

### Generating inputs for the whole collection

Hand-writing rows only makes sense for pilots. To enqueue every standardized genome, expand
the master index (`data/processed/genome_index.tsv`, config key `smorfinder.input_index`) into
the input TSV:

```bash
PYTHONPATH=src python3 scripts/12_build_smorfinder_inputs_from_index.py
```

This overwrites `config/smorfinder_inputs.tsv` with one `single`-mode row per canonical genome
that has `status == ok` and a FASTA resolvable on disk (it prefers the standardized
`fasta_path`, then falls back to `canonical_raw_path`, toggling the `.gz` suffix as needed).
`sample_id` is set to `canonical_id`. Genomes with no resolvable FASTA are skipped and reported.
Useful flags:

- `--limit N` — emit only the first `N` genomes (staged rollout / scale test)
- `--representatives-only` — restrict to GTDB representative genomes
- `--output PATH` — write elsewhere instead of the configured `input_tsv`

The generated file is regenerable data, not curated config; do not treat it as a committed
manifest.

## Environment

SmORFinder runs in its own conda prefix because upstream `smorfinder==1.0.0` depends on `tensorflow==2.3.1` and Python 3.8.

- Env spec: `env/smorfinder.yml`
- Nextflow helper env: `env/nextflow-utils.yml`
- Managed prefix: `../envs/smorfinder`
- Asset checksum manifest: `env/smorfinder-assets.sha256`

On each workflow launch, a serialized preflight step will:

1. Create or update the dedicated env
2. Run `smorf --help` once to trigger the upstream model download if needed
3. Verify the bundled binaries and model files against `env/smorfinder-assets.sha256`
4. Write `data/interim/smorfinder/preflight.ok.json`

## Workflow commands

Local stub/syntax check:

```bash
nextflow run smorfinder.nf -stub-run -profile standard
```

Local execution:

```bash
nextflow run smorfinder.nf -profile standard
```

SLURM execution:

```bash
nextflow run smorfinder.nf -profile slurm -resume
```

The `slurm` profile uses `env/nextflow-utils.yml` for the lightweight Python utility steps and runs SmORFinder itself from the dedicated prefix created under `../envs/smorfinder`.

## Hybrid single -> meta strategy (recommended for the full collection)

SmORFinder bundles a modified Prodigal ("DeepSmORFNET") that **segfaults during single-mode
training** on a subset of genomes (mostly fragmented SAGs). Prodigal's exit code is ignored
upstream, so the crash surfaces later as `IndexError` in `filter_prodigal_small_genes` and
`smorf single` exits 1. Meta mode uses a pretrained model (no training phase) and recovers most
of those failures.

On the full 112,414-genome collection the observed single-mode failure rate was **593 / 112,414
(0.53%)**, not the ~19% suggested by an early concurrent Prodigal probe (that probe was inflated
by resource contention under parallel load).

The hybrid strategy calls trained single-mode genes where they work, then recovers the crashers
in meta mode, in three steps:

```bash
# Phase 1 - single mode for the whole list (crashers are ignored, not fatal)
nextflow run smorfinder.nf -profile slurm -resume

# Promote every genome that lacks a valid single/<id>/_SUCCESS.json to meta mode
PYTHONPATH=src python3 scripts/13_promote_smorfinder_failures.py

# Phase 2 - -resume now re-runs only the promoted (meta) genomes
nextflow run smorfinder.nf -profile slurm -resume
```

Chain them into one automatic run with `&&` (each phase must succeed before the next):

```bash
nextflow run smorfinder.nf -profile slurm -resume \
  && PYTHONPATH=src python3 scripts/13_promote_smorfinder_failures.py \
  && nextflow run smorfinder.nf -profile slurm -resume
```

Notes:

- Only run the promotion step after a **completed** single-mode pass; otherwise genomes that
  simply have not run yet would be promoted prematurely. The `&&` chain enforces this because an
  interrupted `nextflow run` exits non-zero.
- The promotion is idempotent: single successes stay single, rows already in `meta` are untouched.
- A single genome failure no longer terminates the batch: `SMORFINDER_RUN` retries only transient
  resource exit codes (137/140/143/247) and otherwise **ignores** the genome. Non-completed
  genomes appear in the run manifest with a non-`completed` status.

## Outputs

Per-sample outputs are published to:

- `data/interim/smorfinder/single/<sample_id>/`
- `data/interim/smorfinder/meta/<sample_id>/`

Each completed sample directory contains:

- `<sample_id>.faa`
- `<sample_id>.ffn`
- `<sample_id>.gff`
- `<sample_id>.tsv`
- `_SUCCESS.json`

Intermediate bookkeeping files:

- `data/interim/smorfinder/normalized_inputs.tsv`
- `data/interim/smorfinder/input_status.tsv`
- `data/interim/smorfinder/pending_inputs.tsv`
- `data/interim/smorfinder/preflight.ok.json`

Processed run summary:

- `data/processed/smorfinder_run_manifest.tsv`

Hard failures (regenerable from the run manifest; `results/` is gitignored):

```bash
mkdir -p results/tables
awk -F'\t' 'BEGIN{OFS="\t"}
  NR==1 { print "sample_id","mode","fasta_path","status","reason","output_dir","success_marker"; next }
  $4=="failed" || $4=="pending" { print $1,$2,$3,$4,$5,$6,$7 }
' data/processed/smorfinder_run_manifest.tsv > results/tables/smorfinder_hard_failures.tsv
```

## Full collection run (2026-07-13 → 2026-07-14) — FINISHED

Hybrid SLURM run of all resolvable genomes from `data/processed/genome_index.tsv`.
**Do not re-run** unless inputs or SmORFinder cutoffs/assets change.

| Metric | Count |
|--------|------:|
| Input genomes | 112,414 |
| Phase 1 kept `single` (valid `_SUCCESS.json`) | 111,821 |
| Promoted to `meta` after single-mode failure | 593 |
| Phase 2 `meta` successes | 276 |
| Hard failures (no success in either mode) | 317 |
| **Coverage** | **112,097 / 112,414 (99.72%)** |

Phase wall times (Nextflow summaries):

- Phase 1 (`single`): **1d 8h 40m**, ~799 CPU-hours, 593 ignored
- Phase 2 (`meta`): **12m**, ~13.5 CPU-hours, 317 ignored

Prediction totals on successful genomes: **474,562** smORFs (mean ≈ 4.2 / genome; 12,240 genomes with zero predictions — still valid successes).

Hard-failure composition: **315 GORG** + **2 MarDB**. Almost all residual failures are GORG SAGs that crash even in meta mode. List: `results/tables/smorfinder_hard_failures.tsv` (regenerate with the awk above).

Primary artifacts:

- Per-genome outputs: `data/interim/smorfinder/{single,meta}/<sample_id>/`
- Run manifest: `data/processed/smorfinder_run_manifest.tsv`
- Run log: `logs/smorfinder-hybrid-20260713-133555.log`

Note: `data/interim/smorfinder/pending_inputs.tsv` still lists the 593 genomes that were pending at the *start* of phase 2; after phase 2, trust the run manifest / `_SUCCESS.json` counts, not that stale pending file.

## Resumability

There are two resume layers:

1. Native Nextflow resume with `-resume` while `work/` is intact
2. Output-level skipping via `_SUCCESS.json` plus input/parameter fingerprints

An existing sample output is skipped only when:

- all expected result files exist
- `_SUCCESS.json` exists
- the current input FASTA stat fingerprint still matches
- the current SmORFinder cutoff/asset signature still matches

If the marker is stale or invalid, that sample is re-queued.

## Resource defaults

Configured in `config/config.yaml`:

- `single`: `1` CPU, `3 GB`, `12h`
- `meta`: `2` CPUs, `4 GB`, `24h` (right-sized for MAG/SAG recovery; raise if running true community metagenomes)
- workflow max parallelism: `12` tasks

The `meta` command passes `--threads <task.cpus>` to SmORFinder. `single` does not have a thread flag upstream, so parallelism comes from running many tasks at once rather than oversizing one task.

### Running at scale (~112k genomes)

- Concurrency is `executor.queueSize = smorfinder.max_parallel_tasks` (default `12`). Raise it in
  `config/config.yaml` for throughput. `single` reserves `3 GB` each, so on a 110 GB machine keep
  it around `30` to stay memory-safe.
- The run is fully resumable: launch once, and completed genomes are skipped on later `-resume`
  invocations via their `_SUCCESS.json` markers.
- The conda package cache lives under `/home` (see `conda config --show pkgs_dirs`), which can be
  tight. If preflight fails with `NoSpaceLeftError` during the env update, run `conda clean -a`
  and relaunch.
- The full hybrid collection run completed 2026-07-14 (see table above). Re-running without
  input/cutoff/asset changes only re-attempts the 317 hard failures.

## Troubleshooting

- If preflight fails on TensorFlow/protobuf import behavior, the workflow exports `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` before invoking SmORFinder.
- SmORFinder upstream parses `--phmm-overlap-cutoff` as an integer-valued option; keep it at `1` unless you intentionally patch the upstream CLI.
- If the final report or timeline already exists, the workflow config overwrites them automatically.
- If a sample directory exists without a valid `_SUCCESS.json`, that sample is treated as incomplete and re-run.
- `smorf single` failing with `IndexError: list index out of range` in `filter_prodigal_small_genes` means the bundled Prodigal segfaulted during single-mode training and left an empty GFF. Recover that genome with meta mode (see the hybrid strategy above); it is not a data problem.
- Empty prediction sets are valid. The `.faa`, `.ffn`, `.gff`, or `.tsv` files may be empty or header-only and still count as a successful run.
