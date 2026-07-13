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
- `meta`: `8` CPUs, `12 GB`, `24h`
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

## Troubleshooting

- If preflight fails on TensorFlow/protobuf import behavior, the workflow exports `PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python` before invoking SmORFinder.
- SmORFinder upstream parses `--phmm-overlap-cutoff` as an integer-valued option; keep it at `1` unless you intentionally patch the upstream CLI.
- If the final report or timeline already exists, the workflow config overwrites them automatically.
- If a sample directory exists without a valid `_SUCCESS.json`, that sample is treated as incomplete and re-run.
- Empty prediction sets are valid. The `.faa`, `.ffn`, `.gff`, or `.tsv` files may be empty or header-only and still count as a successful run.
