"""ORF calling and peptide extraction helpers."""

from marine_peptides.orf_prediction.smorfinder import (
    SmorfinderInput,
    build_inputs_from_genome_index,
    build_smorf_command,
    ensure_smorfinder_environment,
    load_input_manifest,
    promote_failures_to_meta,
    run_smorfinder_task,
    stage_fasta_for_smorf,
    summarize_run_manifest,
    write_prepared_manifests,
    write_success_marker,
)

__all__ = [
    "SmorfinderInput",
    "build_inputs_from_genome_index",
    "build_smorf_command",
    "ensure_smorfinder_environment",
    "load_input_manifest",
    "promote_failures_to_meta",
    "run_smorfinder_task",
    "stage_fasta_for_smorf",
    "summarize_run_manifest",
    "write_prepared_manifests",
    "write_success_marker",
]
