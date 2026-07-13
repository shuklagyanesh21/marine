#!/usr/bin/env nextflow

nextflow.enable.dsl = 2

include { SMORFINDER_RUN } from './modules/smorfinder'

process PREPARE_SMORFINDER_INPUTS {
    tag "prepare"
    label 'utility'
    cache false
    conda "${projectDir}/env/nextflow-utils.yml"

    output:
    path "normalized_inputs.tsv", emit: normalized_tsv
    path "input_status.tsv", emit: status_tsv
    path "pending_inputs.tsv", emit: pending_tsv

    script:
    """
    set -euo pipefail
    PYTHONPATH="${projectDir}/src" python "${projectDir}/scripts/08_prepare_smorfinder_inputs.py"
    ln -sf "${projectDir}/${params.smorfinder_normalized_tsv}" normalized_inputs.tsv
    ln -sf "${projectDir}/${params.smorfinder_input_status_tsv}" input_status.tsv
    ln -sf "${projectDir}/${params.smorfinder_pending_tsv}" pending_inputs.tsv
    """
}

process SMORFINDER_PREFLIGHT {
    tag "preflight"
    label 'utility'
    cache false
    conda "${projectDir}/env/nextflow-utils.yml"

    output:
    path "preflight.ok.json", emit: stamp

    script:
    """
    set -euo pipefail
    PYTHONPATH="${projectDir}/src" python "${projectDir}/scripts/10_smorfinder_preflight.py"
    ln -sf "${projectDir}/${params.smorfinder_preflight_stamp}" preflight.ok.json
    """
}

process SUMMARIZE_SMORFINDER {
    tag "summarize"
    label 'utility'
    cache false
    conda "${projectDir}/env/nextflow-utils.yml"
    publishDir "${params.data_processed}", mode: 'copy', overwrite: true

    input:
    val sample_dirs
    path status_tsv

    output:
    path "smorfinder_run_manifest.tsv"

    script:
    """
    set -euo pipefail
    PYTHONPATH="${projectDir}/src" python "${projectDir}/scripts/09_summarize_smorfinder.py"
    ln -sf "${projectDir}/${params.smorfinder_processed_manifest}" smorfinder_run_manifest.tsv
    """
}

workflow {
    prepared = PREPARE_SMORFINDER_INPUTS()
    preflight = SMORFINDER_PREFLIGHT()

    sample_inputs = prepared.pending_tsv.flatMap { pending_tsv ->
        def lines = pending_tsv.text.readLines().findAll { it?.trim() }
        if( lines.size() <= 1 )
            return []

        def header = lines[0].split('\t', -1) as List
        lines.drop(1).collect { line ->
            def values = line.split('\t', -1) as List
            def row = [:]
            header.eachWithIndex { key, idx -> row[key] = idx < values.size() ? values[idx] : '' }
            tuple(
                row.sample_id as String,
                row.mode as String,
                row.fasta_path as String,
            )
        }
    }

    smorf_results = SMORFINDER_RUN(sample_inputs, preflight.stamp)

    SUMMARIZE_SMORFINDER(smorf_results.collect().ifEmpty([]), prepared.status_tsv)
}
