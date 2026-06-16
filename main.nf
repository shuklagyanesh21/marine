#!/usr/bin/env nextflow

/*
 * marine - novel peptides from marine prokaryotes
 *
 * SCAFFOLD ONLY: stages are stubbed with TODO markers and no real logic yet.
 * Each process should remain a THIN wrapper that calls into the installable
 * `marine_peptides` package (see AGENTS.md). Implement processes under modules/
 * and import them here.
 */

nextflow.enable.dsl = 2

// ---------------------------------------------------------------------------
// Stage stubs (replace with real processes in modules/ and `include` them)
// ---------------------------------------------------------------------------

process DOWNLOAD_GENOMES {
    tag "download"
    publishDir "${params.data_raw}", mode: 'copy'

    output:
    path "download.placeholder"

    script:
    // TODO: call marine_peptides.download to fetch marine genomes/MAGs + write manifest
    """
    echo "TODO: download marine genomes/MAGs into ${params.data_raw}" > download.placeholder
    """
}

process QC_GENOMES {
    tag "checkm2"

    input:
    path genomes

    output:
    path "qc.placeholder"

    script:
    // TODO: run CheckM2; filter by params.min_completeness / params.max_contamination
    """
    echo "TODO: QC with CheckM2 (completeness>=${params.min_completeness}, contamination<=${params.max_contamination})" > qc.placeholder
    """
}

process PREDICT_ORFS {
    tag "pyrodigal"

    input:
    path genomes

    output:
    path "orf.placeholder"

    script:
    // TODO: predict ORFs/peptides (pyrodigal), min_protein_length=${params.min_protein_length}
    """
    echo "TODO: predict ORFs and peptides" > orf.placeholder
    """
}

process FEATURES {
    tag "features"

    input:
    path peptides

    output:
    path "features.placeholder"

    script:
    // TODO: engineer features via marine_peptides.features
    """
    echo "TODO: build features for ML" > features.placeholder
    """
}

process ML_NOVEL_PEPTIDES {
    tag "ml"

    input:
    path features

    output:
    path "ml.placeholder"

    script:
    // TODO: run ML models via marine_peptides.ml to flag novel peptides
    """
    echo "TODO: ML inference for novel peptide discovery" > ml.placeholder
    """
}

// ---------------------------------------------------------------------------
// Workflow wiring: download -> QC -> ORF -> features -> ML
// ---------------------------------------------------------------------------

workflow {
    genomes  = DOWNLOAD_GENOMES()
    qc       = QC_GENOMES(genomes)
    orfs     = PREDICT_ORFS(qc)
    feats    = FEATURES(orfs)
    ML_NOVEL_PEPTIDES(feats)
}
