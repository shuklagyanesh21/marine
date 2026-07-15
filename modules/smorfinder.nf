process SMORFINDER_RUN {
    tag { "${mode}:${sample_id}" }
    label 'smorfinder'
    conda "${projectDir}/env/nextflow-utils.yml"
    cpus {
        mode == 'meta' ? params.smorfinder_meta_cpus as int : params.smorfinder_single_cpus as int
    }
    memory {
        def base = mode == 'meta' ? params.smorfinder_meta_memory_gb as double : params.smorfinder_single_memory_gb as double
        def scale = Math.pow(params.smorfinder_retry_memory_factor as double, task.attempt - 1)
        return "${Math.ceil(base * scale) as int} GB"
    }
    time {
        mode == 'meta' ? params.smorfinder_meta_time : params.smorfinder_single_time
    }
    maxRetries params.smorfinder_retry_count as int
    errorStrategy {
        // Retry only transient resource failures (OOM/time/signals); after retries
        // are exhausted -- and for any other exit code (e.g. an upstream Prodigal
        // segfault surfacing as exit 1) -- ignore the single genome so one bad
        // assembly cannot terminate the whole ~112k batch. Non-completed genomes
        // are reported in the run manifest and re-attempted on a later -resume.
        def retryCodes = [137, 140, 143, 247]
        (retryCodes.contains(task.exitStatus) && task.attempt <= (params.smorfinder_retry_count as int)) ? 'retry' : 'ignore'
    }
    publishDir(
        { "${params.smorfinder_out_dir}/${mode}" },
        mode: 'copy',
        overwrite: true
    )

    input:
    tuple val(sample_id), val(mode), val(fasta_path)
    path preflight_stamp

    output:
    path "${sample_id}"

    script:
    """
    set -euo pipefail

    PYTHONPATH="${projectDir}/src" python "${projectDir}/scripts/11_run_smorfinder_task.py" \\
        --sample-id "${sample_id}" \\
        --mode "${mode}" \\
        --fasta-path "${fasta_path}" \\
        --cpus ${task.cpus} \\
        --work-dir .
    """
}
