"""marine_peptides: reusable library for the marine prokaryote novel-peptide discovery project.

Submodules:
    download         - genome/MAG acquisition and manifest handling
    orf_prediction   - ORF calling and peptide extraction helpers
    features         - sequence feature engineering for ML
    ml               - model training, inference, and evaluation

Keep reusable, importable, testable logic here. Scripts and Nextflow modules
should be thin wrappers that call into this package (see AGENTS.md).
"""

__version__ = "0.1.0"
