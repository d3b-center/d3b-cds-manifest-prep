# Seed Data Files

This directory holds data files used to generate CDS manifests.

## `file_sample_participant_map.csv`

This is the seed file mapping files, samples, and participants. This file is
used by default to generate all the manifests in the submision. This file is
also copied to the output directory as the file-sample-participant map.

## `openpedcan_histologies.csv`

This is an export from the d3b warehouse. This is used to generate diagnoses.

Generated with:

```sql
SELECT * FROM prod_reporting.openpedcan_histologies
```

## `CBTN - ICD-O.xlsx`

This is a mapping of rw diagnoses to ICD-O codes.

## `updated_nci_submit.csv`

This is a mapping from the bioinformatics unit between samples and genomic
sequencing information.
