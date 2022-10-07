# CDS file submission

1. all-ccdi-move-manifest.py pulls from the dataservice and postgres to geneerate a manifest of all the files in the cds genomics bucket
2. submission_packet_prep.py takes the output of the above and generates the individual files for the cds submission and dbgap submission
3. qc.py does some qc work on the submission packets