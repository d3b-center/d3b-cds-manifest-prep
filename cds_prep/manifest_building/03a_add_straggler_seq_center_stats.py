import os

import pandas as pd
import psycopg2

DB_URL = os.getenv("DATABASE_URL")

genomic_info_table = pd.read_csv("data/submission_packet/genomic_info.csv")
# copy the genomic_info_table so that we can modify the original
genomic_info_table.to_csv("data/temp/genomic_info_step3.csv", index=False)
del genomic_info_table["Unnamed: 0"]
mirna_stats = pd.read_csv(
    "/home/ubuntu/d3b-cds-manifest-prep/data/cbtn-miRNA-metrics.csv"
)
rna_wxs_stats = pd.read_csv(
    "/home/ubuntu/d3b-cds-manifest-prep/data/files_missing_info.filling.csv"
)
rna_wxs_stats = rna_wxs_stats[rna_wxs_stats["library_strategy"] != "miRNA-Seq"]
rna_wxs_stats["coverage"] = rna_wxs_stats["coverage"].apply(
    lambda x: None if x == "\\" else x
)
rna_wxs_stats["insert_size"] = rna_wxs_stats["insert_size"].apply(
    lambda x: None if x == "\\" else x
)
new_stats = pd.concat([rna_wxs_stats, mirna_stats])


out = genomic_info_table[~genomic_info_table["bases"].isna()]
out = pd.concat([out, new_stats[[i for i in out.columns]]])

assert len(out) == len(genomic_info_table)
out.to_csv("data/submission_packet/genomic_info.csv", index=False)
