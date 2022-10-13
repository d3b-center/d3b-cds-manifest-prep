import os

import pandas as pd
import psycopg2

DB_URL = os.getenv("DATABASE_URL")

genomic_info = pd.read_csv(
    "/home/ubuntu/d3b-cds-manifest-prep/genomics_bucket/"
    "data/submission_packet_v5_1/genomic_info.csv"
)
sc_gf = pd.read_csv(
    "/home/ubuntu/d3b-cds-manifest-prep/genomics_bucket/"
    "data/sequencing_center_genomic_info.csv"
)

sample_list = genomic_info["sample_id"].drop_duplicates().to_list()


# breakpoint()
conn = psycopg2.connect(DB_URL)
bssc = pd.read_sql(
    f"""
    select bs.kf_id as sample_id, bs.sequencing_center_id
    from biospecimen bs
    where bs.kf_id in ({str(sample_list)[1:-1]})
    """,
    conn,
)

gi_sc = genomic_info.merge(bssc, on="sample_id", how="left")

out = gi_sc.merge(
    sc_gf,
    left_on=("sequencing_center_id", "library_strategy"),
    right_on=("sequencing_center_id", "experiment_strategy"),
    how="left",
)

breakpoint()
