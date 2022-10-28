import os

import pandas as pd
import psycopg2

DB_URL = os.getenv("DATABASE_URL")
USE_TEMP = True
if USE_TEMP:
    submission_packet_prefix = "data/temp/step_03/"
else:
    submission_packet_prefix = "data/submission_packet/"
genomic_info_table = pd.read_csv(submission_packet_prefix + "genomic_info.csv")
sc_gf = pd.read_csv(
    "/home/ubuntu/d3b-cds-manifest-prep/genomics_bucket/"
    "data/sequencing_center_genomic_info.csv"
)
breakpoint()
genomic_info_table.to_csv("data/temp/step_03/genomic_info.csv", index=False)

genomic_info_sample_list = (
    genomic_info_table["sample_id"].drop_duplicates().to_list()
)


# breakpoint()
conn = psycopg2.connect(DB_URL)
bssc = pd.read_sql(
    f"""
    select bs.kf_id as sample_id, bs.sequencing_center_id
    from biospecimen bs
    where bs.kf_id in ({str(genomic_info_sample_list)[1:-1]})
    """,
    conn,
)

gi_sc = genomic_info_table.merge(bssc, on="sample_id", how="left")

out = gi_sc.merge(
    sc_gf,
    left_on=("sequencing_center_id", "library_strategy"),
    right_on=("sequencing_center_id", "experiment_strategy"),
    how="left",
)

out.to_csv(submission_packet_prefix + "genomic_info.csv", index=False)
breakpoint()
