import os

import pandas as pd
import psycopg2

DB_URL = os.getenv("DATABASE_URL")

genomic_info_table = pd.read_csv("data/submission_packet_v5_2/genomic_info.csv")

needs_se = genomic_info_table[genomic_info_table["library_strategy"].isna()]
doesnt_need_se = genomic_info_table[
    ~genomic_info_table["library_strategy"].isna()
]
genomic_info_sample_list = needs_se["sample_id"].drop_duplicates().to_list()


breakpoint()
conn = psycopg2.connect(DB_URL)
bsse = pd.read_sql(
    f"""
    select distinct bsgf.biospecimen_id,
        --segf.sequencing_experiment_id,
        se.experiment_strategy,
        se.is_paired_end as se_paired_end,
        se.platform,
        se.instrument_model
    from biospecimen bs
    join biospecimen_genomic_file bsgf on bs.kf_id = bsgf.biospecimen_id
    join genomic_file gf on gf.kf_id = bsgf.genomic_file_id
    join sequencing_experiment_genomic_file segf on segf.genomic_file_id = gf.kf_id
    join sequencing_experiment se on segf.sequencing_experiment_id = se.kf_id
    where bs.kf_id in ({str(genomic_info_sample_list)[1:-1]})
    """,
    conn,
)

bs_counts = bsse.value_counts("biospecimen_id")
need_these_samples = bsse[
    bsse["biospecimen_id"].isin(bs_counts[bs_counts == 1].index)
]["biospecimen_id"].to_list()

bsse_info = doesnt_need_se[
    doesnt_need_se["sample_id"].isin(need_these_samples)
][
    [
        "library_id",
        "sample_id",
        "library_strategy",
        "library_layout",
        "platform",
        "instrument_model",
        "library_source",
    ]
].drop_duplicates()
with_bsinfo = needs_se.drop(
    columns=[
        "library_id",
        "library_strategy",
        "library_layout",
        "platform",
        "instrument_model",
        "library_source",
    ]
).merge(bsse_info, how="left")

for_bailey = pd.concat([doesnt_need_se, with_bsinfo])

breakpoint()
