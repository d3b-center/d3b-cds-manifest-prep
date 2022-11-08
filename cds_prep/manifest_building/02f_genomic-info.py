import os

import pandas as pd
import psycopg2

DB_URL = os.getenv("DATABASE_URL")
USE_TEMP = True
if USE_TEMP:
    genomic_info_table = pd.read_csv("data/temp/genomic_info_02e.csv")
else:
    genomic_info_table = pd.read_csv("data/submission_packet/genomic_info.csv")
    genomic_info_table.to_csv("data/temp/genomic_info_02e.csv", index=False)
needs_se = genomic_info_table[genomic_info_table["library_strategy"].isna()]
doesnt_need_se = genomic_info_table[
    ~genomic_info_table["library_strategy"].isna()
]
genomic_info_sample_list = needs_se["sample_id"].drop_duplicates().to_list()

sc_gf = pd.read_csv(
    "/home/ubuntu/d3b-cds-manifest-prep/genomics_bucket/"
    "data/sequencing_center_genomic_info.csv"
)
bs_platform_mat = pd.read_excel(
    "/home/ubuntu/d3b-cds-manifest-prep/data/"
    "BS_SeqCenter_Platform_Model.xlsx"
)
# breakpoint()
conn = psycopg2.connect(DB_URL)
bsse = pd.read_sql(
    f"""
    select distinct bsgf.biospecimen_id,
        segf.sequencing_experiment_id,
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
          and se.is_paired_end
    """,
    conn,
)

# connect sequencing experiments to the genomic files that need it
new_se = (
    needs_se.drop(
        columns=[
            "library_id",
            "library_strategy",
            "library_layout",
            "platform",
            "instrument_model",
            "library_source",
            "experiment_strategy",
            "name",
            "bases",
            "number_of_reads",
            "coverage",
            "avg_read_length",
            "insert_size",
        ]
    )
    .merge(bsse, how="left", left_on="sample_id", right_on="biospecimen_id")
    .merge(
        sc_gf, how="left", on=["sequencing_center_id", "experiment_strategy"]
    )
)

source_map = {
    "WGS": "GENOMIC",
    "RNA-Seq": "TRANSCRIPTOMIC",
    "WXS": "GENOMIC",
    "miRNA-Seq": "TRANSCRIPTOMIC",
    "Targeted Sequencing": "GENOMIC",
    "Other": "GENOMIC",  # There's a random cbtn bam file
}


new_se["library_source"] = new_se["experiment_strategy"].apply(
    lambda x: source_map.get(x)
)


def paired_end_handler(x):
    if isinstance(x, bool):
        if x:
            return "paired-end"
        else:
            return "single"
    else:
        return "unknown"


new_se["library_layout"] = new_se["se_paired_end"].apply(paired_end_handler)


# sort the columns and set column names

new_se = new_se.rename(
    columns={
        "sequencing_experiment_id": "library_id",
        "experiment_strategy": "library_strategy",
    }
)[
    [
        "library_id",
        "sample_id",
        "file_id",
        "library_strategy",
        "library_layout",
        "platform",
        "instrument_model",
        "reference_genome_assembly",
        "library_source",
        "bases",
        "number_of_reads",
        "coverage",
        "avg_read_length",
        "insert_size",
    ]
]

doesnt_need_se = doesnt_need_se.drop(
    columns=[
        "sequencing_center_id",
        "experiment_strategy",
        "name",
    ]
)
out = pd.concat([doesnt_need_se, new_se])

out = out.merge(
    bs_platform_mat,
    how="left",
    left_on="sample_id",
    right_on="Kids_First_Biospecimen_ID",
)
instrument_model_map = {
    "Hiseq X Ten": "Hiseq X Ten",
    "Hiseq 2500": "Illumina Hiseq 2500",
    "NovaSeq 6000": "Illumina NovaSeq 6000",
    "BGISEQ-500": "BGISEQ-500",
}

out["Instrument Model"] = out["Instrument Model"].apply(
    lambda x: instrument_model_map.get(x)
)
out = out.drop(
    columns=["platform", "instrument_model", "Kids_First_Biospecimen_ID"]
).rename(
    columns={"Platform": "platform", "Instrument Model": "instrument_model"}
)[
    [
        "library_id",
        "sample_id",
        "file_id",
        "library_strategy",
        "library_layout",
        "platform",
        "instrument_model",
        "reference_genome_assembly",
        "library_source",
        "bases",
        "number_of_reads",
        "coverage",
        "avg_read_length",
        "insert_size",
    ]
]
# Get harmonization status of files
file_list = out["file_id"].drop_duplicates().to_list()
harmonized_files = pd.read_sql(
    f"""select kf_id
      from genomic_file 
      where kf_id in ({str(file_list)[1:-1]}) 
            and is_harmonized""",
    conn,
)
harmonized_file_list = harmonized_files["kf_id"].to_list()
out["reference_genome_assembly"] = out.apply(
    lambda row: "GRCh38"
    if row["file_id"] in harmonized_file_list
    else row["reference_genome_assembly"],
    axis=1,
)
out["library_layout"] = out.apply(
    lambda row: "paired-end"
    if row["library_strategy"] == "WGS"
    else row["library_layout"],
    axis=1,
)
out["library_strategy"] = out.apply(
    lambda row: "Targeted-Capture"
    if row["library_strategy"] == "Targeted Sequencing"
    else row["library_strategy"],
    axis=1,
)
out["library_id"] = (
    out["library_id"] + "__" + out["sample_id"] + "__" + out["file_id"]
)


out.to_csv("data/submission_packet/genomic_info.csv", index=False)
breakpoint()
