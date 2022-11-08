import os

import pandas as pd
import psycopg2
import sevenbridges as sbg
from d3b_cavatica_tools.utils.logging import get_logger

from queries import (
    all_scrapes_sql,
    already_transferred_sql,
    diagnosis_query,
    participant_query,
    pnoc_sql,
    sample_query,
    sequencing_query,
)
from utils import *

logger = get_logger(__name__, testing_mode=False)

DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)

# Read in the  files from previous steps
out = pd.read_csv("data/temp/ccdi_manifest.csv")
pnoc_manifest = pd.read_csv("data/temp/pnoc_manifest.csv")
merged_cbtn = pd.read_csv("data/temp/ccdi_manifest_cbtn-with_kfids.csv")
s3scrapes = pd.read_csv("data/temp/s3scrapes.csv")
# Load scrape of cds bucket
scrape = pd.read_csv(
    "/home/ubuntu/d3b-cds-manifest-prep/genomics_bucket/data/cds_scrape.tsv",
    sep="\t",
)
scrape["file_url_in_cds"] = "s3://" + scrape["Bucket"] + "/" + scrape["Key"]


# generate the manifest of participants
breakpoint()
participants_list = [
    i for i in out["pt_id"].explode("pt_id").drop_duplicates().to_list() if i
]

logger.info("querying for manifest of participants")
participant_manifest = pd.read_sql(
    participant_query(participants_list),
    conn,
)
gender_map = {
    "Female": "female",
    "Male": "male",
    "Not Reported": "not reported",
    "Not Available": "unknown",
}
race_map = {
    "White": "white",
    "Reported Unknown": "unknown",
    "Not Reported": "not reported",
    "Black or African American": "black or african american",
    "Other": "other",
    "Native Hawaiian or Other Pacific Islander": "native hawaiian or other pacific islander",
    "More Than One Race": "other",
    "Asian": "asian",
    "American Indian or Alaska Native": "american indian or alaska native",
    "Not Available": "unknown",
}
ethnicity_map = {
    "Reported Unknown": "unknown",
    "Not Hispanic or Latino": "not hispanic or latino",
    "Not Reported": "not reported",
    "Hispanic or Latino": "hispanic or latino",
    "Not Available": "unknown",
}

participant_manifest["gender"] = participant_manifest["gender"].apply(
    lambda x: gender_map.get(x)
)
participant_manifest["race"] = participant_manifest["race"].apply(
    lambda x: race_map.get(x)
)
participant_manifest["ethnicity"] = participant_manifest["ethnicity"].apply(
    lambda x: ethnicity_map.get(x)
)
participant_manifest.to_csv(
    "data/submission_packet/participant.csv", index=False
)

# Generate the manifest of samples
sample_list = [
    i
    for i in out["bs_id"]
    .explode("bs_id")
    .explode("bs_id")
    .drop_duplicates()
    .to_list()
    if i
]
logger.info("querying for manifest of samples")
sample_manifest = pd.read_sql(
    sample_query(sample_list),
    conn,
)
status_map = {"Tumor": "tumor", "Normal": "normal", "Non-Tumor": "normal"}
sample_manifest["sample_tumor_status"] = sample_manifest[
    "sample_tumor_status"
].apply(lambda x: status_map.get(x))


def sample_type(row):
    tissue = row["sample_type"]
    status = row["sample_tumor_status"]
    if status == "tumor":
        if tissue == "Derived Cell Line":
            return "Cell Lines"
        elif tissue == "Solid Tissue":
            return "Tumor"
        elif tissue == "Tumor":
            return "Tumor"
        else:
            logger.error(f"Type unknown. Tissue: {tissue}, Status: {status}")
    elif status == "normal":
        if tissue == "Solid Tissue":
            return "Solid Tissue Normal"
        elif tissue == "Peripheral Whole Blood":
            return "Blood Derived Normal"
        elif tissue == "Saliva":
            return "Saliva"
        elif tissue == "Not Reported":
            return "Not Reported"


# set sample anatomical site to "Central Nervous System"
sample_manifest["sample_type"] = sample_manifest.apply(sample_type, axis=1)
sample_manifest = sample_manifest.drop(columns=["sample_anatomical_site"])
sample_manifest.to_csv("data/submission_packet/sample.csv", index=False)

# generate the file table
file_table = out


def file_id(x):
    if not x:
        return None
    if isinstance(x, str):
        return x
    elif isinstance(x, list):
        return x[0]
    else:
        logger.error("type unknown")
        logger.info(type(x))


file_table["file_id"] = file_table["gf_id"].apply(file_id)
file_table["file_url_in_cds"] = file_table["s3path"].str.replace(
    "s3://", "s3://cds-246-phs002517-sequencefiles-p30-fy20/"
)
file_table = file_table.rename(
    columns={
        "name": "file_name",
        "file_format": "file_type",
        "size": "file_size",
        "hash": "md5sum",
    }
)[
    [
        "file_id",
        "file_name",
        "file_type",
        "file_size",
        "md5sum",
        "file_url_in_cds",
    ]
]

# drop out new pnoc008 files
merged = scrape[["file_url_in_cds"]].merge(
    file_table, how="right", indicator=True
)
file_table = merged[merged["_merge"] == "both"].drop(columns=["_merge"])

file_table.to_csv("data/submission_packet/file.csv", index=False)
# mapping
mapping = out[["gf_id", "pt_id", "bs_id"]].rename(
    columns={
        "gf_id": "file_id",
        "pt_id": "participant_id",
        "bs_id": "sample_id",
    }
)
mapping["file_id"] = mapping["file_id"].apply(file_id)
mapping["participant_id"] = mapping["participant_id"].apply(file_id)
mapping["sample_id"] = mapping["sample_id"].apply(lambda x: file_id(file_id(x)))
# clear out files not in the file table
merged = mapping.merge(file_table[["file_id"]], how="outer", indicator=True)
mapping = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
mapping.to_csv(
    "data/submission_packet/file_sample_participant_map.csv", index=False
)

# Genomic Information

logger.info("querying for manifest of genomic information")
genomic_info_table_raw = pd.read_sql(
    sequencing_query(sample_list),
    conn,
)

# bar = pd.read_sql(
#     sequencing_query2(sl),
#     conn,
# )
# gi = build_unique_genomic_manifest(genomic_info_table_raw)
genomic_info_table = genomic_info_table_raw[
    [
        "sequencing_experiment_id",
        "biospecimen_id",
        "genomic_file_id",
        "experiment_strategy",
        "se_paired_end",
        "platform",
        "instrument_model",
        "reference_genome",
    ]
].rename(
    columns={
        "sequencing_experiment_id": "library_id",
        "biospecimen_id": "sample_id",
        "genomic_file_id": "file_id",
        "experiment_strategy": "library_strategy",
        "se_paired_end": "library_layout",
        "platform": "platform",
        "instrument_model": "instrument_model",
        "reference_genome": "reference_genome_assembly",
    }
)
breakpoint()
source_map = {
    "WGS": "GENOMIC",
    "RNA-Seq": "TRANSCRIPTOMIC",
    "WXS": "GENOMIC",
    "miRNA-Seq": "TRANSCRIPTOMIC",
    "Targeted Sequencing": "GENOMIC",
    "Other": "GENOMIC",  # There's a random cbtn bam file
}
genomic_info_table["library_source"] = genomic_info_table[
    "library_strategy"
].apply(lambda x: source_map.get(x))

# genome_reference_map = {
#     "hg19": "hg19",
#     "GRCh38": "GRCh38",
#     "None": "hg19",
#     "Not Reported": "hg19",
#     "GRCh37": "GRCh37",
#     "hg19_with_GenBank_Viral_Genomes": "hg19",
#     "Not Applicable": "hg19",
#     "['GRCh38', 'hg19_with_GenBank_Viral_Genomes']": "hg19",
#     "['GRCh38', 'hg19', 'RefSeq_Build_73']": "hg19",
#     "['GRCh38', 'RefSeq_Build_73']": "hg19",
#     "['GRCh38', 'hg19']": "hg19",
# }


def reference_genome(x):
    if not x:
        return "hg19"
    elif x in ("Not Applicable", "Not Reported"):
        return "hg19"
    else:
        return x


genomic_info_table["reference_genome_assembly"] = genomic_info_table[
    "reference_genome_assembly"
].apply(reference_genome)


def paired_end_handler(x):
    if isinstance(x, bool):
        if x:
            return "paired-end"
        else:
            return "single"
    else:
        return "unknown"


genomic_info_table["library_layout"] = genomic_info_table[
    "library_layout"
].apply(paired_end_handler)
instrument_model_map = {
    "Novaseq 6000": "Illumina NovaSeq 6000",
    "HiSeq 2500": "Illumina HiSeq 2500",
    "Not Reported": "Not Reported",
    "HiSeq": "Illumina HiSeq",
    "HiSeq X": "Illumina HiSeq",
    "DNBseq": "DNBSEQ",
    "DNBSeq": "DNBSEQ",  # This is BIG
}
platform_map = {
    "Illumina": "ILLUMINA",
    "Other": "BGISEQ",
    "Not Reported": "Not Reported",
}
genomic_info_table["platform"] = genomic_info_table["platform"].apply(
    lambda x: platform_map.get(x)
)
genomic_info_table["instrument_model"] = genomic_info_table[
    "instrument_model"
].apply(lambda x: instrument_model_map.get(x))

genomic_info_table.to_csv(
    "data/submission_packet/genomic_info.csv", index=False
)


merged = genomic_info_table.merge(
    mapping.drop(columns=["participant_id"]).drop_duplicates(),
    on=["file_id", "sample_id"],
    how="outer",
)
merged.to_csv(
    "data/submission_packet/genomic_info_merged_with_mapping.csv", index=False
)


conn.close()
# create the dbgap files

# SSM
ssm = (
    mapping[["participant_id", "sample_id"]]
    .drop_duplicates()
    .rename(columns={"participant_id": "SUBJECT_ID", "sample_id": "SAMPLE_ID"})
)
ssm.to_csv("data/3a_SSM_DS.txt", sep="\t", index=False)

# Subject consent


subject_consent = participant_manifest[["participant_id", "gender"]].rename(
    columns={"participant_id": "SUBJECT_ID", "gender": "SEX"}
)
subject_consent["CONSENT"] = 1
subject_consent["SEX"] = subject_consent["SEX"].apply(
    lambda x: {
        "female": "2",
        "male": "1",
        "unknown": "UNK",
        "not reported": "UNK",
    }.get(x)
)
subject_consent[["SUBJECT_ID", "CONSENT", "SEX"]].to_csv(
    "data/2a_SubjectConsent_DS.txt", sep="\t", index=False
)
breakpoint()


scrape = pd.read_csv("data/cds_scrape.tsv", sep="\t")
scrape["file_url_in_cds"] = "s3://" + scrape["Bucket"] + "/" + scrape["Key"]
merged = scrape.merge(file_table, how="outer", indicator=True)
mismatch = merged[merged["_merge"] != "both"]
mismatch.to_csv("data/scrape_mismatch2.csv")

pnoc_unique = build_unique_file_manifest(pnoc_manifest)
all = pd.concat([pnoc_unique[["s3path"]], merged_cbtn[["s3path"]]])
out["s3path"] = "s3://" + out["file_path"]
merge = all.merge(out, on=["s3path"], how="outer", indicator=True)

unregistered_all = merge[merge["_merge"] != "both"]
unregistered_all.merge(s3scrapes, how="left")
unregistered_move_manifest = unregistered_all[["s3path"]].merge(
    s3scrapes, on="s3path", how="left"
)
unregistered_move_manifest["key_source"] = unregistered_move_manifest[
    "s3path"
].apply(lambda x: x.replace("s3://", "").partition("/")[2])
unregistered_move_manifest["key_cds"] = unregistered_move_manifest[
    "s3path"
].apply(lambda x: x.replace("s3://", ""))
unregistered_move_manifest["bucket_source"] = unregistered_move_manifest[
    "s3path"
].apply(lambda x: x.replace("s3://", "").partition("/")[0])
unregistered_move_manifest[
    "bucket_cdsgenomics_deletefrom"
] = "cds-246-phs002517-sequencefiles-p30-fy20"
unregistered_move_manifest[
    "bucket_cdsother_moveto"
] = "cds-246-phs002517-p30-fy20"
unregistered_move_manifest.to_csv(
    "data/unregistered_move_manifest.csv", index=False
)
