import os
from ast import literal_eval

import pandas as pd
import psycopg2
import sevenbridges as sbg
from d3b_cavatica_tools.utils.logging import get_logger

from queries import (
    all_scrapes_sql,
    already_transferred_sql,
    participant_query,
    pnoc_sql,
    sample_query,
    sequencing_query,
)
from utils import *

logger = get_logger(__name__, testing_mode=False)

DB_URL = os.getenv("DATABASE_URL")

out = pd.read_csv("data/ccdi_manifest.csv")


def fix_id(x):
    """fix IDs, so that list ids are converted from strings to list

    :param x: thing to convert
    :type x: str
    :return: x as list or str as applicable
    """
    if "[" in x:
        return literal_eval(x)
    else:
        return x


out["pt_id"] = out["pt_id"].apply(fix_id)
out["gf_id"] = out["gf_id"].apply(fix_id)
out["bs_id"] = out["bs_id"].apply(fix_id)
out["external_participant_id"] = out["external_participant_id"].apply(fix_id)
out["external_aliquot_id"] = out["external_aliquot_id"].apply(fix_id)
out["external_sample_id"] = out["external_sample_id"].apply(fix_id)
# Load scrape of cds bucket
scrape = pd.read_csv("data/cds_scrape.tsv", sep="\t")
scrape["file_url_in_cds"] = "s3://" + scrape["Bucket"] + "/" + scrape["Key"]


# Generate the manifests for CDS

conn = psycopg2.connect(DB_URL)

# generate the file table


def file_id(x, alphabetize=False):
    if not x:
        return None
    if isinstance(x, str):
        return x
    elif isinstance(x, list):
        if alphabetize:
            return sorted(x)[0]
        else:
            return x[0]
    else:
        logger.error("type unknown")
        logger.info(type(x))


def different_external_ids(x):
    if isinstance(x, list):
        if len(x) != len(set(x)):
            logger.debug("list of alliquots contains duplicates")
            return False
        else:
            return True
    else:
        return False


def sample_id(row):
    logger.debug("extracting ids")
    bs_id = row["bs_id"][0]
    aliquot = row["external_aliquot_id"]
    sample = row["external_sample_id"]
    logger.debug("IDs extracted")
    if not bs_id:
        logger.debug(f"Nonetype found: {bs_id}")
        return None
    if isinstance(bs_id, str):
        logger.debug(f"is a string, returning: {bs_id}")
        return bs_id
    elif isinstance(bs_id, list):
        # hanlde lists with one element
        if len(bs_id) == 1:
            return bs_id[0]
        else:
            logger.debug("list discovered")
            different_aliquots = different_external_ids(aliquot)
            different_samples = different_external_ids(sample)
            if different_aliquots or different_samples:
                logger.debug("returning all samples")
                return bs_id
            else:
                logger.error(f"{bs_id}, aliquot: {aliquot}, sample: {sample}")
    else:
        logger.error("type unknown")
        logger.info(type(x))


file_table = out
file_table = file_table[~file_table["pt_id"].apply(file_id).isna()]


file_table["file_id"] = file_table["gf_id"].apply(
    lambda x: file_id(x, alphabetize=True)
)
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
file_table = merged[merged["_merge"] == "both"].drop(columns=["_merge"])[
    [
        "file_id",
        "file_name",
        "file_type",
        "file_size",
        "md5sum",
        "file_url_in_cds",
    ]
]
file_table.to_csv("data/submission_packet/file.csv", index=False)


# mapping
mapping = out
mapping["file_id"] = mapping["gf_id"].apply(
    lambda x: file_id(x, alphabetize=True)
)
mapping["participant_id"] = mapping["pt_id"].apply(file_id)
# mapping["sample_id"] = mapping["bs_id"].apply(lambda x: file_id(file_id(x)))
mapping["sample_id"] = mapping.apply(sample_id, axis=1)

mapping = mapping[
    [
        "file_id",
        "participant_id",
        "sample_id",
    ]
]
mapping = mapping.explode("sample_id")  # have one row per sample
# clear out files not in the file table
merged = mapping.merge(file_table[["file_id"]], how="outer", indicator=True)
mapping = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
# drop rows of mapping with duplicate samples
duplicate_samples = {
    "BS_2ZNM0FAH": {"replace_with": "BS_6BDC3V0J"},
    "BS_EY78ZWZV": {"replace_with": "BS_AF68VM9V"},
    "BS_H93CJZ2K": {"replace_with": "BS_XF1CNRP4"},
    "BS_KCMBYFD4": {"replace_with": "BS_DQZV6K2W"},
    "BS_YGWZ79FV": {"replace_with": "BS_8X2PEXCW"},
    "BS_5BRT60RP": {"replace_with": "BS_C1X2TNTC"},
    "BS_CEG9P39X": {"replace_with": "BS_J9M42E4M"},
    "BS_TA871TZW": {"replace_with": "BS_VC5PKT9Q"},
    "BS_TF76EFP5": {"replace_with": "BS_VKGSMV3F"},
    "BS_XFZ0Z55A": {"replace_with": "BS_P8AER10J"},
    "BS_XW3AR6HG": {"replace_with": "BS_MCFJEQJ9"},
}
mapping["sample_id"] = mapping["sample_id"].apply(
    lambda x: x
    if x not in duplicate_samples.keys()
    else duplicate_samples.get(x)["replace_with"]
)
mapping.to_csv(
    "data/submission_packet/file_sample_participant_map.csv", index=False
)


# generate the manifest of participants
participant_list = [
    i
    for i in mapping["participant_id"]
    .explode("pt_id")
    .drop_duplicates()
    .to_list()
    if i
]

logger.info("querying for manifest of participants")
participant_manifest = pd.read_sql(
    participant_query(participant_list),
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
    for i in mapping["sample_id"]
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


sample_manifest["sample_type"] = sample_manifest.apply(sample_type, axis=1)
sample_manifest = sample_manifest.drop(columns=["sample_anatomical_site"])

sample_manifest = sample_manifest[
    ~sample_manifest["sample_id"].isin(duplicate_samples.keys())
]
sample_manifest.to_csv("data/submission_packet/sample.csv", index=False)


# Genomic Information

logger.info("querying for manifest of genomic information")
genomic_info_table_raw = pd.read_sql(
    sequencing_query(sample_list),
    conn,
)

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

# remove rows with files not in the file manifest
genomic_info_table = genomic_info_table[
    genomic_info_table["file_id"].isin(file_table["file_id"])
]

genomic_info_table.to_csv(
    "data/submission_packet/genomic_info.csv", index=False
)


conn.close()
# create the dbgap files

breakpoint()
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
