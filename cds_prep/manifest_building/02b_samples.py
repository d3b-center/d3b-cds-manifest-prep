import os
from ast import literal_eval

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


# Generate the manifest of samples

sample_list = [
    i
    for i in out["bs_id"]
    .apply(lambda x: literal_eval(x) if "[" in x else x)
    .explode()
    .explode()
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


# map tumor status to anatomical site
anatomical_site_map = {
    "tumor": "Central Nervous System",
    "normal": "Not Reported",
}

sample_manifest["sample_anatomical_site"] = sample_manifest[
    "sample_tumor_status"
].apply(lambda x: anatomical_site_map.get(x))
sample_manifest["sample_type"] = sample_manifest.apply(sample_type, axis=1)

sample_manifest.to_csv("data/submission_packet/sample.csv", index=False)
