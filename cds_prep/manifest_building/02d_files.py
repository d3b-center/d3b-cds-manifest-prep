import os
from ast import literal_eval

import pandas as pd
import psycopg2

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


# breakpoint()
file_table["gf_id"] = file_table["gf_id"].apply(
    lambda x: literal_eval(x) if "[" in x else x
)
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
