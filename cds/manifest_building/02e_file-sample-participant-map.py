import os
from ast import literal_eval

import pandas as pd
import psycopg2
from d3b_cavatica_tools.utils.logging import get_logger

from utils import *

logger = get_logger(__name__, testing_mode=False)

DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)

# Read in the  files from previous steps
out = pd.read_csv("data/temp/ccdi_manifest.csv")

file_manifest = pd.read_csv("data/submission_packet/file.csv")


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


out["gf_id"] = out["gf_id"].apply(lambda x: literal_eval(x) if "[" in x else x)
out["pt_id"] = out["pt_id"].apply(lambda x: literal_eval(x) if "[" in x else x)
out["bs_id"] = out["bs_id"].apply(lambda x: literal_eval(x) if "[" in x else x)
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
merged = mapping.merge(file_manifest[["file_id"]], how="outer", indicator=True)
mapping = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
mapping.dropna().to_csv(
    "data/submission_packet/file_sample_participant_map.csv", index=False
)