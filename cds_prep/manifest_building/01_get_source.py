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


logger.info("Loading local files")
# Load in data from open Data Projects
cbtn_open_data = pd.read_csv(
    "/home/ubuntu/mount/public_files_cbtn.txt", sep="\t"
)[["bs_id", "File Name", "File location"]].rename(
    columns={
        "bs_id": "bs_id_cavatica",
        "File Name": "file_name_cavatica",
        "File location": "file_path_cavatica",
    }
)
cbtn_open_data["open_access"] = True

pnoc003_open_data = pd.read_csv(
    "/home/ubuntu/mount/public_files_pnoc003.txt", sep="\t"
)[["bs_id", "File Name", "File location"]].rename(
    columns={
        "bs_id": "bs_id_cavatica",
        "File Name": "file_name_cavatica",
        "File location": "file_path_cavatica",
    }
)

pnoc008_open_data = pd.read_csv(
    "/home/ubuntu/mount/public_files_pnoc008.txt", sep="\t"
)[["bs_id", "File Name", "File location"]].rename(
    columns={
        "bs_id": "bs_id_cavatica",
        "File Name": "file_name_cavatica",
        "File location": "file_path_cavatica",
    }
)

pnoc003_harmonized_data = pd.read_csv(
    "/home/ubuntu/mount/PNOC003-datastes_harmonized_manifest.txt", sep="\t"
)[["BS_ID", "File Name", "File location"]].rename(
    columns={
        "BS_ID": "bs_id_cavatica",
        "File Name": "file_name_cavatica",
        "File location": "file_path_cavatica",
    }
)

pnoc008_harmonized_data = pd.read_csv(
    "/home/ubuntu/mount/PNOC008-datastes_harmonized_manifest.txt", sep="\t"
)[["BS_ID", "File Name", "File location"]].rename(
    columns={
        "BS_ID": "bs_id_cavatica",
        "File Name": "file_name_cavatica",
        "File location": "file_path_cavatica",
    }
)

pnoc_open_data = pd.concat([pnoc003_open_data, pnoc008_open_data])
pnoc_open_data["open_access"] = True
pnoc_harmonized_data = pd.concat(
    [pnoc003_harmonized_data, pnoc008_harmonized_data]
)
pnoc_all_data = pd.merge(
    pnoc_open_data,
    pnoc_harmonized_data,
    how="outer",
    on=["bs_id_cavatica", "file_name_cavatica", "file_path_cavatica"],
)

conn = psycopg2.connect(DB_URL)

logger.info("querying for manifest of files from pnoc")
pnoc_manifest = pd.read_sql(
    pnoc_sql,
    conn,
)
logger.info("manifest retrieved")
logger.info("querying for manifest of files already transferred from cbtn")
already_transferred_manifest = pd.read_sql(
    already_transferred_sql,
    conn,
)
logger.info("manifest retrieved")
logger.info("querying for manifest of s3scrapes")
s3scrapes = pd.read_sql(
    all_scrapes_sql,
    conn,
)
logger.info("manifest retrieved")
# Fix the path so that it references the source, not the target bucket
cbtn_manifest = already_transferred_manifest[
    already_transferred_manifest["s3path"].str.startswith(
        "s3://cds-246-phs002517-sequencefiles-p30-fy20/"
        "kf-study-us-east-1-prd-sd-bhjxbdqk"
    )
]

cbtn_manifest["s3path"] = cbtn_manifest["s3path"].apply(
    lambda x: x.replace("cds-246-phs002517-sequencefiles-p30-fy20/", "")
)

conn.close()

logger.info("getting unique values for each file from cbtn")
unique_cbtn = build_unique_file_manifest(cbtn_manifest)
unique_cbtn["name"] = unique_cbtn["s3path"].apply(
    lambda x: x.rpartition("/")[2]
)
merged_cbtn = unique_cbtn.merge(
    cbtn_open_data,
    how="left",
    left_on="name",
    right_on="file_name_cavatica",
)
merged_cbtn = merged_cbtn[~merged_cbtn["s3path"].str.contains("failed after")]
merged_cbtn = curate_columns(merged_cbtn)
merged_cbtn["file_format"] = merged_cbtn.apply(
    lambda row: row["file_format"]
    if row["file_format"]
    else file_format(row["s3path"]),
    axis=1,
)
merged_cbtn["data_type"] = merged_cbtn.apply(
    lambda row: row["data_type"]
    if row["data_type"]
    else data_type(row["s3path"]),
    axis=1,
)
merged_cbtn = merged_cbtn.drop(
    columns=[
        "bs_id_cavatica",
        "file_name_cavatica",
        "file_path_cavatica",
    ]
)
logger.info("saving manifest of cbtn data")
merged_cbtn.to_csv("data/ccdi_manifest_cbtn-with_kfids.csv", index=False)

# * PNOC Section
logger.info("getting unique values for each file from pnoc")
# logger.info("getting unique values for each file from pnoc")
unique_pnoc = build_unique_file_manifest(pnoc_manifest)
unique_pnoc["name"] = unique_pnoc["s3path"].apply(
    lambda x: x.rpartition("/")[2]
)
# ! Set aside unregistered files/ files with information we don't have
logger.info("saving manifest of unregistered pnoc files.")
unregistered = unique_pnoc[unique_pnoc["bs_id"].isna()]
unregistered.to_csv("data/unregistered_pnoc_files.csv", index=False)
logger.info("dropping unregistered pnoc files")
unique_pnoc = unique_pnoc[~unique_pnoc["bs_id"].isna()]

merged_pnoc = unique_pnoc.merge(
    pnoc_all_data,
    how="left",
    left_on="name",
    right_on="file_name_cavatica",
)
merged_pnoc = curate_columns(merged_pnoc)
merged_pnoc["file_format"] = merged_pnoc.apply(
    lambda row: row["file_format"]
    if row["file_format"]
    else file_format(row["s3path"]),
    axis=1,
)
merged_pnoc["data_type"] = merged_pnoc.apply(
    lambda row: row["data_type"]
    if row["data_type"]
    else data_type(row["s3path"]),
    axis=1,
)
merged_pnoc2 = merged_pnoc.drop(
    columns=[
        "bs_id_cavatica",
        "file_name_cavatica",
        "file_path_cavatica",
    ]
)
logger.info("Saving manifest of pnoc data")
merged_pnoc.to_csv("ccdi_manifest_pnoc-with_kfids.csv", index=False)


# * Create the manifest for CCDI

# merge pnoc and cbtn
cbtn_and_pnoc = pd.concat([merged_cbtn, merged_pnoc])

# Convert the manifest into the form needed for ccdi
out = cbtn_and_pnoc.rename(
    columns={"s3path": "file_path", "md5": "hash"}
).merge(s3scrapes, how="left", left_on="file_path", right_on="s3path")
file_cols = [
    "file_path",
    "is_harmonized",
    "file_format",
    "data_type",
    "controlled_access",
    "reference_genome",
    "external_participant_id",
    "external_sample_id",
    "size",
    "hash",
]
out["file_format"] = out["file_format"].apply(
    lambda x: file_format_condense_map.get(str(x))
)
out["file_format"] = out.apply(
    lambda row: row["file_format"]
    if row["file_format"] != "Not Reported"
    else FILE_EXT_FORMAT_MAP.get(file_ext(row["file_path"])),
    axis=1,
)
out["data_type"] = out["data_type"].apply(
    lambda x: data_type_condense_map.get(str(x)) or x
)
out["data_type"] = out.apply(
    lambda row: row["data_type"]
    if row["data_type"] != "Not Reported"
    else data_type(row["file_path"]),
    axis=1,
)
out["file_path"] = out["file_path"].apply(lambda x: x.replace("s3://", ""))
out["reference_genome"] = out["reference_genome"].apply(
    lambda x: x or COMMON.NOT_REPORTED
)
out["hash_type"] = "md5"

out["external_sample_id"] = out["external_sample_id"].apply(
    lambda x: str(x).replace("'", "")[1:-1]
)
out["external_participant_id"] = out["external_participant_id"].apply(
    lambda x: str(x).replace("'", "")[1:-1]
)
out = out.dropna(subset=file_cols)

# Remove chris jones and steven keating

jones_and_keating = out[
    (
        (out["external_participant_id"] == "steven_keating")
        | (
            out["file_path"].str.contains(
                "kf-study-us-east-1-prd-sd-bhjxbdqk/source/chrisjones/"
            )
        )
    )
]

out = out[
    ~(
        (out["external_participant_id"] == "steven_keating")
        | (
            out["file_path"].str.contains(
                "kf-study-us-east-1-prd-sd-bhjxbdqk/source/chrisjones/"
            )
        )
    )
]

out.to_csv("data/temp/ccdi_manifest.csv", index=False)
pnoc_manifest.to_csv("data/temp/pnoc_manifest.csv", index=False)
merged_cbtn.to_csv("data/temp/ccdi_manifest_cbtn-with_kfids.csv", index=False)
s3scrapes.to_csv("data/temp/s3scrapes.csv", index=False)
