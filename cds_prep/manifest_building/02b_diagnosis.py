import os

import pandas as pd
import psycopg2
from d3b_cavatica_tools.utils.logging import get_logger
from queries import diagnosis_query
from utils import *

logger = get_logger(__name__, testing_mode=False)

DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)

participant_manifest = pd.read_csv("data/submission_packet/participant.csv")
other_diagnoses = pd.read_csv("data/other_dx_for_cds_manifest.csv")
dx_over_90 = pd.read_csv("data/cds_qc_greater_than_90_dx_age.csv")
participant_list = (
    participant_manifest["participant_id"].drop_duplicates().to_list()
)

# get kfids of other diagnoses
other_ids = other_diagnoses["cohort_participant_id"].drop_duplicates().to_list()
other_kfids = pd.read_sql(
    f"""
select kf_id, external_id as cohort_participant_id 
from participant p 
where p.external_id in ({str(other_ids)[1:-1]}) 
      and p.kf_id in ({str(participant_list)[1:-1]})""",
    conn,
)
other_diagnoses = other_diagnoses.merge(other_kfids, how="left").sort_values(
    ["cohort_participant_id"]
)


def td_handler(
    row, descriptor_column="tumor_descriptor", participant_col="kf_id"
):
    if row[descriptor_column] == "Initial CNS Tumor":
        modifier = "initial"
    elif row[descriptor_column] == "Progressive":
        modifier = "progressive"
    elif row[descriptor_column] == "Recurrence":
        modifier = "recurrence"
    else:
        raise AssertionError
    return row[participant_col] + "_" + modifier


other_diagnoses["diagnosis_id"] = other_diagnoses.apply(td_handler, axis=1)

other_diagnoses = other_diagnoses[
    ["diagnosis_id", "pathology_free_text_diagnosis", "kf_id"]
].rename(
    columns={
        "pathology_free_text_diagnosis": "primary_diagnosis",
        "kf_id": "participant_id",
    }
)

dx_over_90["diagnosis_id"] = dx_over_90.apply(
    lambda row: td_handler(row, "event_type", "participant_id"), axis=1
)
dx_over_90 = dx_over_90[["diagnosis_id", "diagnosis", "participant_id"]].rename(
    columns={"diagnosis": "primary_diagnosis"}
)

# gennerate the manifest of diagnoses
logger.info("querying for diagnoses")
diagnosis_icdo_a = pd.read_excel(
    "/home/ubuntu/d3b-cds-manifest-prep/data/CBTN - ICD-O(3).xlsx",
    sheet_name="CBTN",
)[["primary_diagnosis (free text)", "disease_type (ICD-O-3)"]]
diagnosis_icdo_b = pd.read_excel(
    "/home/ubuntu/d3b-cds-manifest-prep/data/CBTN - ICD-O(3).xlsx",
    sheet_name="Other",
)[["primary_diagnosis (free text)", "disease_type (ICD-O-3)"]]
diagnosis_icdo = pd.concat(
    [diagnosis_icdo_a, diagnosis_icdo_b]
).drop_duplicates()
diagnoses_manifest = pd.read_sql(diagnosis_query(participant_list), conn)
diagnoses_manifest = pd.concat(
    [diagnoses_manifest, other_diagnoses, dx_over_90]
)

diagnoses_manifest = diagnoses_manifest.merge(
    diagnosis_icdo.rename(
        columns={
            "disease_type (ICD-O-3)": "disease_type",
            "primary_diagnosis (free text)": "primary_diagnosis",
        }
    ),
    how="left",
    on="primary_diagnosis",
    # indicator=True,
).dropna(subset=["primary_diagnosis"])

diagnoses_manifest.to_csv("data/submission_packet/diagnosis.csv", index=False)
