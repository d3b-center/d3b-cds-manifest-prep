import os
import re
import pandas as pd
import psycopg2
from d3b_cavatica_tools.utils.logging import get_logger
from queries import diagnosis_query, diagnosis_sample_query
from utils import *

logger = get_logger(__name__, testing_mode=False)

DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)

participant_manifest = pd.read_csv("data/submission_packet/participant.csv")
sample_manifest = pd.read_csv("data/submission_packet/sample.csv")
other_diagnoses = pd.read_csv("data/other_dx_for_cds_manifest.csv")
dx_over_90 = pd.read_csv("data/cds_qc_greater_than_90_dx_age.csv")
other_sample_diagnoses = pd.read_csv("data/CBTN_dx_pull_2022-11-09.csv")
participant_list = (
    participant_manifest["participant_id"].drop_duplicates().to_list()
)
pbta_histologies = pd.read_csv("data/pbta-histologies.tsv", sep="\t")
sample_list = sample_manifest["sample_id"].drop_duplicates().to_list()

#  get the icd-o ontology
diagnosis_icdo_a = pd.read_excel(
    "data/CBTN - ICD-O.xlsx",
    sheet_name="CBTN",
)[["primary_diagnosis (free text)", "disease_type (ICD-O-3)"]]
diagnosis_icdo_b = pd.read_excel(
    "data/CBTN - ICD-O.xlsx",
    sheet_name="Other",
)[["primary_diagnosis (free text)", "disease_type (ICD-O-3)"]]
diagnosis_icdo = pd.concat(
    [diagnosis_icdo_a, diagnosis_icdo_b]
).drop_duplicates()


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


# def td_handler(
#     row, descriptor_column="tumor_descriptor", participant_col="kf_id"
# ):
#     if row[descriptor_column] == "Initial CNS Tumor":
#         modifier = "initial"
#     elif row[descriptor_column] == "Progressive":
#         modifier = "progressive"
#     elif row[descriptor_column] == "Recurrence":
#         modifier = "recurrence"
#     else:
#         raise AssertionError
#     return row[participant_col] + "_" + modifier


# other_diagnoses["diagnosis_id"] = other_diagnoses.apply(td_handler, axis=1)

# other_diagnoses = other_diagnoses[
#     ["diagnosis_id", "pathology_free_text_diagnosis", "kf_id"]
# ].rename(
#     columns={
#         "pathology_free_text_diagnosis": "primary_diagnosis",
#         "kf_id": "participant_id",
#     }
# )

# dx_over_90["diagnosis_id"] = dx_over_90.apply(
#     lambda row: td_handler(row, "event_type", "participant_id"), axis=1
# )
# dx_over_90 = dx_over_90[["diagnosis_id", "diagnosis", "participant_id"]].rename(
#     columns={"diagnosis": "primary_diagnosis"}
# )

# # handle other sample-diagnoses
# other_dx_samples = (
#     other_sample_diagnoses["CBTN Specimen Group ID"].drop_duplicates().to_list()
# )
# other_sample_types = pd.read_sql(
#     f"""
# select distinct external_sample_id, source_text_tissue_type
# from biospecimen
# where source_text_tissue_type = 'Tumor'
#       and external_sample_id in ({str(other_dx_samples)[1:-1]})
# """,
#     conn,
# )

# other_sample_diagnoses["primary_diagnosis"] = other_sample_diagnoses.apply(
#     lambda row: row["Diagnosis"]
#     if row["Diagnosis"] != "Other"
#     else row["Other Diagnosis Description"],
#     axis=1,
# )
# other_sample_diagnoses = other_sample_diagnoses[
#     other_sample_diagnoses["CBTN Specimen Group ID"].isin(
#         other_sample_types["external_sample_id"].to_list()
#     )
# ]

# other_sample_diagnoses = (
#     other_sample_diagnoses[["KF Participant ID", "primary_diagnosis"]]
#     .drop_duplicates()
#     .rename(columns={"KF Participant ID": "participant_id"})
# )
# other_sample_diagnoses["diagnosis_id"] = (
#     other_sample_diagnoses["participant_id"]
#     + "__"
#     + other_sample_diagnoses["primary_diagnosis"].apply(
#         lambda x: re.sub("[^0-9a-zA-Z]+", "_", x)
#     )
# )

# # gennerate the manifest of diagnoses
# logger.info("querying for diagnoses")

# # diagnoses_manifest = pd.read_sql(diagnosis_query(participant_list), conn)
# diagnoses_manifest = pd.read_sql(diagnosis_sample_query(sample_list), conn)

# # diagnoses_manifest = pd.concat(
# #     [diagnoses_manifest, other_diagnoses, dx_over_90]
# # )
# diagnoses_manifest = pd.concat([diagnoses_manifest, other_sample_diagnoses])


histology_diagnosis = (
    pbta_histologies[
        pbta_histologies["Kids_First_Biospecimen_ID"].isin(sample_list)
    ][
        [
            "Kids_First_Biospecimen_ID",
            "Kids_First_Participant_ID",
            "pathology_diagnosis",
            "broad_histology",
        ]
    ]
    .rename(
        columns={
            "Kids_First_Biospecimen_ID": "diagnosis_id",
            "Kids_First_Participant_ID": "participant_id",
        }
    )
    .dropna()
)
histology_diagnosis["primary_diagnosis"] = histology_diagnosis.apply(
    lambda row: row["broad_histology"]
    if row["pathology_diagnosis"] == "Other"
    else row["pathology_diagnosis"],
    axis=1,
)
histology_diagnosis = histology_diagnosis[
    ["diagnosis_id", "primary_diagnosis", "participant_id"]
]
histology_diagnosis["diagnosis_id"] = (
    "DG__" + histology_diagnosis["diagnosis_id"]
)
histology_diagnosis = (
    histology_diagnosis.join(
        histology_diagnosis["primary_diagnosis"].str.split(";", expand=True)
    )
    .drop(columns="primary_diagnosis")
    .melt(
        id_vars=["diagnosis_id", "participant_id"],
        var_name="dx_number",
        value_name="primary_diagnosis",
    )
    .dropna()
)
histology_diagnosis["diagnosis_id"] = (
    histology_diagnosis["diagnosis_id"]
    + "__"
    + histology_diagnosis["dx_number"].apply(str)
)
histology_diagnosis = histology_diagnosis[
    ["diagnosis_id", "primary_diagnosis", "participant_id"]
]


diagnoses_manifest = histology_diagnosis.merge(
    diagnosis_icdo.rename(
        columns={
            "disease_type (ICD-O-3)": "disease_type",
            "primary_diagnosis (free text)": "primary_diagnosis",
        }
    ),
    how="left",
    on="primary_diagnosis",
).dropna(subset=["primary_diagnosis"])
# diagnoses_manifest[diagnoses_manifest["_merge"] == "left_only"][
#     "primary_diagnosis"
# ].drop_duplicates().sort_values().to_csv("foo.csv", index=False)
# breakpoint()
diagnoses_manifest.to_csv("data/submission_packet/diagnosis.csv", index=False)
