from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import diagnosis_query

import pandas as pd
import pkg_resources
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def load_ontology_mapping():
    logger.debug("loading ontology mapping")
    fname = pkg_resources.resource_filename("cds", "data/CBTN - ICD-O.xlsx")
    diagnosis_icdo_a = pd.read_excel(fname, sheet_name="CBTN")[
        ["primary_diagnosis (free text)", "disease_type (ICD-O-3)"]
    ]
    diagnosis_icdo_b = pd.read_excel(fname, sheet_name="Other")[
        ["primary_diagnosis (free text)", "disease_type (ICD-O-3)"]
    ]
    diagnosis_icdo = (
        pd.concat([diagnosis_icdo_a, diagnosis_icdo_b])
        .drop_duplicates()
        .rename(
            columns={
                "disease_type (ICD-O-3)": "disease_type",
                "primary_diagnosis (free text)": "primary_diagnosis",
            }
        )
    )
    return diagnosis_icdo


def load_histologies():
    logger.debug("loading histology file")
    fname = pkg_resources.resource_filename(
        "cds", "data/openpedcan_histologies.csv"
    )
    return pd.read_csv(fname)


def find_missing_diagnoses(participants_missing_diagnoses, fsp, conn):
    missing_samples = (
        fsp[fsp["participant_id"].isin(participants_missing_diagnoses)][
            "sample_id"
        ]
        .drop_duplicates()
        .to_list()
    )
    missing_diagnoses = pd.read_sql(
        diagnosis_query(missing_samples, "Tumor"), conn
    )
    # check that all participants have diagnoses
    missing_participants = [
        p
        for p in participants_missing_diagnoses
        if p not in missing_diagnoses["participant_id"].to_list()
    ]
    if len(missing_participants) > 0:
        missing_samples = (
            fsp[fsp["participant_id"].isin(missing_participants)]["sample_id"]
            .drop_duplicates()
            .to_list()
        )
        more_diagnoses = pd.read_sql(diagnosis_query(missing_samples), conn)
        missing_diagnoses = pd.concat([missing_diagnoses, more_diagnoses])
    return missing_diagnoses


def build_diagnosis_table(
    db_url,
    sample_list,
    submission_package_dir,
    generate_sample_diagnosis_map=True,
    fsp=False,
):
    logger.info("Building diagnosis table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    # load the ontology mapping
    ontology = load_ontology_mapping()
    histologies = load_histologies()
    histology_diagnosis = (
        histologies[histologies["Kids_First_Biospecimen_ID"].isin(sample_list)][
            [
                "Kids_First_Biospecimen_ID",
                "Kids_First_Participant_ID",
                "pathology_diagnosis",
                "broad_histology",
            ]
        ]
        .rename(
            columns={
                "Kids_First_Biospecimen_ID": "sample_id",
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
        ["sample_id", "primary_diagnosis", "participant_id"]
    ]
    # Hanlde any participants missing a diagnosis
    missing_participants = [
        p
        for p in fsp["participant_id"].drop_duplicates().to_list()
        if p not in histology_diagnosis["participant_id"].to_list()
    ]
    missing_diagnoses = find_missing_diagnoses(missing_participants, fsp, conn)
    # breakpoint()
    combined_diagnoses = pd.concat([histology_diagnosis, missing_diagnoses])
    combined_diagnoses["diagnosis_id"] = (
        "DG__" + combined_diagnoses["sample_id"]
    )
    combined_diagnoses = (
        combined_diagnoses.join(
            combined_diagnoses["primary_diagnosis"].str.split(";", expand=True)
        )
        .drop(columns="primary_diagnosis")
        .melt(
            id_vars=["diagnosis_id", "participant_id", "sample_id"],
            var_name="dx_number",
            value_name="primary_diagnosis",
        )
        .dropna()
    )
    combined_diagnoses["diagnosis_id"] = (
        combined_diagnoses["diagnosis_id"]
        + "__"
        + combined_diagnoses["dx_number"].apply(str)
    )
    if generate_sample_diagnosis_map:
        logger.info("generating sample-diagnosis mapping.")
        mapping = combined_diagnoses[["diagnosis_id", "sample_id"]]
        mapping.to_csv(
            f"{submission_package_dir}diagnosis_sample_mapping.csv", index=False
        )

    combined_diagnoses = combined_diagnoses[
        ["diagnosis_id", "primary_diagnosis", "participant_id"]
    ]

    diagnoses_manifest = combined_diagnoses.merge(
        ontology,
        how="left",
        on="primary_diagnosis",
    ).dropna(subset=["primary_diagnosis"])
    diagnoses_manifest.to_csv(
        f"{submission_package_dir}diagnosis.csv", index=False
    )
