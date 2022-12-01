from d3b_cavatica_tools.utils.logging import get_logger

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


def load_pbta_histologies():
    logger.debug("loading histology file")
    fname = pkg_resources.resource_filename("cds", "data/pbta-histologies.tsv")
    return pd.read_csv(fname, sep="\t")


def build_diagnosis_table(
    db_url,
    sample_list,
    submission_package_dir,
    generate_sample_diagnosis_map=True,
):
    logger.info("Building diagnosis table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    # load the ontology mapping
    ontology = load_ontology_mapping()
    histologies = load_pbta_histologies()

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
    histology_diagnosis["diagnosis_id"] = (
        "DG__" + histology_diagnosis["sample_id"]
    )
    histology_diagnosis = (
        histology_diagnosis.join(
            histology_diagnosis["primary_diagnosis"].str.split(";", expand=True)
        )
        .drop(columns="primary_diagnosis")
        .melt(
            id_vars=["diagnosis_id", "participant_id", "sample_id"],
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
    if generate_sample_diagnosis_map:
        logger.info("generating sample-diagnosis mapping.")
        mapping = histology_diagnosis[["diagnosis_id", "sample_id"]]
        mapping.to_csv(
            f"{submission_package_dir}diagnosis_sample_mapping.csv", index=False
        )

    histology_diagnosis = histology_diagnosis[
        ["diagnosis_id", "primary_diagnosis", "participant_id"]
    ]

    diagnoses_manifest = histology_diagnosis.merge(
        ontology,
        how="left",
        on="primary_diagnosis",
    ).dropna(subset=["primary_diagnosis"])
    diagnoses_manifest.to_csv(
        f"{submission_package_dir}diagnosis.csv", index=False
    )
