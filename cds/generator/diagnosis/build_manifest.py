from itertools import product

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


def dx_from_histology(sample_list, histologies, fsp):
    """Extract diagnosis from the histologies

    Extracts diagnosis by

    1.  the histology is queried by participant_id.
    2.  extract diagnosis from the pathology_diagnosis column. If `Other` use
        the value in broad_histology.
    3.  For every participant, remove any rows containing no diagnosis. If this
        cleaning removes all diagnoses for a participant, associate all of that
        participant's samples with all diagnoses for that participant,
        regardless of wether those diagnoses are related to samples in the
        sample list

    :param sample_list: list of samples to get diagnoses for
    :type sample_list: list
    :param histologies: histologies table
    :type histologies: pandas.DataFrame
    :param fsp: mapping between file, sample and participant
    :type fsp: pandas.DataFrame
    :return: diagnoses extracted from the histology table
    rtype: pandas.DataFrame
    """
    participant_list = fsp["participant_id"].drop_duplicates().to_list()
    participant_diagnosis = histologies[
        histologies["Kids_First_Participant_ID"].isin(participant_list)
    ][
        [
            "Kids_First_Biospecimen_ID",
            "Kids_First_Participant_ID",
            "pathology_diagnosis",
            "broad_histology",
        ]
    ].rename(
        columns={
            "Kids_First_Biospecimen_ID": "sample_id",
            "Kids_First_Participant_ID": "participant_id",
        }
    )
    # extract the diagnosis
    participant_diagnosis["primary_diagnosis"] = participant_diagnosis.apply(
        lambda row: row["broad_histology"]
        if row["pathology_diagnosis"] == "Other"
        else row["pathology_diagnosis"],
        axis=1,
    )
    participant_diagnosis = participant_diagnosis.drop(
        columns=["broad_histology", "pathology_diagnosis"]
    )
    # build table of diagnoses related to samples in sample list
    sample_diagnosis = participant_diagnosis[
        participant_diagnosis["sample_id"].isin(sample_list)
    ]
    # Clean the diagnoses.
    dx_table_list = []
    logger.debug("cleaning diagnoses per participants")
    for participant in participant_list:
        # extract tows where there is a diagnosis. Normal samples don't have a
        # diagnosis.
        dx_table = sample_diagnosis[
            sample_diagnosis["participant_id"] == participant
        ]
        dropped_na = dx_table.dropna()
        if len(dropped_na) == 0:
            # handle if there is no diagnosis. use the diagnosis/es associated
            # to the participant and associate those diagnoses with samples
            # for the participant in question
            participant_dx = participant_diagnosis[
                participant_diagnosis["participant_id"] == participant
            ]
            diagnosis_list = (
                participant_dx["primary_diagnosis"]
                .dropna()
                .drop_duplicates()
                .to_list()
            )
            if len(diagnosis_list) > 0:
                participant_samples = (
                    fsp[fsp["participant_id"] == participant]["sample_id"]
                    .drop_duplicates()
                    .to_list()
                )
                cleaned_dx = pd.DataFrame(
                    product(participant_samples, [participant], diagnosis_list),
                    columns=[
                        "sample_id",
                        "participant_id",
                        "primary_diagnosis",
                    ],
                )
            else:
                logger.warning(f"No diagnoses found for {participant}")
        else:
            cleaned_dx = dropped_na
        dx_table_list.append(cleaned_dx)
    diagnosis_df = pd.concat(dx_table_list)
    return diagnosis_df


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
    histology_diagnosis = dx_from_histology(sample_list, histologies, fsp)
    # Hanlde any participants missing a diagnosis
    missing_participants = [
        p
        for p in fsp["participant_id"].drop_duplicates().to_list()
        if p not in histology_diagnosis["participant_id"].to_list()
    ]
    missing_diagnoses = find_missing_diagnoses(missing_participants, fsp, conn)
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
