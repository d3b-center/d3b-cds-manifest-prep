from itertools import product

from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import diagnosis_query

import pandas as pd
import pkg_resources
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def load_ontology_mapping():
    """load the ontology mapping

    :return: mapping between diagnosis and icd-o code
    :rtype: pandas.DataFrame
    """
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
    """Load the histologies data

    :return: histologies data
    :rtype: pandas.DataFrame
    """
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
        the value in broad_histology. If broad_histology is NA, use
        pathology_free_text_diagnosis.
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

    def extract_diagnosis(row):
        dx = row["pathology_diagnosis"]
        if dx == "Other":
            dx = row["broad_histology"]
            if pd.isna(dx):
                dx = row["pathology_free_text_diagnosis"]
        return dx

    participant_list = fsp["participant_id"].drop_duplicates().to_list()
    participant_diagnosis = histologies[
        histologies["Kids_First_Participant_ID"].isin(participant_list)
    ][
        [
            "Kids_First_Biospecimen_ID",
            "Kids_First_Participant_ID",
            "sample_id",
            "pathology_diagnosis",
            "broad_histology",
            "pathology_free_text_diagnosis",
        ]
    ].rename(
        columns={
            "sample_id": "event_id",
            "Kids_First_Biospecimen_ID": "sample_id",
            "Kids_First_Participant_ID": "participant_id",
        }
    )
    # extract the diagnosis
    participant_diagnosis["primary_diagnosis"] = participant_diagnosis.apply(
        extract_diagnosis,
        axis=1,
    )
    participant_diagnosis = participant_diagnosis.drop(
        columns=[
            "broad_histology",
            "pathology_diagnosis",
            "pathology_free_text_diagnosis",
        ]
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
                participant_dx[["event_id", "primary_diagnosis"]]
                .dropna()
                .drop_duplicates()
            )
            if len(diagnosis_list) > 0:
                participant_samples = (
                    fsp[fsp["participant_id"] == participant]["sample_id"]
                    .drop_duplicates()
                    .to_list()
                )
                sample_events = participant_dx[
                    participant_dx["sample_id"].isin(participant_samples)
                ][["sample_id", "event_id"]].drop_duplicates()
                cleaned_dx = sample_events.merge(diagnosis_list, how="left")
                cleaned_dx["participant_id"] = participant
                cleaned_dx = cleaned_dx[
                    [
                        "sample_id",
                        "participant_id",
                        "event_id",
                        "primary_diagnosis",
                    ]
                ]
            else:
                logger.warning(f"No diagnoses found for {participant}")
        else:
            cleaned_dx = dropped_na
        dx_table_list.append(cleaned_dx)
    diagnosis_df = pd.concat(dx_table_list)
    return diagnosis_df


def find_missing_diagnoses(participants_missing_diagnoses, fsp, conn):
    """discover missing diagnoses

    discover missing diagnoses if a diagnosis can't be found in the histologies
    file.

    :param participants_missing_diagnoses: participant ids of participants
    missing diagnoses
    :type participants_missing_diagnoses: list
    :param fsp: mapping between file sample and participants
    :type fsp: pandas.DataFrame
    :param conn: database connection object to kf prd postgres
    :type conn: psycopg2.connection
    :return: diagnosis mapping for specified participants
    :rtype: pandas.DataFrame
    """
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


def order_columns(manifest):
    """order columns in the manifest

    :param manifest: The manifest to order columns for
    :type manifest: pandas.DataFrame
    :return: The manifest with columns needed in the correct order
    :rtype: pandas.DataFrame
    """
    columns = [
        "diagnosis_id",
        "primary_diagnosis",
        "participant_id",
        "disease_type",
    ]
    return manifest[columns]


def generate_diagnosis_id(diagnosis_table):
    """generate unique diagnosis ids

    generate the diagnosis manifest with diagnosis IDS that are unique

    :param diagnosis_table: diagnosis table without ids. Requires columns
    `sample_id`, and `primary_diagnosis`.
    :type diagnosis_table: pandas.DataFrame
    :return: diagnosis manifest with added column `diagnosis_id` with diagnoses
    that are unique.
    :rtype: pandas.DataFrame
    """
    diagnosis_table["diagnosis_id"] = "DG__" + diagnosis_table["event_id"]
    grouped_dx = diagnosis_table.sort_values(
        ["diagnosis_id", "primary_diagnosis"]
    ).groupby("diagnosis_id", group_keys=False)

    def grouped_dx_id(grp):
        diagnoses = grp[["primary_diagnosis"]].drop_duplicates()
        diagnoses["dx_number"] = range(len(diagnoses))
        grp_ids = grp.drop(columns="dx_number").merge(diagnoses)
        return grp_ids

    dx_with_ids = grouped_dx.apply(grouped_dx_id)
    dx_with_ids["diagnosis_id"] = (
        dx_with_ids["diagnosis_id"] + "__" + dx_with_ids["dx_number"].apply(str)
    )
    return dx_with_ids


def build_diagnosis_table(
    db_url,
    sample_list,
    submission_package_dir,
    generate_sample_diagnosis_map=True,
    fsp=False,
    find_missing_dx_in_dataservice=False,
):
    """Build the diagnosis manifest

    Build the diagnosis manifest. Map participants and specimens to their
    respective diagnoses.

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param sample_list: samples to get diagnoses for
    :type sample_list: list
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :param generate_sample_diagnosis_map: generate a mapping between sample and
    diagnosis, defaults to True
    :type generate_sample_diagnosis_map: bool, optional
    :param find_missing_dx_in_dataservice: for participants not found in the
    histology table, should diagnosis be searched for in the dataservice,
    defaults to False
    :type find_missing_dx_in_dataservice: bool, optional
    :param fsp: mapping between file sample and participant, defaults to False
    :type fsp: pd.DataFrame or bool, optional
    :return: Diagnosis table and *if generate_sample_diagnosis_map is True*,
    diagnosis-sample mapping table, otherwise None
    :rtype: tuple
    """
    logger.info("Building diagnosis table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    # load the ontology mapping
    ontology = load_ontology_mapping()
    histologies = load_histologies()
    histology_diagnosis = dx_from_histology(sample_list, histologies, fsp)
    # Hanlde any participants missing a diagnosis
    if find_missing_dx_in_dataservice:
        missing_participants = [
            p
            for p in fsp["participant_id"].drop_duplicates().to_list()
            if p not in histology_diagnosis["participant_id"].to_list()
        ]
        missing_diagnoses = find_missing_diagnoses(
            missing_participants, fsp, conn
        )
        combined_diagnoses = pd.concat([histology_diagnosis, missing_diagnoses])
        diagnosis_table = combined_diagnoses
    else:
        diagnosis_table = histology_diagnosis

    diagnosis_table.reset_index(
        drop=True, inplace=True
    )  # some diagnoses may share indices

    # split diagnoses on `;` because some are `;` delimited, then generate
    # diagnosis ids
    diagnosis_table = (
        diagnosis_table.join(
            diagnosis_table["primary_diagnosis"].str.split(";", expand=True)
        )
        .drop(columns="primary_diagnosis")
        .melt(
            id_vars=["participant_id", "sample_id", "event_id"],
            var_name="dx_number",
            value_name="primary_diagnosis",
        )
        .dropna()
        .drop_duplicates()
    )
    diagnosis_table = generate_diagnosis_id(diagnosis_table)
    if generate_sample_diagnosis_map:
        logger.info("generating sample-diagnosis mapping.")
        # Set the column order and sort on key column
        mapping = diagnosis_table[["diagnosis_id", "sample_id"]].sort_values(
            ["diagnosis_id", "sample_id"]
        )
        mapping.to_csv(
            f"{submission_package_dir}diagnosis_sample_mapping.csv", index=False
        )

    diagnosis_table = diagnosis_table[
        ["diagnosis_id", "primary_diagnosis", "participant_id"]
    ]

    diagnoses_manifest = diagnosis_table.merge(
        ontology,
        how="left",
        on="primary_diagnosis",
    ).dropna(subset=["primary_diagnosis"])
    # Set the column order and sort on key column
    diagnoses_manifest = (
        order_columns(diagnoses_manifest)
        .sort_values("diagnosis_id")
        .drop_duplicates()
    )
    diagnoses_manifest.to_csv(
        f"{submission_package_dir}diagnosis.csv", index=False
    )

    if generate_sample_diagnosis_map:
        return (diagnoses_manifest, mapping)
    else:
        return (diagnoses_manifest, None)
