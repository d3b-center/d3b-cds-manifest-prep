import pandas as pd
import pkg_resources
import psycopg2


def sample_diagnosis_query(sample_list):
    query = f"""
    select pt.study_id,
           bs.kf_id as sample_id,
           composition as sample_type,
           bs.participant_id as participant_id,
           source_text_tissue_type as sample_tumor_status,
           source_text_anatomical_site as anatomic_site,
           age_at_event_days as participant_age_at_collection,
           bs.external_aliquot_id as alternate_sample_id
    from biospecimen bs
    join participant pt on pt.kf_id = bs.participant_id
    where bs.kf_id in ({str(sample_list)[1:-1]})
    """
    return query


def load_histologies(output_table):
    """Load the histologies data

    :return: histologies data
    :rtype: pandas.DataFrame
    """
    output_table.logger.debug("loading histology file")
    fname = pkg_resources.resource_filename(
        "cds", "data/openpedcan_histologies.csv"
    )
    return pd.read_csv(fname)


def load_ontology_mapping(output_table):
    """load the ontology mapping

    :return: mapping between diagnosis and icd-o code
    :rtype: pandas.DataFrame
    """
    output_table.logger.debug("loading ontology mapping")
    fname = pkg_resources.resource_filename("cds", "data/diagnosis_icd-o.csv")
    mapping = pd.read_csv(fname)
    mapping["diagnosis_icd_o"] = mapping["diagnosis_icd_o"].str.strip()
    return mapping


def extract_diagnosis(row):
    dx = row["pathology_diagnosis"]
    if dx == "Other":
        dx = row["pathology_free_text_diagnosis"]
    return dx


disease_phase_map = {
    "Initial CNS Tumor": "Initial Diagnosis",
    "Recurrence": "Recurrent Disease",
    "Progressive": "Disease Progression",
    "Second Malignancy": "Initial Diagnosis",
    "Deceased": "Post-treatment",
    "Progressive Disease Post-Mortem": "Disease Progression",
}


def build_sample_diagnosis_table(output_table, sample_list, icd_o_dictionary):
    """Build the sample table

    Build the sample manifest

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param sample_list: sample IDs to have in the sample manifest
    :type sample_list: list
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :return: sample table
    :rtype: pandas.DataFrame
    """
    output_table.logger.info("Building sample table")
    output_table.logger.debug("loading histology file")
    histologies = load_histologies(output_table)
    ontology = load_ontology_mapping(output_table)
    sample_histologies = histologies[
        (histologies["Kids_First_Biospecimen_ID"].isin(sample_list))
        & (histologies["sample_type"] == "Tumor")
    ][
        [
            "Kids_First_Biospecimen_ID",
            "tumor_descriptor",
            "pathology_diagnosis",
            "pathology_free_text_diagnosis",
        ]
    ]
    sample_histologies["primary_diagnosis"] = sample_histologies.apply(
        extract_diagnosis, axis=1
    )
    diagnosis_table = sample_histologies[
        [
            "Kids_First_Biospecimen_ID",
            "tumor_descriptor",
            "primary_diagnosis",
        ]
    ]
    diagnosis_table = (
        diagnosis_table.join(
            diagnosis_table["primary_diagnosis"].str.split(";", expand=True)
        )
        .drop(columns="primary_diagnosis")
        .melt(
            id_vars=["Kids_First_Biospecimen_ID", "tumor_descriptor"],
            var_name="dx_number",
            value_name="primary_diagnosis",
        )
        .dropna(subset=["primary_diagnosis"])
        .drop_duplicates()
    )
    sample_diagnosis = diagnosis_table.merge(
        ontology, how="left", on="primary_diagnosis"
    )
    icd_o_dictionary["diagnosis_icd_o"] = icd_o_dictionary["Term"].apply(
        lambda x: x.partition(":")[0].strip()
    )
    icd_o_dictionary = icd_o_dictionary[["diagnosis_icd_o", "Term"]]
    sample_diagnosis = sample_diagnosis.merge(
        icd_o_dictionary, on="diagnosis_icd_o", how="left"
    )
    # report the diagnosis that are not mapped to a term
    unmatched = sample_diagnosis[sample_diagnosis["Term"].isna()]
    if len(unmatched) > 0:
        output_table.logger.warning(
            "diagnosis found that do not have approved term. "
            "See diagnoses_with_no_term.csv"
        )
        unmatched.to_csv("diagnoses_with_no_term.csv", index=False)
        output_table.logger.warning("Removing Diagnoses with no term")
        sample_diagnosis = sample_diagnosis[~sample_diagnosis["Term"].isna()]

    sample_diagnosis["sample.sample_id"] = sample_diagnosis[
        "Kids_First_Biospecimen_ID"
    ]
    sample_diagnosis["sample_diagnosis_id"] = (
        "DX-"
        + sample_diagnosis["Kids_First_Biospecimen_ID"]
        + "-"
        + sample_diagnosis["primary_diagnosis"]
    )
    sample_diagnosis["diagnosis_icd_o"] = sample_diagnosis["Term"]
    sample_diagnosis["disease_phase"] = sample_diagnosis[
        "tumor_descriptor"
    ].apply(lambda x: disease_phase_map.get(x))
    sample_diagnosis["tumor_classification"] = "Primary"
    sample_diagnosis["type"] = output_table.name
    return sample_diagnosis
