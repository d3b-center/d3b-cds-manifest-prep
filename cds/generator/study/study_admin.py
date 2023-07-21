import pandas as pd
import psycopg2


def study_query(participant_list):
    query = f"""
    select distinct
        pt.study_id
    from participant pt
    where pt.kf_id in ({str(participant_list)[1:-1]})
    """
    return query


import pandas as pd
import psycopg2


def study_query(participant_list):
    query = f"""
    select distinct
        pt.study_id
    from participant pt
    where pt.kf_id in ({str(participant_list)[1:-1]})
    """
    return query


def build_study_admin_table(output_table, db_url, participant_list):
    """Build the study table

    Build the study manifest

    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :param submission_template_dict: template submission manifests
    :type submission_template_dict: dict
    :return: study table
    :return: study table
    :rtype: OutputTable
    """
    output_table.logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    output_table.logger.info("Querying for manifest of studies")
    study_admin_table = pd.read_sql(study_query(participant_list), conn)
    conn.close()
    output_table.logger.info("Converting KF enums to CDS enums")
    study_admin_table["study_admin_id"] = study_admin_table["study_id"]
    study_admin_table["organism_species"] = "Human"
    study_admin_table["adult_or_childhood_study"] = "Pediatric"
    study_admin_table["type"] = output_table.name
    # rename the study ID column
    study_admin_table = study_admin_table.rename(
        columns={"study_id": "study.study_id"}
    )
    return study_admin_table
