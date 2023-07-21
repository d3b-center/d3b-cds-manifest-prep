import pandas as pd
import psycopg2


def study_query(participant_list):
    query = f"""
    select distinct
        pt.study_id,
        sd.name as name,
        sd.short_name as study_short_title,
        sd.short_code as study_acronym,
        sd.attribution as external_url
    from participant pt
    join study sd on pt.study_id = sd.kf_id
    where pt.kf_id in ({str(participant_list)[1:-1]})
    """
    return query


def build_study_table(output_table, db_url, participant_list):
    """Build the study table

    Build the study manifest

    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :param submission_template_dict: template submission manifests
    :type submission_template_dict: dict
    :return: study table
    :rtype: OutputTable
    """
    output_table.logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    output_table.logger.info("Querying for manifest of studies")
    study_table = pd.read_sql(study_query(participant_list), conn)
    conn.close()
    output_table.logger.info("Converting KF enums to CDS enums")
    study_table["consent"] = "GRU"
    study_table["acl"] = "Open Access"
    study_table["type"] = output_table.name
    return study_table
