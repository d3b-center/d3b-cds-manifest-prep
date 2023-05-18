from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import participant_query
from cds.generator.participant.map import ethnicity_map, gender_map, race_map

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def order_columns(manifest):
    """order columns in the manifest

    :param manifest: The manifest to order columns for
    :type manifest: pandas.DataFrame
    :return: The manifest with columns needed in the correct order
    :rtype: pandas.DataFrame
    """
    columns = [
        "type",
        "study.study_id",
        "participant_id",
        "race",
        "gender",
        "ethnicity",
        "altertnate_participant_id",
    ]
    return manifest[columns]


def build_participant_table(db_url, participant_list, submission_package_dir):
    """Build the participant table

    Build the participant manifest

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param participant_list: participant IDs to have in the participant manifest
    :type participant_list: list
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :return: participant table
    :rtype: pandas.DataFrame
    """
    logger.info("Building participant table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)

    logger.info("Querying for manifest of participants")
    participant_table = pd.read_sql(participant_query(participant_list), conn)
    logger.info("Converting KF enums to CDS enums")

    # rename the stusy ID column
    participant_table = participant_table.rename(
        columns={"study_id": "study.study_id"}
    )

    participant_table["type"] = "participant"
    participant_table["gender"] = participant_table["gender"].apply(
        lambda x: gender_map.get(x)
    )
    participant_table["race"] = participant_table["race"].apply(
        lambda x: race_map.get(x)
    )
    participant_table["ethnicity"] = participant_table["ethnicity"].apply(
        lambda x: ethnicity_map.get(x)
    )
    # Set the column order and sort on key column
    participant_table = order_columns(participant_table).sort_values(
        "participant_id"
    )
    logger.info("saving sample manifest to file")
    participant_table.to_csv(
        f"{submission_package_dir}/participant.csv", index=False
    )
    return participant_table
