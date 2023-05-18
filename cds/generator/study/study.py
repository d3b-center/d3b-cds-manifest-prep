from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.columns import order_columns

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def build_study_table(
    db_url, participant_list, submission_package_dir, submission_template_dict
):
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
    output_table = submission_template_dict["study"]
    # Set the column order and sort on key column
    output_table = order_columns(
        output_table, "study", submission_template_dict
    ).sort_values("participant_id")
    logger.info("saving sample manifest to file")
    output_table.to_csv(
        f"{submission_package_dir}/participant.csv", index=False
    )
    return output_table
