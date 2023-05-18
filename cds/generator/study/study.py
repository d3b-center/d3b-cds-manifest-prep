from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.columns import order_columns
from cds.common.tables import OutputTable

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def build_study_table(submission_package_dir, submission_template_dict):
    """Build the study table

    Build the study manifest

    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :param submission_template_dict: template submission manifests
    :type submission_template_dict: dict
    :return: study table
    :rtype: OutputTable
    """
    table_object = OutputTable(
        "study", submission_template_dict=submission_template_dict
    )
    # Build the output table
    table_object.template_is_output()
    # Save the table
    table_object.save_table(submission_package_dir)
    return table_object
