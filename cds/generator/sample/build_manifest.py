from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import sample_query
from cds.generator.sample.mapping import *

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
        "sample_id",
        "sample_type",
        "participant_id",
        "sample_tumor_status",
        "sample_anatomical_site",
        "sample_age_at_collection",
    ]
    return manifest[columns]


def build_sample_table(db_url, sample_list, submission_package_dir):
    logger.info("Building sample table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)

    logger.info("Querying for manifest of samples      ")
    sample_table = pd.read_sql(sample_query(sample_list), conn)
    logger.info("Converting KF enums to CDS enums")
    sample_table["sample_tumor_status"] = sample_table[
        "sample_tumor_status"
    ].apply(lambda x: status_map.get(x))

    sample_table["sample_anatomical_site"] = sample_table[
        "sample_tumor_status"
    ].apply(lambda x: anatomical_site_map.get(x))
    sample_table["sample_type"] = sample_table.apply(sample_type, axis=1)
    sample_table = order_columns(sample_table)
    logger.info("saving sample manifest to file")
    sample_table.to_csv(f"{submission_package_dir}/sample.csv", index=False)
