from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import histologies_query

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def fetch_histologies_file(
    db_url, output_location="openpedcan_histologies.csv"
):
    """fetch and save the histologies data

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param output_location: _description_, defaults to "openpedcan_histologies.csv"
    :type output_location: str, optional
    :return: histologies data
    :rtype: pandas.DataFrame
    """  # noqa
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    histologies = pd.read_sql(histologies_query(), conn)
    conn.close()
    histologies.to_csv(output_location, index=False)
    return histologies
