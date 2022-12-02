from ast import literal_eval

from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.constants import seq_file_bucket_name
from cds.common.queries import file_bucket_query, file_kf_query, file_query

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def build_file_table(db_url, file_list, submission_package_dir):
    logger.info("Building file table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)

    logger.info("Querying for manifest of files")
    file_table = pd.read_sql(file_query(file_list, seq_file_bucket_name), conn)
    logger.info("saving file manifest to file")
    file_table.to_csv(f"{submission_package_dir}/file.csv", index=False)
