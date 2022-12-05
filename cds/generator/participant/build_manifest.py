from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import participant_query
from cds.generator.participant.map import *

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def build_participant_table(db_url, participant_list, submission_package_dir):
    logger.info("Building participant table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)

    logger.info("Querying for manifest of participants")
    participant_table = pd.read_sql(participant_query(participant_list), conn)
    logger.info("Converting KF enums to CDS enums")

    participant_table["gender"] = participant_table["gender"].apply(
        lambda x: gender_map.get(x)
    )
    participant_table["race"] = participant_table["race"].apply(
        lambda x: race_map.get(x)
    )
    participant_table["ethnicity"] = participant_table["ethnicity"].apply(
        lambda x: ethnicity_map.get(x)
    )
    logger.info("saving sample manifest to file")
    participant_table.to_csv(
        f"{submission_package_dir}/participant.csv", index=False
    )
