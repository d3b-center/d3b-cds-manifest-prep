import os
from ast import literal_eval

import pandas as pd
import psycopg2
import sevenbridges as sbg
from d3b_cavatica_tools.utils.logging import get_logger

from queries import (
    all_scrapes_sql,
    already_transferred_sql,
    participant_query,
    pnoc_sql,
    sample_query,
    sequencing_query,
)
from utils import *

logger = get_logger(__name__, testing_mode=False)

DB_URL = os.getenv("DATABASE_URL")

issues = pd.read_csv("issues.csv")

issues["url"] = "s3://" + issues["Key"]

url_list = issues["url"].to_list()

query = f""" 
    SELECT hashes.*
    from file_metadata.hashes
    where hashes.s3path in ({str(url_list)[1:-1]})
"""

conn = psycopg2.connect(DB_URL)
hashes = pd.read_sql(
    query,
    conn,
)

breakpoint()

# confirm that the hashes match

hashes["target_url"] = hashes["s3path"].apply(
    lambda x: x.replace("s3://", "s3://cds-246-phs002517-p30-fy20/")
)


copy_query = f""" 
    SELECT hashes.*
    from file_metadata.hashes
    where hashes.s3path in ({str(hashes['target_url'].to_list())[1:-1]})
"""

hash_copies = pd.read_sql(
    copy_query,
    conn,
)


conn.close()

merge = hashes.merge(
    hash_copies, left_on="target_url", right_on="s3path", how="outer"
)
