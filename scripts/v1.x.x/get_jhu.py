import os

from d3b_cavatica_tools.utils.logging import get_logger

import pandas as pd
from sqlalchemy import create_engine, text

logger = get_logger(__name__, testing_mode=False)

POSTGRES_WAREHOUSE_HOST = "d3b-warehouse-aurora.cluster-cxxdzxepyea2.us-east-1.rds.amazonaws.com"  # noqa
POSTGRES_WAREHOUSE_PORT = "5432"
POSTGRES_WAREHOUSE_DB = "postgres"


d3bwarehouse_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('D3BWAREHOUSE_USERNAME')}:"
    f"{os.getenv('D3BWAREHOUSE_PASSWORD')}"
    f"@{POSTGRES_WAREHOUSE_HOST}:{POSTGRES_WAREHOUSE_PORT}/"
    f"{POSTGRES_WAREHOUSE_DB}"
)


# query for jhu samples so that they can be excluded from the cds release
jhu_query = """
SELECT DISTINCT
    kf_id
FROM kfpostgres.biospecimen bs
LEFT JOIN prod.specimens spec ON bs.external_sample_id=spec.sample_id
-- this is not the most direct join but if we join on aliquot we have to
-- cast/change column types because the fields are not compatible. so this
-- works great as long as we include 'distinct' in the select
WHERE coordinating_institution = 'Johns Hopkins Medicine'
"""
logger.info("querying for JHU samples")
conn = d3bwarehouse_engine.connect()
jhu_samples = pd.read_sql(text(jhu_query), conn)
logger.info("Complete")
jhu_samples.to_csv("data/jhu_samples.csv", index=False)
logger.info("removing jhu samples from the file sample participant mapping")
breakpoint()
