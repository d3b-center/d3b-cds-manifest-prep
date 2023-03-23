import os

import pandas as pd
from sqlalchemy import create_engine, text

db_url = os.getenv("DATABASE_URL")

POSTGRES_HOST = "kf-api-dataservice-postgres-prd-rds.cnbodenpufmp.us-east-1.rds.amazonaws.com"  # noqa
POSTGRES_PORT = "5432"
POSTGRES_DB = "kfpostgresprd"

engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('KFPOSTGRES_USERNAME')}:"
    f"{os.getenv('KFPOSTGRES_PASSWORD')}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

conn = engine.connect()

query = """
SELECT DISTINCT
  bg.genomic_file_id AS file_id,
  bs.participant_id AS participant_id,
  bg.biospecimen_id AS sample_id
FROM genomic_file AS gf
JOIN biospecimen_genomic_file AS bg
  ON gf.kf_id = bg.genomic_file_id
JOIN biospecimen AS bs
  ON bs.kf_id = bg.biospecimen_id
WHERE gf.external_id LIKE 's3://cds-306-phs002517-x01%%'
ORDER BY sample_id, participant_id, file_id
"""

x01_fsp = pd.read_sql(
    text(query),
    conn,
)

x01_fsp.to_csv("cds/data/x01_fsp.csv", index=False)
