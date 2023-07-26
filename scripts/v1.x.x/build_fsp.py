"""
Build the file-sample-participant mapping for cds v1.x and validate that every
file in it is accounted for
"""
import os
from collections import Counter

from d3b_cavatica_tools.utils.logging import get_logger

import pandas as pd
from sqlalchemy import create_engine, text

logger = get_logger(__name__, testing_mode=False)

POSTGRES_HOST = "kf-api-dataservice-postgres-prd-rds.cnbodenpufmp.us-east-1.rds.amazonaws.com"  # noqa
POSTGRES_PORT = "5432"
POSTGRES_DB = "kfpostgresprd"

X01_BUCKET = "cds-306-phs002517-x01"

kf_engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('KFPOSTGRES_USERNAME')}:"
    f"{os.getenv('KFPOSTGRES_PASSWORD')}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

query = f"""
SELECT DISTINCT
  bg.genomic_file_id AS file_id,
  bs.participant_id AS participant_id,
  bg.biospecimen_id AS sample_id,
  gf.external_id AS s3path
FROM genomic_file AS gf
JOIN biospecimen_genomic_file AS bg
  ON gf.kf_id = bg.genomic_file_id
JOIN biospecimen AS bs
  ON bs.kf_id = bg.biospecimen_id
WHERE gf.external_id LIKE 's3://{X01_BUCKET}%%'
ORDER BY sample_id, participant_id, file_id
"""
conn = kf_engine.connect()
logger.info("Querying for fsp")
x01_fsp = pd.read_sql(
    text(query),
    conn,
)
logger.info("Complete")

s3scrape_query = f"""
WITH most_recent AS (
  SELECT max(scrape_timestamp)::date AS max_scrape_time
  FROM file_metadata.aws_scrape
  WHERE bucket = '{X01_BUCKET}'
)

SELECT DISTINCT s3path, lastmodified::date
FROM file_metadata.aws_scrape
WHERE
  bucket = '{X01_BUCKET}'
  AND scrape_timestamp::date = (
    SELECT max_scrape_time
    FROM most_recent
  )
"""

logger.info("Querying for s3scrape")
s3scrape = pd.read_sql(text(s3scrape_query), conn)
logger.info("Complete")

conn.close()

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

logger.info("removing jhu samples from the file sample participant mapping")
breakpoint()
x01_fsp = x01_fsp[~x01_fsp["sample_id"].isin(jhu_samples["kf_id"].to_list())]
# Validation

# * Accounting
manifest_file_set = set(x01_fsp["s3path"].drop_duplicates())
bucket_file_set = set(s3scrape["s3path"].drop_duplicates())
logger.info(f"manifest file count: {len(manifest_file_set)}")
logger.info(f"bucket file count: {len(bucket_file_set)}")

files_in_both = manifest_file_set.intersection(bucket_file_set)
files_only_in_manifest = manifest_file_set.difference(bucket_file_set)
files_only_in_bucket = bucket_file_set.difference(manifest_file_set)
logger.info(f"Count of files in common: {len(files_in_both)}")
logger.info(f"Count of files only in manifest: {len(files_only_in_manifest)}")
logger.info(f"Count of files only in bucket: {len(files_only_in_bucket)}")

logger.info("Information about files only in the bucket")

# Investigate the files only in the bucket


def extract_root_dir(s3path):
    return s3path.replace(f"s3://{X01_BUCKET}/", "").partition("/")[0]


def count_and_report(things, count_text):
    thing_counts = Counter(things)
    for k, v in zip(thing_counts.keys(), thing_counts.values()):
        print(f"{count_text} {k}: {v}")


bucket_only_files_root_dir = list(map(extract_root_dir, files_only_in_bucket))

logger.info("information about the root directory")
count_and_report(bucket_only_files_root_dir, "Count of files in")

logger.info("Investigating Source Files")

source_files = [
    f
    for f, i in zip(files_only_in_bucket, bucket_only_files_root_dir)
    if i == "source"
]
logger.info("Source file extensions")
source_file_extensions = list(map(lambda x: x.rpartition(".")[2], source_files))
count_and_report(source_file_extensions, "Count of files with extension")


source_tsv = [
    f for f, i in zip(source_files, source_file_extensions) if i == "tsv"
]
logger.info(f"Source TSV file: {source_tsv[0]}")

logger.info("Investigating Harmonized Files")
harmonized_files = [
    f
    for f, i in zip(files_only_in_bucket, bucket_only_files_root_dir)
    if i == "harmonized-data"
]
harmonized_path1 = set(
    map(
        lambda x: x.replace(
            f"s3://{X01_BUCKET}/harmonized-data/", ""
        ).partition("/")[0],
        harmonized_files,
    )
)
for dir in harmonized_path1:
    dir_files = [
        f
        for f in harmonized_files
        if f.startswith(f"s3://{X01_BUCKET}/harmonized-data/{dir}/")
    ]
    dir_parts = list(
        map(
            lambda x: x.replace(
                f"s3://{X01_BUCKET}/harmonized-data/{dir}/", ""
            ).partition("/")[0],
            dir_files,
        )
    )
    dir_part_count = len(set(dir_parts))
    if dir_part_count == "workflow-outputs":
        count_and_report(dir_parts, f"Count of files in harmonized-data/{dir}/")
    else:
        logger.info(f"Count of keys in harmonized-data/{dir}: {dir_part_count}")

logger.info("Saving output fsp")
x01_fsp.to_csv("cds/data/x01_fsp.csv", index=False)
