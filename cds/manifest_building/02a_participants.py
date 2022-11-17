import os
from ast import literal_eval

from d3b_cavatica_tools.utils.logging import get_logger

import pandas as pd
import psycopg2
from queries import participant_query
from utils import *

logger = get_logger(__name__, testing_mode=False)

DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)

# Read in the  files from previous steps
out = pd.read_csv("data/temp/ccdi_manifest.csv")

# generate the manifest of participants
participants_list = [
    i
    for i in out["pt_id"]
    .apply(lambda x: literal_eval(x) if "[" in x else x)
    .explode()
    .drop_duplicates()
    .to_list()
    if i
]

logger.info("querying for manifest of participants")
participant_manifest = pd.read_sql(
    participant_query(participants_list),
    conn,
)
gender_map = {
    "Female": "female",
    "Male": "male",
    "Not Reported": "not reported",
    "Not Available": "unknown",
}
race_map = {
    "White": "white",
    "Reported Unknown": "unknown",
    "Not Reported": "not reported",
    "Black or African American": "black or african american",
    "Other": "other",
    "Native Hawaiian or Other Pacific Islander": "native hawaiian or other pacific islander",
    "More Than One Race": "other",
    "Asian": "asian",
    "American Indian or Alaska Native": "american indian or alaska native",
    "Not Available": "unknown",
}
ethnicity_map = {
    "Reported Unknown": "unknown",
    "Not Hispanic or Latino": "not hispanic or latino",
    "Not Reported": "not reported",
    "Hispanic or Latino": "hispanic or latino",
    "Not Available": "unknown",
}

participant_manifest["gender"] = participant_manifest["gender"].apply(
    lambda x: gender_map.get(x)
)
participant_manifest["race"] = participant_manifest["race"].apply(
    lambda x: race_map.get(x)
)
participant_manifest["ethnicity"] = participant_manifest["ethnicity"].apply(
    lambda x: ethnicity_map.get(x)
)
participant_manifest.to_csv(
    "data/submission_packet/participant.csv", index=False
)
