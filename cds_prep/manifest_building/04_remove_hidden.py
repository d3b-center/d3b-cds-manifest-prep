import os

import pandas as pd
import psycopg2
from tqdm import tqdm

DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)

file_manifest = pd.read_csv("data/submission_packet/file.csv")
mapping = pd.read_csv("data/submission_packet/file_sample_participant_map.csv")
participant_manifest = pd.read_csv("data/submission_packet/participant.csv")
genomic_info_table = pd.read_csv("data/submission_packet/genomic_info.csv")
sample_manifest = pd.read_csv("data/submission_packet/sample.csv")
diagnosis_manifest = pd.read_csv("data/submission_packet/diagnosis.csv")

participant_list = (
    participant_manifest["participant_id"].drop_duplicates().to_list()
)
sample_list = sample_manifest["sample_id"].drop_duplicates().to_list()
file_list = file_manifest["file_id"].drop_duplicates().to_list()

p_vis = pd.read_sql(
    f"""
    select kf_id as participant_id, visible as p_visible
    from participant
    where kf_id in ({str(participant_list)[1:-1]})
    """,
    conn,
)

bs_vis = pd.read_sql(
    f"""
    select kf_id as sample_id, visible as bs_visible
    from biospecimen
    where kf_id in ({str(sample_list)[1:-1]})
    """,
    conn,
)

gf_vis = pd.read_sql(
    f"""
    select kf_id as file_id, visible as gf_visible
    from genomic_file
    where kf_id in ({str(file_list)[1:-1]})
    """,
    conn,
)


with_vis = mapping.merge(gf_vis).merge(bs_vis).merge(p_vis)

any_hidden = with_vis[
    (~with_vis["gf_visible"])
    | (~with_vis["bs_visible"])
    | (~with_vis["p_visible"])
]


files_to_remove = any_hidden["file_id"].drop_duplicates().to_list()
samples_hidden = (
    any_hidden[(~any_hidden["p_visible"]) | (~any_hidden["bs_visible"])][
        "sample_id"
    ]
    .drop_duplicates()
    .to_list()
)
participants_hidden = (
    any_hidden[~any_hidden["p_visible"]]["participant_id"]
    .drop_duplicates()
    .to_list()
)

files_missing_info = (
    genomic_info_table[
        genomic_info_table.drop(columns=["insert_size"]).isnull().any(axis=1)
    ]["file_id"]
    .drop_duplicates()
    .to_list()
)

participants_to_remove_for_no_files = []
# get the participants and specimens whom no longer have any files in cds:
for p in tqdm(participant_list):
    files = with_vis[with_vis["participant_id"] == p]["file_id"].to_list()
    all_files__being_removed = all([f in files_to_remove for f in files])
    if all_files__being_removed:
        participants_to_remove_for_no_files.append(p)

participants_to_remove = list(
    set(participants_hidden + participants_to_remove_for_no_files)
)

samples_to_remove_for_no_files = []
for s in tqdm(sample_list):
    files = with_vis[with_vis["sample_id"] == s]["file_id"].to_list()
    all_files__being_removed = all([f in files_to_remove for f in files])
    if all_files__being_removed:
        participants_to_remove_for_no_files.append(s)

samples_to_remove = list(set(samples_hidden + samples_to_remove_for_no_files))

breakpoint()


new_file_manifest = file_manifest[
    ~file_manifest["file_id"].isin(files_to_remove)
]

new_mapping = mapping[
    (~mapping["file_id"].isin(files_to_remove))
    & (~mapping["sample_id"].isin(samples_to_remove))
    & (~mapping["participant_id"].isin(participants_to_remove))
]
new_participant_manifest = participant_manifest[
    ~participant_manifest["participant_id"].isin(participants_to_remove)
]
new_genomic_info_table = genomic_info_table[
    (~genomic_info_table["file_id"].isin(files_to_remove))
    & (~genomic_info_table["sample_id"].isin(samples_to_remove))
]
new_sample_manifest = sample_manifest[
    (~sample_manifest["participant_id"].isin(participants_to_remove))
    & (~sample_manifest["sample_id"].isin(samples_to_remove))
]

new_diagnosis_manifest = diagnosis_manifest[
    ~diagnosis_manifest["participant_id"].isin(participants_to_remove)
]
