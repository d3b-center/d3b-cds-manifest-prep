import os

import pandas as pd
import psycopg2
from kf_utils.dataservice.scrape import yield_entities_from_kfids
from tqdm import tqdm

USE_TEMP = True
DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)

if not USE_TEMP:
    file_manifest = pd.read_csv("data/submission_packet/file.csv")
    mapping = pd.read_csv(
        "data/submission_packet/file_sample_participant_map.csv"
    )
    participant_manifest = pd.read_csv("data/submission_packet/participant.csv")
    genomic_info_table = pd.read_csv("data/submission_packet/genomic_info.csv")
    sample_manifest = pd.read_csv("data/submission_packet/sample.csv")
    diagnosis_manifest = pd.read_csv("data/submission_packet/diagnosis.csv")
else:
    file_manifest = pd.read_csv("data/temp/step_04/file.csv")
    mapping = pd.read_csv("data/temp/step_04/file_sample_participant_map.csv")
    participant_manifest = pd.read_csv("data/temp/step_04/participant.csv")
    genomic_info_table = pd.read_csv("data/temp/step_04/genomic_info.csv")
    sample_manifest = pd.read_csv("data/temp/step_04/sample.csv")
    diagnosis_manifest = pd.read_csv("data/temp/step_04/diagnosis.csv")

# copy files to a temp safe location
file_manifest.to_csv("data/temp/step_04/file.csv", index=False)
mapping.to_csv("data/temp/step_04/file_sample_participant_map.csv", index=False)
participant_manifest.to_csv("data/temp/step_04/participant.csv", index=False)
genomic_info_table.to_csv("data/temp/step_04/genomic_info.csv", index=False)
sample_manifest.to_csv("data/temp/step_04/sample.csv", index=False)
diagnosis_manifest.to_csv("data/temp/step_04/diagnosis.csv", index=False)


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


files_hidden = any_hidden["file_id"].drop_duplicates().to_list()
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
        genomic_info_table.drop(columns=["insert_size", "coverage"])
        .isnull()
        .any(axis=1)
    ]["file_id"]
    .drop_duplicates()
    .to_list()
)

files_to_remove = list(set(files_hidden + files_missing_info))

participants_to_remove_for_no_files = []
print("get the participants whom no longer have any files in cds")
for p in tqdm(participant_list):
    files = with_vis[with_vis["participant_id"] == p]["file_id"].to_list()
    all_files__being_removed = all([f in files_to_remove for f in files])
    if all_files__being_removed:
        participants_to_remove_for_no_files.append(p)

participants_to_remove = list(
    set(participants_hidden + participants_to_remove_for_no_files)
)
print("get the specimens whom no longer have any files in cds")
samples_to_remove_for_no_files = []
for s in tqdm(sample_list):
    files = with_vis[with_vis["sample_id"] == s]["file_id"].to_list()
    all_files_being_removed = all([f in files_to_remove for f in files])
    if all_files_being_removed:
        samples_to_remove_for_no_files.append(s)

samples_to_remove = list(set(samples_hidden + samples_to_remove_for_no_files))


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


new_file_manifest.to_csv("data/submission_packet/file.csv", index=False)
new_mapping.to_csv(
    "data/submission_packet/file_sample_participant_map.csv", index=False
)
new_participant_manifest.to_csv(
    "data/submission_packet/participant.csv", index=False
)
new_genomic_info_table.to_csv(
    "data/submission_packet/genomic_info.csv", index=False
)
new_sample_manifest.to_csv("data/submission_packet/sample.csv", index=False)
new_diagnosis_manifest.to_csv(
    "data/submission_packet/diagnosis.csv", index=False
)

breakpoint()
# Get info about the files to delete

scrape_query = """
with most_recent_scrape as (
    select max(scrape_timestamp::date)
    from file_metadata.aws_scrape
    where bucket = 'cds-246-phs002517-sequencefiles-p30-fy20'
)

select *
from file_metadata.aws_scrape
where bucket = 'cds-246-phs002517-sequencefiles-p30-fy20'
        and scrape_timestamp::date = (select max from most_recent_scrape)
order by lastmodified
"""

conn = psycopg2.connect(DB_URL)
scrape = pd.read_sql(scrape_query, conn)

file_query = f"""
select distinct gf.kf_id, idx.url 
from genomic_file gf
join file_metadata.indexd_scrape idx on gf.latest_did = idx.did
where kf_id in ({str(files_to_remove)[1:-1]})
"""
file_list = pd.read_sql(file_query, conn)


file_list["s3path"] = file_list["url"].apply(
    lambda x: x.replace(
        "s3://", "s3://cds-246-phs002517-sequencefiles-p30-fy20/"
    )
)
file_paths = file_list.merge(scrape, on="s3path", how="left")

# todo: delete the below. this is for data exploration
# check how many subjects are biegel subjects

# biegel_subjects = pd.read_csv(
#     "/home/ubuntu/d3b-cds-manifest-prep/data/CBTN_Biegel_subjects_2022-10-04.csv"
# )

# conn = psycopg2.connect(DB_URL)
# p_q = f"""
# select distinct  kf_id, external_id
# from participant
# where external_id in ({str(biegel_subjects['research_id'].to_list())[1:-1]})
# """

# p_id = pd.read_sql(p_q, conn)
