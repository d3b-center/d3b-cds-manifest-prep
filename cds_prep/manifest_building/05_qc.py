import os

import pandas as pd
import psycopg2
from tqdm import tqdm

DB_URL = os.getenv("DATABASE_URL")

conn = psycopg2.connect(DB_URL)
submission_packet_prefix = "data/submission_packet/"
file_manifest = pd.read_csv(submission_packet_prefix + "file.csv")
mapping = pd.read_csv(
    submission_packet_prefix + "file_sample_participant_map.csv"
)
participant_manifest = pd.read_csv(submission_packet_prefix + "participant.csv")
genomic_info_table = pd.read_csv(submission_packet_prefix + "genomic_info.csv")
sample_manifest = pd.read_csv(submission_packet_prefix + "sample.csv")
diagnosis_manifest = pd.read_csv(submission_packet_prefix + "diagnosis.csv")

# samples
sample_list = sample_manifest["sample_id"].drop_duplicates().to_list()
mapping_sample_list = mapping["sample_id"].drop_duplicates().to_list()
print("all samples in mapping")
for sample in tqdm(sample_list):
    if sample not in mapping_sample_list:
        print(sample)

print("all mapping samples in sample manifest")
for sample in tqdm(mapping_sample_list):
    if sample not in sample_list:
        print(sample)

breakpoint()
# participants
participant_list = (
    participant_manifest["participant_id"].drop_duplicates().to_list()
)
mapping_participant_list = mapping["participant_id"].drop_duplicates().to_list()
diagnosis_participant_list = (
    diagnosis_manifest["participant_id"].drop_duplicates().to_list()
)

print("all participants in mapping")
for participant in tqdm(participant_list):
    if participant not in mapping_participant_list:
        print(participant)

print("all mapping participants in participant manifest")
for participant in tqdm(mapping_participant_list):
    if participant not in participant_list:
        print(participant)

print("all participants in diagnosis")
foo = []
for participant in tqdm(participant_list):
    if participant not in diagnosis_participant_list:
        print(participant)
        foo.append(participant)

print("all diagnosis participants in participant manifest")
for participant in tqdm(diagnosis_participant_list):
    if participant not in participant_list:
        print(participant)

print("all participants in sample_manifest")
sample_participant_list = (
    sample_manifest["participant_id"].drop_duplicates().to_list()
)
for participant in tqdm(participant_list):
    if participant not in sample_participant_list:
        print(participant)

print("all sample_manifest participants in participant manifest")
for participant in tqdm(sample_participant_list):
    if participant not in participant_list:
        print(participant)


# files
breakpoint()
print("all files in mapping")
file_list = file_manifest["file_id"].drop_duplicates().to_list()
mapping_file_list = mapping["file_id"].drop_duplicates().to_list()
foo = []
for file in tqdm(file_list):
    if file not in mapping_file_list:
        foo.append(file)

print("all mapping files in file manifest")
for file in tqdm(mapping_file_list):
    if file not in file_list:
        print(file)

# Genomic Information Mapping File

genomic_info_file_list = (
    genomic_info_table["file_id"].drop_duplicates().to_list()
)
genomic_info_sample_list = (
    genomic_info_table["sample_id"].drop_duplicates().to_list()
)
files_not_in_gi = []
gi_files_not_in_files = []
samples_not_in_gi = []
gi_samples_not_in_samples = []
print("all files in genomic_info")
for file in tqdm(file_list):
    if file not in genomic_info_file_list:
        files_not_in_gi.append(file)
        print(file)

print("all genomic_info files in file manifest")
for file in tqdm(genomic_info_file_list):
    if file not in file_list:
        gi_files_not_in_files.append(file)
        print(file)

print("all samples in genomic_info")
for sample in tqdm(sample_list):
    if sample not in genomic_info_sample_list:
        samples_not_in_gi.append(sample)
        print(sample)

print("all genomic_info samples in sample manifest")
for sample in tqdm(genomic_info_sample_list):
    if sample not in sample_list:
        gi_samples_not_in_samples.append(sample)
        print(sample)

# confirm that every file in the bucket is in the manifest
print("Fetching scrape of cds genomics bucket")
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
# scrape = pd.read_csv("data/cds_scrape.tsv", sep="\t")


merged = file_manifest.merge(
    scrape,
    left_on="file_url_in_cds",
    right_on="s3path",
    how="outer",
    indicator=True,
)
issues = merged[merged["_merge"] != "both"]
print(len(issues))


# check that the move manifest is the same as issues
file_move = pd.read_csv("data/file_moves_step04.csv")
issue_paths = issues[["s3path"]]
issue_paths["not_in_files_sheet"] = True

merge = issue_paths.merge(file_move, on="s3path", how="outer", indicator=True)

breakpoint()

# diagnosis stuff

ptdx = pd.read_sql(
    f"""
select * from diagnosis where participant_id in ({str(foo)[1:-1]})
""",
    conn,
)
p = pd.read_sql(
    f"""
select distinct study_id from participant where kf_id in ({str(foo)[1:-1]})
""",
    conn,
)


pt_cat = pd.read_sql(
    f"""
select kf_id, diagnosis_category from participant where kf_id in ({str(foo)[1:-1]})
""",
    conn,
)
