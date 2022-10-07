import pandas as pd
from tqdm import tqdm

file_manifest = pd.read_csv("data/submission_packet/file.csv")
mapping = pd.read_csv("data/submission_packet/file_sample_participant_map.csv")
participant_manifest = pd.read_csv("data/submission_packet/participant.csv")
genomic_info_table = pd.read_csv("data/submission_packet/genomic_info.csv")
sample_manifest = pd.read_csv("data/submission_packet/sample.csv")
diagnosis_manifest = pd.read_csv("data/submission_packet/diagnosis.csv")


# samples
print("all samples in mapping")
for sample in tqdm(sample_manifest["sample_id"]):
    if sample not in mapping["sample_id"].drop_duplicates().to_list():
        print(sample)

print("all mapping samples in sample manifest")
for sample in tqdm(mapping["sample_id"].drop_duplicates().to_list()):
    if sample not in sample_manifest["sample_id"].drop_duplicates().to_list():
        print(sample)

# participants
print("all participants in mapping")
for participant in tqdm(participant_manifest["participant_id"]):
    if participant not in mapping["participant_id"].drop_duplicates().to_list():
        print(participant)

print("all mapping participants in participant manifest")
for participant in tqdm(mapping["participant_id"].drop_duplicates().to_list()):
    if (
        participant
        not in participant_manifest["participant_id"]
        .drop_duplicates()
        .to_list()
    ):
        print(participant)
# files
print("all files in mapping")
foo = []
for file in tqdm(file_manifest["file_id"]):
    if file not in mapping["file_id"].drop_duplicates().to_list():
        foo.append(file)
        print(file)

print("all mapping files in file manifest")
for file in tqdm(mapping["file_id"].drop_duplicates().to_list()):
    if file not in file_manifest["file_id"].drop_duplicates().to_list():
        print(file)


# confirm that every file in the bucket is in the manifest
scrape = pd.read_csv("data/cds_scrape.tsv", sep="\t")
scrape["file_url_in_cds"] = "s3://" + scrape["Bucket"] + "/" + scrape["Key"]


merged = file_manifest.merge(
    scrape, on="file_url_in_cds", how="outer", indicator=True
)
issues = merged[merged["_merge"] != "both"]
print(len(issues))


breakpoint()
# compare two versions

file_manifest_v5 = pd.read_csv("data/submission_packet_v5/file-v5.csv")
mapping_v5 = pd.read_csv(
    "data/submission_packet_v5/file_sample_participant_map-v5.csv"
)
participant_manifest_v5 = pd.read_csv(
    "data/submission_packet_v5/participant-v5.csv"
)
genomic_info_table_v5 = pd.read_csv(
    "data/submission_packet_v5/genomic_info-v5.csv"
)
sample_manifest_v5 = pd.read_csv("data/submission_packet_v5/sample-v5.csv")

file_ids = file_manifest["file_id"].drop_duplicates().to_list()
file_ids_v5 = file_manifest_v5["file_id"].drop_duplicates().to_list()
for file in tqdm(file_ids):
    if file not in file_ids_v5:
        print(file)

dx_pt = participant_manifest["participant_id"].drop_duplicates().to_list()
participant_list_v5 = (
    participant_manifest_v5["participant_id"].drop_duplicates().to_list()
)
participant_list = (
    participant_manifest["participant_id"].drop_duplicates().to_list()
)
for a in tqdm(dx_pt):
    if a not in participant_list:
        print(a)
for a in tqdm(dx_pt):
    if a not in participant_list_v5:
        print(a)
for a in tqdm(participant_list):
    if a not in participant_list_v5:
        print(a)
for a in tqdm(participant_list_v5):
    if a not in participant_list:
        print(a)

sample_list_v5 = sample_manifest_v5["sample_id"].drop_duplicates().to_list()
sample_list = sample_manifest["sample_id"].drop_duplicates().to_list()
for a in tqdm(sample_list):
    if a not in sample_list_v5:
        print(a)
for a in tqdm(sample_list_v5):
    if a not in sample_list:
        print(a)
# v2_not_in_v1 = file_manifest[file_manifest["file_id"].isin(foo)]

# md5s = v2_not_in_v1["md5sum"].to_list()

# for md5 in tqdm(md5s):
#     if md5 not in v1_files["md5sum"].drop_duplicates().to_list():
#         print(md5)
