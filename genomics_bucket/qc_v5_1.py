import pandas as pd
from tqdm import tqdm

file_manifest = pd.read_csv("data/submission_packet_v5_1/file.csv")
mapping = pd.read_csv(
    "data/submission_packet_v5_1/file_sample_participant_map.csv"
)
participant_manifest = pd.read_csv(
    "data/submission_packet_v5_1/participant.csv"
)
genomic_info_table = pd.read_csv("data/submission_packet_v5_1/genomic_info.csv")
sample_manifest = pd.read_csv("data/submission_packet_v5_1/sample.csv")
diagnosis_manifest = pd.read_csv("data/submission_packet/diagnosis.csv")
breakpoint()

# samples
sample_list = sample_manifest["sample_id"].drop_duplicates().to_list()
mapping_sample_list = mapping["sample_id"].drop_duplicates().to_list()
print("all samples in mapping")
for sample in tqdm(sample_list):
    if sample not in mapping_sample_list:
        print(sample)

print("all mapping samples in sample manifest")
foo = []
for sample in tqdm(mapping_sample_list):
    if sample not in sample_list:
        foo.append(sample)
        print(sample)

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
print("all files in mapping")
file_list = file_manifest["file_id"].drop_duplicates().to_list()
mapping_file_list = mapping["file_id"].drop_duplicates().to_list()
foo = []
for file in tqdm(file_list):
    if file not in mapping_file_list:
        foo.append(file)
        print(file)

print("all mapping files in file manifest")
for file in tqdm(mapping_file_list):
    if file not in file_list:
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
