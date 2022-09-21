import pandas as pd

mapping = pd.read_csv("data/submission_packet/file_sample_participant_map.csv")
participant_manifest = pd.read_csv("data/submission_packet/participant.csv")

# SSM
ssm = (
    mapping[["participant_id", "sample_id"]]
    .drop_duplicates()
    .rename(columns={"participant_id": "SUBJECT_ID", "sample_id": "SAMPLE_ID"})
)
ssm.to_csv("data/3a_SSM_DS.txt", sep="\t", index=False)

# Subject consent


subject_consent = participant_manifest[["participant_id", "gender"]].rename(
    columns={"participant_id": "SUBJECT_ID", "gender": "SEX"}
)
subject_consent["CONSENT"] = 1
subject_consent["SEX"] = subject_consent["SEX"].apply(
    lambda x: {
        "female": "2",
        "male": "1",
        "unknown": "UNK",
        "not reported": "UNK",
    }.get(x)
)
subject_consent[["SUBJECT_ID", "CONSENT", "SEX"]].to_csv(
    "data/2a_SubjectConsent_DS.txt", sep="\t", index=False
)
