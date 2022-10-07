from turtle import left

import pandas as pd
from kf_utils.dataservice.patch import *
from kf_utils.dataservice.scrape import *

old_diagnoses = pd.read_csv("data/diagnosis.csv")
good_dx_txt = pd.read_excel("data/CBTN - ICD-O(1).xlsx", sheet_name="CBTN")[
    "primary_diagnosis (free text)"
]
bad_dx_txt = pd.read_excel("data/CBTN - ICD-O(1).xlsx", sheet_name="Other")[
    ["source_text_diagnosis", "primary_diagnosis (free text)"]
].rename(columns={"primary_diagnosis (free text)": "new_primary_dx"})


new_dx = (
    old_diagnoses[["diagnosis_id", "primary_diagnosis"]]
    .merge(
        good_dx_txt,
        how="left",
        left_on="primary_diagnosis",
        right_on="primary_diagnosis (free text)",
    )
    .merge(
        bad_dx_txt,
        how="left",
        left_on="primary_diagnosis",
        right_on="source_text_diagnosis",
    )
)

new_dx["external_id"] = new_dx.apply(
    lambda x: x["primary_diagnosis (free text)"]
    if pd.isnull(x["new_primary_dx"])
    else x["new_primary_dx"],
    axis=1,
)
patches = (
    new_dx[["diagnosis_id", "external_id"]]
    .set_index("diagnosis_id")
    .to_dict(orient="index")
)

breakpoint()
api_url = "https://kf-api-dataservice.kidsfirstdrc.org"

# get the old diagnosis stuff for the sake of safety

# Yield all entities matching the given kfids
participants = []
for e in yield_entities_from_kfids(api_url, plist, show_progress=True):
    participants.append(e)


for k, v in patches.items():
    print(k, v)
    send_patches(api_url, {k: v})
