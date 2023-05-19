import pkg_resources

p30_seq_files_bucket_name = "cds-246-phs002517-sequencefiles-p30-fy20"

submission_package_default_dir = "data/submission_packet/"


all_generator_list = {
    "study",
    "study_admin",
    "study_arm",
    "study_funding",
    "study_personnel",
    "publication",
    "participant",
    "family_relationship",
    "diagnosis",
    "therapeutic_procedure",
    "medical_history",
    "exposure",
    "follow_up",
    "molecular_test",
    "sample",
    "sample_diagnosis",
    "pdx",
    "sequencing_file",
    "clinical_measure_file",
    "methylation_array_file",
    "imaging_file",
    "single_cell_sequencing_file",
    "synonym",
}

default_postgres_url = (
    "postgresql://username:password@localhost:5432/default_database"
)

seq_file_bucket_name = "cds-246-phs002517-sequencefiles-p30-fy20/"

file_sample_participant_map_default = "data/file_sample_participant_map.csv"

template_default = "data/CCDI_Submission_Template_v1.1.1.xlsx"
