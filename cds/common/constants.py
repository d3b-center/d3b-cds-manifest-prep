import pkg_resources

p30_seq_files_bucket_name = "cds-246-phs002517-sequencefiles-p30-fy20"

submission_package_default_dir = "data/submission_packet/"


output_tables = {
    "study": {"parent_concept": None, "order_by": "study_id"},
    "study_admin": {"order_by": ["study.study_id", "study_admin_id"]},
    "study_arm": {"order_by": ["study.study_id", "study_arm_id"]},
    "study_funding": {"order_by": ["study.study_id", "study_funding_id"]},
    "study_personnel": {"order_by": ["study.study_id", "study_personnel_id"]},
    "publication": {"order_by": ["study.study_id", "publication_id"]},
    "participant": {"order_by": ["study.study_id", "participant_id"]},
    "family_relationship": {
        "order_by": ["participant.participant_id", "family_relationship_id"]
    },
    "diagnosis": {"order_by": ["participant.participant_id", "diagnosis_id"]},
    "therapeutic_procedure": {
        "order_by": ["participant.participant_id", "therapeutic_procedure_id"]
    },
    "medical_history": {
        "order_by": ["participant.participant_id", "medical_history_id"]
    },
    "exposure": {"order_by": ["participant.participant_id", "exposure_id"]},
    "follow_up": {"order_by": ["participant.participant_id", "follow_up_id"]},
    "molecular_test": {
        "order_by": ["participant.participant_id", "molecular_test_id"]
    },
    "sample": {"order_by": ["participant.participant_id", "sample_id"]},
    "sample_diagnosis": {"order_by": ["sample_id", ""]},
    "pdx": {"order_by": ["sample_id", ""]},
    "sequencing_file": {"order_by": ["sample_id", ""]},
    "clinical_measure_file": {"order_by": ""},
    "methylation_array_file": {"order_by": ["sample_id", ""]},
    "imaging_file": {"order_by": ["sample_id", ""]},
    "single_cell_sequencing_file": {"order_by": ["sample_id", ""]},
    "synonym": {"order_by": ["sample_id", ""]},
}

all_generator_list = output_tables.keys

default_postgres_url = (
    "postgresql://username:password@localhost:5432/default_database"
)

seq_file_bucket_name = "cds-246-phs002517-sequencefiles-p30-fy20/"

file_sample_participant_map_default = "data/file_sample_participant_map.csv"

template_default = "data/CCDI_Submission_Template_v1.1.1.xlsx"
