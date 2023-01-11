import os

from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.constants import p30_seq_files_bucket_name
from cds.qc.files import get_bucket_scrape, qc_bucket_files, qc_files
from cds.qc.participants import qc_participants
from cds.qc.samples import qc_samples

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def qc_submission_package(
    db_url=os.getenv("DATABASE_URL"),
    submission_packager_dir="data/submission_packet/",
):
    """QC a CDS submision Package

    Performs relationship testing between documents in a CDS submission.

    :param db_url: connection url to database that has information about your
    manifests, defaults to os.getenv("DATABASE_URL")
    :type db_url: str, optional
    :param submission_packager_dir: Directory that holds your submission
    package, defaults to "data/submission_packet/"
    :type submission_packager_dir: str, optional
    """
    logger.info(f"Loading manifests in {submission_packager_dir}")
    # Load the individual manifests
    file_manifest = pd.read_csv(submission_packager_dir + "file.csv")
    mapping = pd.read_csv(
        submission_packager_dir + "file_sample_participant_map.csv"
    )
    participant_manifest = pd.read_csv(
        submission_packager_dir + "participant.csv"
    )
    genomic_info_table = pd.read_csv(
        submission_packager_dir + "genomic_info.csv"
    )
    sample_manifest = pd.read_csv(submission_packager_dir + "sample.csv")
    diagnosis_manifest = pd.read_csv(submission_packager_dir + "diagnosis.csv")
    try:
        diagnosis_sample_mapping = pd.read_csv(
            submission_packager_dir + "diagnosis_sample_mapping.csv"
        )
    except FileNotFoundError:
        logger.warning("No diagnosis_sample_mapping file found")
        diagnosis_sample_mapping = pd.DataFrame(
            {"diagnosis_id": [], "sample_id": []}
        )
    # connect to your database
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    # generate lists
    logger.info("generating unique lists of items from manifests")
    sample_list = sample_manifest["sample_id"].drop_duplicates().to_list()
    mapping_sample_list = mapping["sample_id"].drop_duplicates().to_list()
    genomic_info_sample_list = (
        genomic_info_table["sample_id"].drop_duplicates().to_list()
    )
    participant_list = (
        participant_manifest["participant_id"].drop_duplicates().to_list()
    )
    mapping_participant_list = (
        mapping["participant_id"].drop_duplicates().to_list()
    )
    diagnosis_participant_list = (
        diagnosis_manifest["participant_id"].drop_duplicates().to_list()
    )
    sample_participant_list = (
        sample_manifest["participant_id"].drop_duplicates().to_list()
    )
    file_list = file_manifest["file_id"].drop_duplicates().to_list()
    mapping_file_list = mapping["file_id"].drop_duplicates().to_list()
    genomic_info_file_list = (
        genomic_info_table["file_id"].drop_duplicates().to_list()
    )
    diagnosis_sample_map_sample_list = (
        diagnosis_sample_mapping["sample_id"].drop_duplicates().to_list()
    )
    diagnosis_sample_map_diagnosis_list = (
        diagnosis_sample_mapping["diagnosis_id"].drop_duplicates().to_list()
    )

    # run QC
    sample_qc_dict = qc_samples(
        sample_list,
        mapping_sample_list,
        genomic_info_sample_list,
        diagnosis_sample_map_sample_list,
    )
    participant_qc_dict = qc_participants(
        participant_list,
        mapping_participant_list,
        diagnosis_participant_list,
        sample_participant_list,
    )
    file_qc_dict = qc_files(
        file_list, mapping_file_list, genomic_info_file_list
    )

    bucket_scrape = get_bucket_scrape(conn, p30_seq_files_bucket_name)
    bucket_file_qc_df = qc_bucket_files(file_manifest, bucket_scrape)
    # save qc results to files:
    pd.DataFrame.from_dict(sample_qc_dict, orient="index").to_csv(
        "data/qc/samples.csv"
    )
    pd.DataFrame.from_dict(participant_qc_dict, orient="index").to_csv(
        "data/qc/participants.csv"
    )
    pd.DataFrame.from_dict(file_qc_dict, orient="index").to_csv(
        "data/qc/files.csv"
    )
    bucket_file_qc_df.to_csv("data/qc/bucket_file.csv", index=False)
