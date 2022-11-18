import os

from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.constants import p30_seq_files_bucket_name

import pandas as pd
import psycopg2
from tqdm import tqdm

logger = get_logger(__name__, testing_mode=False)


def report_qc_result(error_count, entity, item_name):
    """Report result of a QC step to the logger. Escelate log level if QC
    indicated an error.

    :param error_count: Count of errors found in a QC step
    :type error_count: int
    :param entity: Name of things being QC'd
    :type entity: str
    :param item_name: Name of manifest being QC'd against
    :type item_name: str
    """
    message = f"{str(error_count)} {entity} are not in {item_name}"
    if error_count > 0:
        logger.error(message)
    else:
        logger.info(message)


def qc_samples(sample_list, mapping_sample_list, genomic_info_sample_list):
    """QC Relations between samples

    Test that, for every item in all input lists, each item is in all lists.

    :param sample_list: list of samples in the sample manifest
    :type sample_list: list
    :param mapping_sample_list: list of samples in the file-sample-participant
    mapping
    :type mapping_sample_list: list
    :param genomic_info_sample_list: list of samples in the genomic_info
    manifest
    :type genomic_info_sample_list: list
    :return: Information about item membership in each input list. Dict of dicts
    where each key is an identifier and the value is a dict describing if the
    item is in each input list.
    :rtype: dict
    """
    all_samples = list(
        set(sample_list + mapping_sample_list + genomic_info_sample_list)
    )
    result_dict = {
        i: {
            "in_sample_manifest": None,
            "in_genomic_info": None,
            "in_mapping": None,
        }
        for i in all_samples
    }
    logger.info("Checking that all samples are in all manifests")
    not_in_sample = []
    not_in_genomic_info = []
    not_in_mapping = []
    for sample in tqdm(all_samples):
        in_sample_manifest = sample in sample_list
        in_genomic_info = sample in genomic_info_sample_list
        in_mapping = sample in mapping_sample_list
        result_dict[sample]["in_sample_manifest"] = in_sample_manifest
        result_dict[sample]["in_genomic_info"] = in_genomic_info
        result_dict[sample]["in_mapping"] = in_mapping
        if not in_sample_manifest:
            not_in_sample.append(sample)
            logger.debug(f"{sample} not in sample_list")
        if not in_genomic_info:
            not_in_genomic_info.append(sample)
            logger.debug(f"{sample} not in genomic_info")
        if not in_mapping:
            not_in_mapping.append(sample)
            logger.debug(f"{sample} not in mapping")
    report_qc_result(len(not_in_sample), "samples", "sample_manifest")
    report_qc_result(
        len(not_in_genomic_info), "samples", "genomic_info_manifest"
    )
    report_qc_result(len(not_in_mapping), "samples", "mapping")
    return result_dict


def qc_participants(
    participant_list,
    mapping_participant_list,
    diagnosis_participant_list,
    sample_participant_list,
):
    """QC Relations between participants

    Test that, for every item in all input lists, each item is in all lists.

    :param participant_list: list of participants in the participant manifest
    :type participant_list: list
    :param mapping_participant_list: list of participants in the mapping file
    :type mapping_participant_list: list
    :param diagnosis_participant_list: list of participants in the diagnosis
    manifest
    :type diagnosis_participant_list: list
    :param sample_participant_list: list of participants in the sample manifest
    :type sample_participant_list: list
    :return: Information about item membership in each input list. Dict of dicts
    where each key is an identifier and the value is a dict describing if the
    item is in each input list.
    :rtype: dict
    """
    all_participants = list(
        set(
            participant_list
            + mapping_participant_list
            + diagnosis_participant_list
            + sample_participant_list
        )
    )
    result_dict = {
        i: {
            "in_participant_manifest": None,
            "in_diagnosis_manifest": None,
            "in_sample_manifest": None,
            "in_mapping": None,
        }
        for i in all_participants
    }
    logger.info("Checking that all participants are in all manifests")
    not_in_participant = []
    not_in_diagnosis = []
    not_in_sample = []
    not_in_mapping = []
    for participant in tqdm(all_participants):
        in_participant_manifest = participant in participant_list
        in_diagnosis = participant in diagnosis_participant_list
        in_sample = participant in sample_participant_list
        in_mapping = participant in mapping_participant_list
        result_dict[participant][
            "in_participant_manifest"
        ] = in_participant_manifest
        result_dict[participant]["in_diagnosis_manifest"] = in_diagnosis
        result_dict[participant]["in_sample_manifest"] = in_sample
        result_dict[participant]["in_mapping"] = in_mapping
        if not in_participant_manifest:
            not_in_participant.append(participant)
            logger.debug(f"{participant} not in participant_list")
        if not in_diagnosis:
            not_in_diagnosis.append(participant)
            logger.debug(f"{participant} not in diagnosis_participant_list")
        if not in_sample:
            not_in_sample.append(participant)
            logger.debug(f"{participant} not in sample_participant_list")
        if not in_mapping:
            not_in_mapping.append(participant)
            logger.debug(f"{participant} not in mapping")
    report_qc_result(
        len(not_in_participant), "participants", "participant_manifest"
    )
    report_qc_result(
        len(not_in_diagnosis), "participants", "diagnosis_manifest"
    )
    report_qc_result(len(not_in_sample), "participants", "sample_manifest")
    report_qc_result(len(not_in_mapping), "participants", "mapping")
    return result_dict


def qc_files(file_list, mapping_file_list, genomic_info_file_list):
    """QC Relations between files

    Test that, for every item in all input lists, each item is in all lists.

    :param file_list: list of files in the file manifest
    :type file_list: list
    :param mapping_file_list: list of files in the mapping file
    :type mapping_file_list: list
    :param genomic_info_file_list: list of files in the genomic_info manifest
    :type genomic_info_file_list: list
    :return: Information about item membership in each input list. Dict of dicts
    where each key is an identifier and the value is a dict describing if the
    item is in each input list.
    :rtype: dict
    """
    all_files = list(
        set(file_list + mapping_file_list + genomic_info_file_list)
    )
    result_dict = {
        i: {
            "in_file_manifest": None,
            "in_genomic_info": None,
            "in_mapping": None,
        }
        for i in all_files
    }
    logger.info("Checking that all files are in all manifests")
    not_in_file = []
    not_in_genomic_info = []
    not_in_mapping = []
    for file in tqdm(all_files):
        in_file_manifest = file in file_list
        in_genomic_info = file in genomic_info_file_list
        in_mapping = file in mapping_file_list
        result_dict[file]["in_file_manifest"] = in_file_manifest
        result_dict[file]["in_genomic_info"] = in_genomic_info
        result_dict[file]["in_mapping"] = in_mapping
        if not in_file_manifest:
            not_in_file.append(file)
            logger.debug(f"{file} not in file_list")
        if not in_genomic_info:
            not_in_genomic_info.append(file)
            logger.debug(f"{file} not in genomic_info")
        if not in_mapping:
            not_in_mapping.append(file)
            logger.debug(f"{file} not in mapping")
    report_qc_result(len(not_in_file), "files", "file_manifest")
    report_qc_result(len(not_in_genomic_info), "files", "genomic_info_manifest")
    report_qc_result(len(not_in_mapping), "files", "mapping")
    return result_dict


def get_bucket_scrape(conn, bucket_name):
    scrape_query = f"""
    with most_recent_scrape as (
        select max(scrape_timestamp::date)
        from file_metadata.aws_scrape
        where bucket = '{bucket_name}'
    )

    select *
    from file_metadata.aws_scrape
    where bucket = '{bucket_name}'
            and scrape_timestamp::date = (select max from most_recent_scrape)
    order by lastmodified
    """
    logger.info(f"querying database to get scrape of {bucket_name}")
    scrape = pd.read_sql(scrape_query, conn)
    return scrape


def qc_bucket_files(file_manifest, bucket_scrape):
    """Confirm that every file in file manifest is in bucket and vice versa

    :param file_manifest: CDS submission file manifest
    :type file_manifest: pandas.DataFrame
    :param bucket_scrape: Scrape of bucket where files are located
    :type bucket_scrape: pandas.DataFrame
    """
    logger.info("Checking that every file is in bucket and vice versa")
    merged = file_manifest.merge(
        bucket_scrape,
        left_on="file_url_in_cds",
        right_on="s3path",
        how="outer",
        indicator=True,
    )
    issues = merged[merged["_merge"] != "both"]
    return issues


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

    # run QC
    sample_qc_dict = qc_samples(
        sample_list, mapping_sample_list, genomic_info_sample_list
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
