from d3b_cavatica_tools.utils.logging import get_logger

from cds.qc.report import report_qc_result

import pandas as pd
from tqdm import tqdm

logger = get_logger(__name__, testing_mode=False)


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
