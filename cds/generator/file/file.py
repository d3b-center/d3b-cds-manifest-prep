from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.columns import order_columns
from cds.common.constants import seq_file_bucket_name
from cds.common.queries import file_query

import pandas as pd
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def order_columns(manifest):
    """order columns in the manifest

    :param manifest: The manifest to order columns for
    :type manifest: pandas.DataFrame
    :return: The manifest with columns needed in the correct order
    :rtype: pandas.DataFrame
    """
    columns = [
        "type",
        "sample.sample_id",
        "sequencing_file_id",
        "file_name",
        "file_type",
        "file_description",
        "file_size",
        "md5sum",
        "file_url_in_cds",
        "dcf_indexd_guid",
        "library_id",
        "library_selection",
        "library_strategy",
        "library_layout",
        "library_source",
        "number_of_bp",
        "number_of_reads",
        "design_description",
        "platform",
        "instrument_model",
        "avg_read_length",
        "coverage",
        "reference_genome_assembly",
        "checksum_algorithm",
        "checksum_value",
        "custom_assembly_fasta_file_for_alignment",
        "file_mapping_level",
        "sequence_alignment_software",
    ]
    return manifest[columns]


def build_sequencing_file_table(
    db_url,
    file_sample_participant_map,
    submission_package_dir,
    submission_template_dict,
):
    """Build the file table

    Build the file manifest

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param file_list: file IDs to have in the file manifest
    :type file_list: list
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :return: file table
    :rtype: pandas.DataFrame
    """
    manifest_name = "sequencing_file"
    logger.info(f"Building {manifest_name} table")
    file_list = (
        file_sample_participant_map["file_id"].drop_duplicates().to_list()
    )
    logger.info("connecting to database")
    breakpoint()
    conn = psycopg2.connect(db_url)

    logger.info("Querying for manifest of files")
    output_table = file_sample_participant_map[["file_id", "sample_id"]]

    db_info = pd.read_sql(file_query(file_list, seq_file_bucket_name), conn)
    output_table = output_table.merge(db_info, how="left", on="file_id")
    output_table["type"] = manifest_name
    # Set the column order and sort on key column
    output_table = order_columns(
        output_table, manifest_name, submission_template_dict
    ).sort_values("file_id")
    logger.info("saving file manifest to file")
    output_table.to_csv(f"{submission_package_dir}/file.csv", index=False)
    return output_table
