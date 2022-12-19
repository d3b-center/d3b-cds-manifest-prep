from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import file_genome_query

import pandas as pd
import pkg_resources
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def load_bix_mapping():
    """Load the bix-generated mapping of sample to genomic info metrics

    Load the data in the file `updated_nci_submit.csv`.

    :return: mapping of sample to genomic information metrics
    :rtype: pandas.DataFrame
    """
    logger.debug("loading bix mapping")
    mapping_cols = [
        "sample_id",
        "bases",
        "number_of_reads",
        "coverage",
        "avg_read_length",
        "insert_size",
        "platform",
        "instrument_model",
        "library_source",
        "library_strategy",
        "library_layout",
    ]
    fname = pkg_resources.resource_filename(
        "cds", "data/updated_nci_submit.csv"
    )
    mapping = pd.read_csv(fname).rename(
        columns={"insert_size_metrics": "insert_size"}
    )
    mapping["instrument_model"] = mapping["instrument_model"].apply(
        lambda x: x.strip()
    )
    mapping["library_strategy"] = mapping["library_strategy"].apply(
        lambda x: "Targeted-Capture" if x == "Targeted" else x
    )
    mapping["library_layout"] = mapping["library_layout"].apply(
        lambda x: "single" if x == "single-end" else x
    )
    return mapping[mapping_cols]


def get_file_genomes(file_list, conn):
    """Get the mapping of file to reference genome

    Queries the dataservice for the reference genome of files and sets
    harmonized files to GRCh38

    :param file_list: list of files to get reference genome for
    :type file_list: list
    :param conn: database connection object to kf prd postgres
    :type conn: psycopg2.connection
    :return: mapping between genomic_file ID and reference genome
    :rtype: pandas.DataFrame
    """
    genomes_ds = pd.read_sql(file_genome_query(file_list), conn)
    genomes_ds["reference_genome_assembly"] = genomes_ds.apply(
        lambda row: "GRCh38"
        if row["is_harmonized"]
        else row["reference_genome"],
        axis=1,
    )
    return genomes_ds[["file_id", "reference_genome_assembly"]]


def order_columns(manifest):
    """order columns in the manifest

    :param manifest: The manifest to order columns for
    :type manifest: pandas.DataFrame
    :return: The manifest with columns needed in the correct order
    :rtype: pandas.DataFrame
    """
    columns = [
        "sample_id",
        "file_id",
        "bases",
        "number_of_reads",
        "coverage",
        "avg_read_length",
        "insert_size",
        "platform",
        "instrument_model",
        "library_source",
        "library_strategy",
        "library_layout",
        "reference_genome_assembly",
        "library_id",
    ]
    return manifest[columns]


def build_genomic_info_table(
    db_url, file_sample_participant_map, submission_package_dir
):
    """Generate the genomic_info manifest table

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param file_sample_participant_map: mapping between file sample and
    participant
    :type file_sample_participant_map: pandas.DataFrame
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    """
    logger.info("Building genomic_info table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    file_list = (
        file_sample_participant_map["file_id"].drop_duplicates().to_list()
    )

    bix_map = load_bix_mapping()
    logger.info("Querying for reference genomes of files")
    file_genomes = get_file_genomes(file_list, conn)
    genomic_info = (
        file_sample_participant_map.drop(columns=["participant_id"])
        .drop_duplicates()
        .merge(bix_map, on="sample_id")
        .merge(file_genomes, on="file_id")
    )
    genomic_info["library_id"] = (
        genomic_info["sample_id"] + "__" + genomic_info["file_id"]
    )
    genomic_info = order_columns(genomic_info)
    genomic_info.to_csv(
        f"{submission_package_dir}/genomic_info.csv", index=False
    )
