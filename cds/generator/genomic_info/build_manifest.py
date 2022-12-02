from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.queries import (
    file_genome_query,
    harmonized_file_query,
    sequencing_query,
)
from cds.generator.genomic_info.mapping import *

import pandas as pd
import pkg_resources
import psycopg2

logger = get_logger(__name__, testing_mode=False)


def load_sc_gf_info():
    logger.debug("loading sc_genomic_info")
    fname = pkg_resources.resource_filename(
        "cds", "data/sequencing_center_genomic_info.csv"
    )
    return pd.read_csv(fname)


def load_bs_platform_map():
    logger.debug("loading bs_platform_map")
    fname = pkg_resources.resource_filename(
        "cds", "data/BS_SeqCenter_Platform_Model.xlsx"
    )
    return pd.read_excel(fname)


def build_genomic_info_table(
    db_url, file_sample_participant_map, submission_package_dir
):
    logger.info("Building genomic_info table")
    logger.info("connecting to database")
    conn = psycopg2.connect(db_url)

    sample_list = (
        file_sample_participant_map["sample_id"].drop_duplicates().to_list()
    )
    file_list = (
        file_sample_participant_map["file_id"].drop_duplicates().to_list()
    )

    logger.info("Querying for manifest of genomic information")
    sequencing_info = pd.read_sql(sequencing_query(sample_list), conn)
    platform_map = load_bs_platform_map().rename(
        columns={
            "Kids_First_Biospecimen_ID": "biospecimen_id",
            "seq_center": "name",
            "Platform": "platform",
            "Instrument Model": "instrument_model",
        }
    )
    center_genomic_info = load_sc_gf_info()
    # join sequencing center information to sequencing information
    sequencing_info = sequencing_info.merge(
        center_genomic_info,
        how="left",
        on=["sequencing_center_id", "experiment_strategy"],
    ).merge(platform_map, how="left", on="biospecimen_id")
    # use the supplied values as possible
    sequencing_info["platform"] = sequencing_info.apply(
        lambda row: row["platform_x"].upper()
        if row["platform_y"] != row["platform_y"]
        else row["platform_y"],
        axis=1,
    )
    sequencing_info["instrument_model"] = sequencing_info.apply(
        lambda row: row["instrument_model_x"]
        if row["instrument_model_y"] != row["instrument_model_y"]
        else row["instrument_model_y"],
        axis=1,
    ).apply(lambda x: instrument_model_map.get(x, x))
    sequencing_info = sequencing_info.merge(
        file_sample_participant_map[["sample_id", "file_id"]].rename(
            columns={"sample_id": "biospecimen_id"}
        ),
        how="left",
        on="biospecimen_id",
    )

    # convert kf values to cds enums
    sequencing_info["library_source"] = sequencing_info[
        "experiment_strategy"
    ].apply(lambda x: source_map.get(x))
    sequencing_info["library_strategy"] = sequencing_info.apply(
        lambda row: "Targeted-Capture"
        if row["experiment_strategy"] == "Targeted Sequencing"
        else row["experiment_strategy"],
        axis=1,
    )
    sequencing_info["library_layout"] = sequencing_info.apply(
        lambda row: "paired-end"
        if row["library_strategy"] == "WGS"
        else "single",
        axis=1,
    )
    harmonized_files = pd.read_sql(harmonized_file_query(file_list), conn)[
        "kf_id"
    ].to_list()
    file_genomes = pd.read_sql(file_genome_query(file_list), conn)
    sequencing_info = sequencing_info.merge(file_genomes)
    sequencing_info["reference_genome_assembly"] = sequencing_info.apply(
        lambda row: "GRCh38"
        if row["file_id"] in harmonized_files
        else row["reference_genome"],
        axis=1,
    )
    sequencing_info = (
        sequencing_info[
            [
                "biospecimen_id",
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
            ]
        ]
        .rename(columns={"biospecimen_id": "sample_id"})
        .drop_duplicates()
    )
    sequencing_info["library_id"] = (
        sequencing_info["sample_id"] + "__" + sequencing_info["file_id"]
    )
    sequencing_info.to_csv(
        f"{submission_package_dir}/genomic_info.csv", index=False
    )
