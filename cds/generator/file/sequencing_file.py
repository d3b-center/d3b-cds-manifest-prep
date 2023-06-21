import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from os import listdir

from cds.common.constants import cds_x01_bucket_name
from cds.generator.file.file_queries import (
    bg_sequencing_experiment_query,
    file_query,
    file_sequencing_experiment_query,
)

import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
from tqdm import tqdm


def file_type_mapper(output_table, row):
    file_type_map = {
        "bai": "bam_index",
        "bam": "bam",
        "crai": "cram_index",
        "cram": "cram",
        "csv": "csv",
        "maf": "maf",
        "md5": "txt",
        "pdf": "pdf",
        "png": "png",
        "tsv": "tsv",
        "txt": "txt",
        "vcf": "vcf",
    }
    if row["file_type"] in file_type_map.keys():
        return file_type_map.get(row["file_type"])
    # per cnvkit docs, seg and cns are tabular files
    # https://cnvkit.readthedocs.io/en/stable/fileformats.html
    elif row["file_type"] in ["cns", "seg"]:
        return "tsv"
    elif row["file_type"] == "html":
        "txt"
    elif row["file_type"] == "ped":
        return "tsv"
    elif row["file_type"] == "tar":
        if row["file_name"].endswith("RNASeQC.counts.tar.gz"):
            return "tar"
        elif row["file_name"].endswith(".gatk_cnv.tar.gz"):
            return "tar"
        else:
            output_table.logger.error(
                f"unknown file type: {row['file_type']}, {row['file_name']}"
            )
    elif row["file_type"] == "Not Reported":
        if row["file_name"].endswith(".Aligned.out.sorted.cram.crai"):
            return "cram_index"
        else:
            output_table.logger.error(
                f"unknown file type: {row['file_type']}, {row['file_name']}"
            )
    else:
        output_table.logger.error(
            f"unknown file type: {row['file_type']}, {row['file_name']}"
        )


library_strategy_map = {
    "WGS": "WGS",
    "RNA-Seq": "RNA-Seq",
    "WXS": "WXS",
    "Targeted Sequencing": "Targeted-Capture",
}


def platform_mapper(output_table, row):
    if row["platform"] == "Illumina":
        return "Illumina"
    elif row["platform"] == "Other":
        if (row["instrument_model"] in ["DNBSeq", "DNBseq"]) & (
            row["library_strategy"] == "RNA-Seq"
        ) & row["sequencing_center_id"] == "SC_FAD4KCQG":
            # all dnbseq, rna-seq samples sequenced at BGI are presumed to go
            # through BGISEQ
            return "BGQSEQ"


# q = f"""
# select distinct
# se.external_id as library_id,
# se.created_at::date,
# pt.study_id
# from participant pt
#   join biospecimen bs on pt.kf_id = bs.participant_id
#   join biospecimen_genomic_file bg on bs.kf_id = bg.biospecimen_id
#   join genomic_file gf on gf.kf_id = bg.genomic_file_id
#   join sequencing_experiment_genomic_file sg on gf.kf_id = sg.genomic_file_id
#   join sequencing_experiment se on se.kf_id = sg.sequencing_experiment_id
# where se.external_id in ({str(se_lib_list)[1:-1]})
# """


def get_sequencing_experiment(
    output_table,
    conn,
    file_sample_participant_map,
    use_cache=True,
    cache_file_name=".cached_sequencing_experiment_information.json",
    method=("gf", "bg"),
    temp_cache_dir="~/mount/temp_cds_cache",
):
    use_cache_override = False
    if use_cache:
        output_table.logger.info(
            "Attempting to use cached sequencing_experiment information"
        )
        try:
            sequencing_info = pd.read_json(cache_file_name)
        except FileNotFoundError as e:
            output_table.logger.warning(
                "Cache file not found. Consider setting use_cache=False"
            )
            output_table.logger.warning(e)
            use_cache_override = True
        except ValueError as e:
            output_table.logger.warning(
                "Found `ValueError`. This can happen if the cache file is not "
                "formatted correctly. Consider setting use_cache=False to "
                "regenerate. Error message:"
            )
            output_table.logger.warning(e)
            use_cache_override = True
        except Exception as e:
            output_table.logger.error("Detected unrecognized error:")
            output_table.logger.error(type(e))
            output_table.logger.error(e)
            sys.exit()
        else:
            output_table.logger.info("Loaced cache file successfully.")
    if use_cache_override:
        output_table.logger.warning("Falling back to regenerating.")
    if (not use_cache) or use_cache_override:
        output_table.logger.info(
            "Generating sequencing_experiment information. "
            "This may take a while."
        )
        if "gf" in method:
            output_table.logger.info(
                "Getting sequencing experiments by genomic file"
            )
            file_list = (
                file_sample_participant_map["file_id"]
                .drop_duplicates()
                .to_list()
            )
            chunk_size = 1000
            chunked_file_list = [
                file_list[i : i + chunk_size]
                for i in range(0, len(file_list), chunk_size)
            ]
            sequencing_info_list = []
            for flist in tqdm(chunked_file_list):
                sequencing_info_list.append(
                    pd.read_sql(
                        text(file_sequencing_experiment_query(flist)), conn
                    )
                )
            sequencing_info = pd.concat(sequencing_info_list).reset_index(
                drop=True
            )
        elif "bg" in method:
            output_table.logger.info(
                "Getting sequencing experiments by biospecimen-genomic file"
            )

            def get_bg_sequencing_experiment(file_id, biospecimen_id, conn):
                bg_se = pd.read_sql(
                    text(
                        bg_sequencing_experiment_query(file_id, biospecimen_id)
                    ),
                    conn,
                )
                bg_se.to_csv(
                    temp_cache_dir + f"/{file_id}_{biospecimen_id}.csv"
                )
                return bg_se

            already_gathered_files = listdir(
                temp_cache_dir.replace("~", "/home/ubuntu")
            )
            file_sample_participant_map["file_sample_file_name"] = (
                file_sample_participant_map["file_id"]
                + "_"
                + file_sample_participant_map["sample_id"]
                + ".csv"
            )

            things_needed = file_sample_participant_map[
                ~file_sample_participant_map["file_sample_file_name"].isin(
                    already_gathered_files
                )
            ]

            file_list = things_needed["file_id"]
            biospecimen_list = things_needed["sample_id"]
            output_table.logger.info(
                "Gathering sequencing experiments for "
                f"{len(file_list)} file-specimen combinations"
            )
            # Read all the data from database and save it
            with tqdm(total=len(file_list)) as pbar:
                with ThreadPoolExecutor() as tpex:
                    futures = {}
                    for file_id, biospecimen_id in zip(
                        file_list, biospecimen_list
                    ):
                        output_table.logger.debug(
                            f"Getting sequencing experiment for {file_id},"
                            f" {biospecimen_id}"
                        )
                        futures[
                            tpex.submit(
                                get_bg_sequencing_experiment,
                                file_id,
                                biospecimen_id,
                                conn,
                            )
                        ] = f"{file_id}_{biospecimen_id}"
                    sequencing_info_list = {}
                    for f in as_completed(futures):
                        sequencing_info_list[futures[f]] = f.result()
                        pbar.update(1)
            # read the files saved
            all_gathered_files = [
                temp_cache_dir + "/" + f
                for f in listdir(temp_cache_dir.replace("~", "/home/ubuntu"))
            ]
            chunk_size = 1000
            chunked_file_list = [
                all_gathered_files[i : i + chunk_size]
                for i in range(0, len(all_gathered_files), chunk_size)
            ]
            output_table.logger.info(
                "reading all the sequencing experiments (in groups of "
                f"{chunk_size} into one dataframe"
            )

            def csv_reader(file_name):
                try:
                    out = pd.read_csv(file_name)
                except pd.errors.EmptyDataError:
                    output_table.logger.error(
                        f"file had empty data error: {file_name}"
                    )
                return out

            sequencing_info_list = []
            for flist in tqdm(chunked_file_list):
                with ThreadPoolExecutor() as tpex:
                    sequencing_info_list.append(
                        pd.concat(tpex.map(csv_reader, flist))
                    )
            s
        else:
            output_table.logger.error(
                "unrecognized method. Must be one of 'gf' or 'bg'"
            )
            sys.exit()
        output_table.logger.info(
            f"saving sequencing experiments to cache file: {cache_file_name}"
        )
        breakpoint()
        sequencing_info.to_json(cache_file_name)
    return sequencing_info


def build_sequencing_file_table(
    output_table,
    db_url,
    file_sample_participant_map,
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
    file_list = (
        file_sample_participant_map["file_id"].drop_duplicates().to_list()
    )
    output_table.logger.info("connecting to database")
    sql_engine = create_engine(
        db_url.replace("postgresql://", "postgresql+psycopg2://")
    )
    conn = sql_engine.connect()
    output_table.logger.info(
        "Querying for manifest of files' sequencing information"
    )
    sequencing_info = get_sequencing_experiment(
        output_table=output_table,
        conn=conn,
        file_sample_participant_map=file_sample_participant_map,
        method="bg",
    )
    sequencing_info["library_strategy"] = sequencing_info[
        "library_strategy"
    ].apply(lambda x: library_strategy_map.get(x))
    sequencing_info = sequencing_info.rename(
        columns={"sample_id": "sample.sample_id"}
    )

    output_table.logger.info("Querying for manifest of files")
    db_info = pd.read_sql(
        text(file_query(file_list, cds_x01_bucket_name)), conn
    )
    conn.close()
    # curate the data to map to cds values
    file_info = (
        file_sample_participant_map[["file_id", "sample_id"]]
        .drop_duplicates()
        .rename(columns={"sample_id": "sample.sample_id"})
        .merge(db_info, how="left", on="file_id")
    )
    file_info["sequencing_file_id"] = file_info["file_id"]
    file_info["file_type"] = file_info.apply(
        lambda row: file_type_mapper(output_table, row), axis=1
    )

    file_info["type"] = output_table.name
    sequencing_file = file_info.merge(
        sequencing_info, on=("file_id", "sample.sample_id"), how="left"
    )
    return sequencing_file
