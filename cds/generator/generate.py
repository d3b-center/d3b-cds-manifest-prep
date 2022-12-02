from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.constants import all_generator_list
from cds.generator.diagnosis.build_manifest import build_diagnosis_table
from cds.generator.file.build_manifest import build_file_table
from cds.generator.participant.build_manifest import build_participant_table
from cds.generator.sample.build_manifest import build_sample_table

import pandas as pd

logger = get_logger(__name__, testing_mode=False)


def generate_submission_package(
    postgres_connection_url, submission_package_dir, seed_file, generator_list
):
    if "all" in generator_list:
        logger.info("Generating all manifests in submission packet")
        generator_list = all_generator_list
    else:
        logger.info(
            "Generating specified manifests in submission packet: "
            + str(generator_list)
        )

    logger.info("Reading seed file")
    file_sample_participant_map = pd.read_csv(seed_file)
    participant_list = (
        file_sample_participant_map["participant_id"]
        .drop_duplicates()
        .to_list()
    )
    sample_list = (
        file_sample_participant_map["sample_id"].drop_duplicates().to_list()
    )
    file_list = (
        file_sample_participant_map["file_id"].drop_duplicates().to_list()
    )
    if "participant" in generator_list:
        build_participant_table(
            postgres_connection_url, participant_list, submission_package_dir
        )
    if "sample" in generator_list:
        build_sample_table(
            postgres_connection_url, sample_list, submission_package_dir
        )
    if "diagnosis" in generator_list:
        build_diagnosis_table(
            postgres_connection_url, sample_list, submission_package_dir
        )
    if "file" in generator_list:
        build_file_table(
            postgres_connection_url, file_list, submission_package_dir
        )
