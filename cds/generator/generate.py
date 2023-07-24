import sys
import warnings
from os import listdir

from d3b_cavatica_tools.utils.logging import get_logger

from cds.common.constants import all_generator_list
from cds.common.tables import OutputTable
from cds.generator.file.sequencing_file import build_sequencing_file_table
from cds.generator.participant.family_relationship import (
    build_family_relationship_table,
)
from cds.generator.participant.participant import build_participant_table
from cds.generator.sample.sample import build_sample_table
from cds.generator.sample.sample_diagnosis import build_sample_diagnosis_table
from cds.generator.study.study import build_study_table
from cds.generator.study.study_admin import build_study_admin_table

import pandas as pd

logger = get_logger(__name__, testing_mode=False)


def generate_submission_package(
    postgres_connection_url,
    submission_package_dir,
    seed_file,
    generator_list,
    submission_template_file,
    cache_dir,
    build_excel_output=False,
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
    # ignore user warnings b/c excel had data validation tools not implimented
    # by openpyxl
    warnings.simplefilter(action="ignore", category=UserWarning)
    submission_template_dict = pd.read_excel(
        submission_template_file, sheet_name=None
    )
    warnings.resetwarnings()
    participant_list = (
        file_sample_participant_map["participant_id"]
        .drop_duplicates()
        .to_list()
    )
    sample_list = (
        file_sample_participant_map["sample_id"].drop_duplicates().to_list()
    )
    # Save the seed file
    file_sample_participant_map.to_csv(
        f"{submission_package_dir}/file_sample_participant_map.csv", index=False
    )
    # Build a dict to store all the output tables
    output_dict = {
        table_name: OutputTable(
            table_name, submission_template_dict=submission_template_dict
        )
        for table_name in generator_list
    }

    # tables that are returned empty
    not_implemented_tables = [
        "study_arm",
        "study_funding",
        "study_personnel",
        "publication",
        "diagnosis",
        "therapeutic_procedure",
        "medical_history",
        "exposure",
        "follow_up",
        "molecular_test",
        "pdx",
        "clinical_measure_file",
        "methylation_array_file",
        "imaging_file",
        "single_cell_sequencing_file",
        "synonym",
    ]
    for table_name in generator_list:
        output_dict[table_name].logger.info(
            f"Beginning to build {table_name} table"
        )
        if table_name in not_implemented_tables:
            logger.info(
                f"method for {table_name} not implimented, using template"
            )
            output_dict[table_name].build_output(use_template=True)
        elif table_name == "participant":
            output_dict[table_name].build_output(
                build_participant_table,
                db_url=postgres_connection_url,
                participant_list=participant_list,
            )
        elif table_name == "family_relationship":
            output_dict[table_name].build_output(
                build_family_relationship_table,
                db_url=postgres_connection_url,
                participant_list=participant_list,
            )
        elif table_name == "sample":
            output_dict[table_name].build_output(
                build_sample_table,
                db_url=postgres_connection_url,
                sample_list=sample_list,
            )
        elif table_name == "sample_diagnosis":
            dictionary = submission_template_dict["Terms and Value Sets"]
            icd_o_dictionary = dictionary[
                dictionary["Value Set Name"] == "diagnosis_icd_o"
            ]
            output_dict[table_name].build_output(
                build_sample_diagnosis_table,
                sample_list=sample_list,
                icd_o_dictionary=icd_o_dictionary,
            )
        elif table_name == "sequencing_file":
            output_dict[table_name].build_output(
                build_sequencing_file_table,
                db_url=postgres_connection_url,
                file_sample_participant_map=file_sample_participant_map,
                submission_template_dict=submission_template_dict,
                cache_dir=cache_dir,
            )
        elif table_name == "study":
            output_dict[table_name].build_output(
                build_study_table,
                db_url=postgres_connection_url,
                participant_list=participant_list,
            )
        elif table_name == "study_admin":
            output_dict[table_name].build_output(
                build_study_admin_table,
                db_url=postgres_connection_url,
                participant_list=participant_list,
            )
        else:
            output_dict[table_name].logger.error(
                f"Table {table_name} has no method"
            )
            sys.exit()
        output_dict[table_name].save_table(submission_package_dir)
    # Write the output to excel
    # For every table, if it has just been constructed, use that. Otherwise,
    # if the table exists in the submission package directory, use that
    # pre-built version of the manifest. If the manifest doesn't exist in the
    # submission package directory, use the template version of the file.
    if build_excel_output:
        logger.info("saving files into one excel file")
        pre_built_files = listdir(submission_package_dir)
        with pd.ExcelWriter(
            f"{submission_package_dir}/cbtn_ccdi_clinical_data.xlsx"
        ) as writer:
            for table_name, table in submission_template_dict.items():
                logger.info(f"saving {table_name}")
                if table_name in output_dict:
                    output_dict[table_name].output_table.to_excel(
                        writer, sheet_name=table_name
                    )
                elif table_name + ".csv" in pre_built_files:
                    pd.read_csv(
                        submission_package_dir + "/" + table_name + ".csv"
                    ).to_excel(writer, sheet_name=table_name)
                else:
                    table.to_excel(writer, sheet_name=table_name)
