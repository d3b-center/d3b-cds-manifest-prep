from os import listdir

from d3b_cavatica_tools.utils.logging import get_logger

import openpyxl as opxl
import pandas as pd

logger = get_logger(__name__, testing_mode=False)


def write_to_excel(
    submission_package_dir,
    submission_template_file=None,
    submission_template_dict=None,
    output_dict=None,
    output_table=None,
):
    if not submission_template_file and not submission_template_dict:
        raise TypeError(
            "Must provide either `submission_template_file` or "
            "submission_template_dict"
        )
    breakpoint()
    pre_built_files = listdir(submission_package_dir)
    with pd.ExcelWriter(
        f"{submission_package_dir}/cbtn_ccdi_clinical_data.xlsx"
    ) as writer:
        for table_name, table in submission_template_dict.items():
            logger.info(f"saving {table_name}")
            if table_name in output_dict:
                output_dict[table_name].output_table.to_excel(
                    writer, sheet_name=table_name, index=False
                )
            elif table_name + ".csv" in pre_built_files:
                pd.read_csv(
                    submission_package_dir + "/" + table_name + ".csv"
                ).to_excel(writer, sheet_name=table_name, index=False)
            else:
                table.to_excel(writer, sheet_name=table_name, index=False)
