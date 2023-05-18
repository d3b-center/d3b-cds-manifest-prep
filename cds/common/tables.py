from d3b_cavatica_tools.utils.logging import get_logger

import pandas as pd


def extract_parent_from_template(template_df):
    """"""
    parent_col = template_df.columns[1]
    return parent_col.partition(".")[0] if "." in parent_col else None


class OutputTable(object):
    """Table of information for submission to CDS"""

    def __init__(
        self,
        name,
        key_column=None,
        parent=None,
        template_df=None,
        submission_template_dict=None,
    ):
        self.name = name
        self.template_df = template_df or submission_template_dict[self.name]
        self.parent = parent or extract_parent_from_template(self.template_df)
        self.key_column = (
            key_column or self.template_df.columns[2]
            if self.parent is not None
            else self.template_df.columns[1]
        )
        self.logger = get_logger(__name__, testing_mode=False)

    def template_is_output(self):
        self.output_table = self.template_df

    def save_table(self, submission_package_dir, **kwargs):
        output_location = f"{submission_package_dir}/{self.name}.csv"
        self.output_table.to_csv(output_location, index=False, **kwargs)
        self.logger.info(f"Saved {self.name} manifest to {output_location}")
