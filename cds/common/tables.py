from d3b_cavatica_tools.utils.logging import get_logger

import pandas as pd


def extract_parent_from_template(template_df):
    """Extract parent concept from template

    :param template_df: template data to extract from
    :type template_df: pandas.DataFrame
    :return: parent concept name
    :rtype: str
    """
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
        """Initialize an OutputTable

        Initialize an OutputTable. If template_df is unknown, infer it from
        name and submission_template_dict. If key column or parent are unknown,
        infer from template_df

        :param name: name of the table to create
        :type name: str
        :param key_column: key index column of the table. Will order the table
        on this column and parent key column, defaults to None
        :type key_column: str, optional
        :param parent: parent table name, defaults to None
        :type parent: str, optional
        :param template_df: template dataframe to base this table off of,
        defaults to None
        :type template_df: pandas.DataFrame, optional
        :param submission_template_dict: collection of all the possible
        templates that this table could be, defaults to None
        :type submission_template_dict: dict, optional
        """
        self.logger = get_logger(__name__, testing_mode=False)
        self.name = name
        if template_df is None:
            self.logger.debug(
                "template_df not supplied, deriving from"
                "submission_template_dict"
            )
            if submission_template_dict is None:
                self.logger.error(
                    "must supply submission_template_dict if template_df not"
                    "supplied"
                )
        self.template_df = template_df or submission_template_dict[self.name]
        if parent is None:
            self.logger.debug("parent not supplied, deriving from template_df")
        self.parent = parent or extract_parent_from_template(self.template_df)
        if key_column is None:
            self.logger.debug(
                "key_column not supplied, deriving from template_df"
            )
        self.key_column = (
            key_column or self.template_df.columns[2]
            if self.parent is not None
            else self.template_df.columns[1]
        )

    def template_is_output(self):
        """Set the output table to be the template_df

        Use this if the output table should be empty
        """
        self.output_table = self.template_df

    def save_table(self, submission_package_dir, **kwargs):
        """Save the output table to a file with other files in a submission
        package

        :param submission_package_dir: directory to save files
        :type submission_package_dir: str
        """
        output_location = f"{submission_package_dir}/{self.name}.csv"
        self.output_table.to_csv(output_location, index=False, **kwargs)
        self.logger.info(f"Saved {self.name} manifest to {output_location}")
