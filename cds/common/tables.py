from d3b_cavatica_tools.utils.logging import get_logger


def extract_parent_from_template(template_df, submission_template_dict):
    """Extract parent concept from template

    :param template_df: template data to extract from
    :type template_df: pandas.DataFrame
    :param submission_template_dict: collection of all the possible
    templates that this table could be, defaults to None
    :type submission_template_dict: dict, optional
    :return: parent concept name
    :rtype: str
    """
    parent_col = template_df.columns[1]
    parent_name = parent_col.partition(".")[0] if "." in parent_col else None
    if parent_name:
        return OutputTable(
            parent_name, submission_template_dict=submission_template_dict
        )
    else:
        return None


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
        self.parent = (
            parent
            or extract_parent_from_template(
                self.template_df, submission_template_dict
            ),
        )[0]

        if key_column is None:
            self.logger.debug(
                "key_column not supplied, deriving from template_df"
            )
        self.key_column = (
            key_column or self.template_df.columns[2]
            if self.parent is not None
            else self.template_df.columns[1]
        )

    def order_columns(self, df):
        """Order columns and sort values

        Using the column order specified in the template_df, order the columns
        in df. Then sort the values in that data frame based on the key_column
        of the table and the table's parent key column

        :param df: table to order the columns in
        :type df: pandas.DataFrame
        :return: table with ordered columns and sorted values
        :rtype: pandas.DataFrame
        """
        key_columns = [
            f"{self.parent.name}.{self.parent.key_column}",
            self.key_column,
        ]
        # confirm that all the expected columns are in the output table
        for column_name in self.template_df.columns:
            if column_name not in df.columns:
                df[column_name] = None
        return df[self.template_df.columns].sort_values(key_columns)

    def build_output(self, build_func=None, use_template=False, **kwargs):
        """Build the output table

        Build the output table using the supplied build_func or using the
        template. Use the template if the output table should be empty. Output
        of the build_func will have columns ordered and values sorted.

        :param build_func: function to use to build the output table. Must
        return a dataframe, defaults to None
        :type build_func: function, optional
        :param use_template: should the template_df be used as the output table?
        defaults to False
        :type use_template: bool, optional
        """
        if use_template:
            self.output_table = self.template_df
        else:
            self.output_table = self.order_columns(build_func(self, **kwargs))

    def save_table(self, submission_package_dir, **kwargs):
        """Save the output table to a file with other files in a submission
        package

        :param submission_package_dir: directory to save files
        :type submission_package_dir: str
        """
        output_location = f"{submission_package_dir}/{self.name}.csv"
        self.output_table.to_csv(output_location, index=False, **kwargs)
        self.logger.info(f"Saved {self.name} manifest to {output_location}")
