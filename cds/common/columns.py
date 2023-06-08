from d3b_cavatica_tools.utils.logging import get_logger

logger = get_logger(__name__, testing_mode=False)


def order_columns(manifest, manifest_name, submission_template_dict):
    """order columns in the manifest

    :param manifest: The manifest to order columns for
    :type manifest: pandas.DataFrame
    :return: The manifest with columns needed in the correct order
    :rtype: pandas.DataFrame
    """
    columns = submission_template_dict[manifest_name].columns
    return manifest[columns]


def build_table_from_template(
    manifest_name, submission_template_dict, submission_package_dir
):
    """Build a blank manifest from template

    Builds empty manifest based on the template and save it

    :param manifest_name: name of the manifest
    :type manifest_name: str
    :param submission_template_dict: contents of the submission template
    :type: dict
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :return: blank manifest
    :rtype: pandas.DataFrame
    """
    output_table = submission_template_dict[manifest_name]
    # Set the column order and sort on key column
    output_table = order_columns(
        output_table, manifest_name, submission_template_dict
    )
    logger.info(f"saving {manifest_name} manifest to file")
    output_table.to_csv(
        f"{submission_package_dir}/{manifest_name}.csv", index=False
    )
    return output_table
