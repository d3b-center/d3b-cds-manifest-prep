from cds.common.tables import OutputTable


def build_study_personnel_table(
    submission_package_dir, submission_template_dict
):
    """Build the study_personnel table

    Build the study_personnel manifest

    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :param submission_template_dict: template submission manifests
    :type submission_template_dict: dict
    :return: study_personnel table
    :rtype: OutputTable
    """
    table_object = OutputTable(
        "study_personnel", submission_template_dict=submission_template_dict
    )
    # Build the output table
    table_object.template_is_output()
    # Save the table
    table_object.save_table(submission_package_dir)
    return table_object
