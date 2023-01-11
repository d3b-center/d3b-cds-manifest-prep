from d3b_cavatica_tools.utils.logging import get_logger

logger = get_logger(__name__, testing_mode=False)


def report_qc_result(error_count, entity, item_name):
    """Report result of a QC step to the logger. Escelate log level if QC
    indicated an error.

    :param error_count: Count of errors found in a QC step
    :type error_count: int
    :param entity: Name of things being QC'd
    :type entity: str
    :param item_name: Name of manifest being QC'd against
    :type item_name: str
    """
    message = f"{str(error_count)} {entity} are not in {item_name}"
    if error_count > 0:
        logger.error(message)
    else:
        logger.info(message)
