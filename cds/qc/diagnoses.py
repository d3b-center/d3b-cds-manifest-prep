from d3b_cavatica_tools.utils.logging import get_logger

from cds.qc.report import report_qc_result

from tqdm import tqdm

logger = get_logger(__name__, testing_mode=False)


def qc_diagnoses(diagnosis_list, diagnosis_sample_map_diagnosis_list):
    """QC Relations between diagnoses

    :param diagnosis_list: list of diagnoses in the diagnosis_manifest
    :type diagnosis_list: list
    :param diagnosis_sample_map_diagnosis_list: list of diagnoses in the
    diagnosis-sample mapping
    :type diagnosis_sample_map_diagnosis_list: list
    :return: Information about item membership in each input list. Dict of dicts
    where each key is an identifier and the value is a dict describing if the
    item is in each input list.
    :rtype: dict
    """
    all_diagnoses = list(
        set(diagnosis_list + diagnosis_sample_map_diagnosis_list)
    )
    result_dict = {
        i: {
            "in_diagnosis_manifest": None,
            "in_diagnosis_sample_manifest": None,
        }
        for i in all_diagnoses
    }
    logger.info("Checking that all diagnoses are in all manifests")
    not_in_diagnosis = []
    not_in_diagnosis_sample_map = []
    for diagnosis in tqdm(all_diagnoses):
        in_diagnosis = diagnosis in diagnosis_list
        in_diagnosis_sample_map = (
            diagnosis in diagnosis_sample_map_diagnosis_list
        )
        result_dict[diagnosis][in_diagnosis] = in_diagnosis
        result_dict[diagnosis][
            in_diagnosis_sample_map
        ] = in_diagnosis_sample_map
        if not in_diagnosis:
            not_in_diagnosis.append(diagnosis)
            logger.debug(f"{diagnosis} not in diagnosis_list")
        if not in_diagnosis_sample_map:
            not_in_diagnosis_sample_map.append(diagnosis)
            logger.debug(f"{diagnosis} not in diagnosis-sample mapping")
    report_qc_result(len(not_in_diagnosis), "diagnoses", "diagnosis_manifest")
    report_qc_result(
        len(not_in_diagnosis_sample_map),
        "diagnoses",
        "diagnosis_sample_mapping",
    )
    return result_dict
