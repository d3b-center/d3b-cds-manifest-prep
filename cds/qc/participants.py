from d3b_cavatica_tools.utils.logging import get_logger

from cds.qc.report import report_qc_result

from tqdm import tqdm

logger = get_logger(__name__, testing_mode=False)


def qc_participants(
    participant_list,
    mapping_participant_list,
    diagnosis_participant_list,
    sample_participant_list,
):
    """QC Relations between participants

    Test that, for every item in all input lists, each item is in all lists.

    :param participant_list: list of participants in the participant manifest
    :type participant_list: list
    :param mapping_participant_list: list of participants in the mapping file
    :type mapping_participant_list: list
    :param diagnosis_participant_list: list of participants in the diagnosis
    manifest
    :type diagnosis_participant_list: list
    :param sample_participant_list: list of participants in the sample manifest
    :type sample_participant_list: list
    :return: Information about item membership in each input list. Dict of dicts
    where each key is an identifier and the value is a dict describing if the
    item is in each input list.
    :rtype: dict
    """
    all_participants = list(
        set(
            participant_list
            + mapping_participant_list
            + diagnosis_participant_list
            + sample_participant_list
        )
    )
    result_dict = {
        i: {
            "in_participant_manifest": None,
            "in_diagnosis_manifest": None,
            "in_sample_manifest": None,
            "in_mapping": None,
        }
        for i in all_participants
    }
    logger.info("Checking that all participants are in all manifests")
    not_in_participant = []
    not_in_diagnosis = []
    not_in_sample = []
    not_in_mapping = []
    for participant in tqdm(all_participants):
        in_participant_manifest = participant in participant_list
        in_diagnosis = participant in diagnosis_participant_list
        in_sample = participant in sample_participant_list
        in_mapping = participant in mapping_participant_list
        result_dict[participant][
            "in_participant_manifest"
        ] = in_participant_manifest
        result_dict[participant]["in_diagnosis_manifest"] = in_diagnosis
        result_dict[participant]["in_sample_manifest"] = in_sample
        result_dict[participant]["in_mapping"] = in_mapping
        if not in_participant_manifest:
            not_in_participant.append(participant)
            logger.debug(f"{participant} not in participant_list")
        if not in_diagnosis:
            not_in_diagnosis.append(participant)
            logger.debug(f"{participant} not in diagnosis_participant_list")
        if not in_sample:
            not_in_sample.append(participant)
            logger.debug(f"{participant} not in sample_participant_list")
        if not in_mapping:
            not_in_mapping.append(participant)
            logger.debug(f"{participant} not in mapping")
    report_qc_result(
        len(not_in_participant), "participants", "participant_manifest"
    )
    report_qc_result(
        len(not_in_diagnosis), "participants", "diagnosis_manifest"
    )
    report_qc_result(len(not_in_sample), "participants", "sample_manifest")
    report_qc_result(len(not_in_mapping), "participants", "mapping")
    return result_dict
