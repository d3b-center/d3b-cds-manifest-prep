from d3b_cavatica_tools.utils.logging import get_logger

from cds.qc.report import report_qc_result

from tqdm import tqdm

logger = get_logger(__name__, testing_mode=False)


def qc_samples(
    sample_list,
    mapping_sample_list,
    genomic_info_sample_list,
    diagnosis_sample_map_sample_list,
):
    """QC Relations between samples

    Test that, for every item in all input lists, each item is in all lists.

    :param sample_list: list of samples in the sample manifest
    :type sample_list: list
    :param mapping_sample_list: list of samples in the file-sample-participant
    mapping
    :type mapping_sample_list: list
    :param genomic_info_sample_list: list of samples in the genomic_info
    manifest
    :type genomic_info_sample_list: list
    :param diagnosis_sample_map_sample_list: list of samples in the diagnosis-
    sample mapping
    :type diagnosis_sample_map_sample_list: list
    :return: Information about item membership in each input list. Dict of dicts
    where each key is an identifier and the value is a dict describing if the
    item is in each input list.
    :rtype: dict
    """
    all_samples = list(
        set(
            sample_list
            + mapping_sample_list
            + genomic_info_sample_list
            + diagnosis_sample_map_sample_list,
        )
    )
    result_dict = {
        i: {
            "in_sample_manifest": None,
            "in_genomic_info": None,
            "in_mapping": None,
        }
        for i in all_samples
    }
    logger.info("Checking that all samples are in all manifests")
    not_in_sample = []
    not_in_genomic_info = []
    not_in_mapping = []
    not_in_diagnosis_sample_map = []
    for sample in tqdm(all_samples):
        in_sample_manifest = sample in sample_list
        in_genomic_info = sample in genomic_info_sample_list
        in_mapping = sample in mapping_sample_list
        in_diagnosis_sample_map = sample in diagnosis_sample_map_sample_list
        result_dict[sample]["in_sample_manifest"] = in_sample_manifest
        result_dict[sample]["in_genomic_info"] = in_genomic_info
        result_dict[sample]["in_mapping"] = in_mapping
        result_dict[sample]["in_diagnosis_sample_map"] = in_diagnosis_sample_map
        if not in_sample_manifest:
            not_in_sample.append(sample)
            logger.debug(f"{sample} not in sample_list")
        if not in_genomic_info:
            not_in_genomic_info.append(sample)
            logger.debug(f"{sample} not in genomic_info")
        if not in_mapping:
            not_in_mapping.append(sample)
            logger.debug(f"{sample} not in mapping")
        if not in_diagnosis_sample_map:
            not_in_diagnosis_sample_map.append(sample)
            logger.debug(f"{sample} not in diagnosis-sample mapping")
    report_qc_result(len(not_in_sample), "samples", "sample_manifest")
    report_qc_result(
        len(not_in_genomic_info), "samples", "genomic_info_manifest"
    )
    report_qc_result(len(not_in_mapping), "samples", "mapping")
    report_qc_result(
        len(not_in_diagnosis_sample_map), "samples", "diagnosis_sample_mapping"
    )
    return result_dict


def qc_diagnosis_samples(diagnosis_sample_map_sample_list, sample_manifest):
    tumor_samples = sample_manifest[
        sample_manifest["sample_tumor_status"] == "tumor"
    ]["sample_id"].to_list()
    all_samples = list(set(diagnosis_sample_map_sample_list + tumor_samples))

    result_dict = {
        i: {
            "in_tumor_list": None,
            "in_diagnosis_sample_map": None,
        }
        for i in all_samples
    }
    logger.info(
        "Checking that only & all tumor samples are in the diagnosis sample map"
    )
    not_in_diagnosis_sample_map = []
    not_in_tumor_list = []
    for sample in tqdm(all_samples):
        in_tumor_list = sample in tumor_samples
        in_diagnosis_sample_map = sample in diagnosis_sample_map_sample_list
        result_dict[sample]["in_tumor_list"] = in_tumor_list
        result_dict[sample]["in_diagnosis_sample_map"] = in_diagnosis_sample_map
        if not in_tumor_list:
            not_in_tumor_list.append(sample)
            logger.debug(f"{sample} not in tumor list")
        if not in_diagnosis_sample_map:
            not_in_diagnosis_sample_map.append(sample)
            logger.debug(f"{sample} not in diagnosis-sample mapping")
    report_qc_result(len(not_in_tumor_list), "samples", "list of tumors")
    report_qc_result(
        len(not_in_diagnosis_sample_map),
        "tumor samples",
        "diagnosis_sample_mapping",
    )
    return result_dict
