import pandas as pd
import psycopg2


def sample_type(output_table, row):
    tissue = row["sample_type"]
    status = row["sample_tumor_status"]
    if status == "tumor":
        if tissue == "Derived Cell Line":
            return "Cell"
        elif tissue == "Solid Tissue":
            return "Tissue"
        elif tissue == "Tumor":
            return "Tumor"
        else:
            output_table.logger.error(
                f"Type unknown. Tissue: {tissue}, Status: {status}"
            )
    elif status == "normal":
        if tissue == "Solid Tissue":
            return "Tissue"
        elif tissue == "Peripheral Whole Blood":
            return "Blood Derived"
        elif tissue == "Saliva":
            return "Saliva"
        elif tissue == "Not Reported":
            return "Unspecified"


status_map = {
    "Tumor": "Tumor",
    "Normal": "Normal",
    # is non-tumor normal? or is it abnormal?
    "Non-Tumor": "Normal",
}

# If an anatomical site is not in this list, use "Brain, NOS"
anatomical_site_map = {
    "Brain Stem- Midbrain/Tectum": "Brain stem",
    "Brain Stem- Midbrain/Tectum; Brain Stem- Pons": "Brain stem",
    "Brain Stem- Pons": "Brain stem",
    "Brain Stem-Medulla": "Brain stem",
    "Brain Stem-Medulla; Brain Stem- Midbrain/Tectum": "Brain stem",
    "Brain Stem-Medulla; Brain Stem- Midbrain/Tectum; Brain Stem- Pons": "Brain stem",
    "Brain Stem-Medulla; Brain Stem- Midbrain/Tectum; Brain Stem- Pons; Cerebellum/Posterior Fossa": "Brain stem",
    "Brain Stem-Medulla; Brain Stem- Pons": "Brain stem",
    "Brainstem": "Brain stem",
    "Cerebellum/Posterior Fossa": "Cerebellum, NOS",
    "Cranial Nerves NOS": "Cranial nerve, NOS",
    "Frontal Lobe": "Frontal lobe",
    "Intramedullary Spinal Cord Mass along C5-T1": "Spinal cord",
    "Meninges/Dura": "Cerebral meninges",
    "Not Applicable": "Not Available",
    "Occipital Lobe": "Occipital lobe",
    "Parietal Lobe": "Parietal lobe",
    "Peripheral Whole Blood": "Blood",
    "Pons/Brainstem": "Brain stem",
    "Right Frontal": "Frontal lobe",
    "Right Parietal Lobe": "Parietal lobe",
    "Right Temporal Lobe": "Temporal lobe",
    "Right frontal lobe": "Frontal lobe",
    "Skull": "Bones of skull and face and associated joints",
    "Spinal Cord- Cervical": "Spinal cord",
    "Spinal Cord- Cervical; Spinal Cord- Lumbar/Thecal Sac; Spinal Cord- Thoracic": "Spinal cord",
    "Spinal Cord- Cervical; Spinal Cord- Lumbar/Thecal Sac; Spinal Cord- Thoracic; Suprasellar/Hypothalamic/Pituitary": "Spinal cord",
    "Spinal Cord- Cervical; Spinal Cord- Thoracic": "Spinal cord",
    "Spinal Cord- Lumbar/Thecal Sac": "Spinal cord",
    "Spinal Cord- Lumbar/Thecal Sac; Spinal Cord- Thoracic": "Spinal cord",
    "Spinal Cord- Lumbar/Thecal Sac; Spinal Cord- Thoracic; Ventricles": "Spinal cord",
    "Spinal Cord- Thoracic": "Spinal cord",
    "Spinal Cord- Thoracic; Spine NOS": "Spinal cord",
    "Spine": "Spinal cord",
    "Spine NOS": "Spinal cord",
    "Temporal Lobe": "Temporal lobe",
    "Ventricles": "Ventricle, NOS",
    "cerebellum": "Cerebellum, NOS",
    "left occipital lobe": "Occipital lobe",
    "left parietal lobe": "Parietal lobe",
    "left temporal lobe": "Temporal lobe",
    "right anterior temporal lobe": "Temporal lobe",
    "right cortex": "Cerebrum",
    "right frontal": "Frontal lobe",
    "right frontal lobe": "Frontal lobe",
    "right medial frontal lobe": "",
    "right occipital": "Occipital lobe",
    "right temporal lobe": "Temporal lobe",
    "spinal cord": "Spinal cord",
    "temporal lobe": "Temporal lobe",
}


def sample_query(sample_list):
    query = f"""
    select pt.study_id,
           bs.kf_id as sample_id,
           composition as sample_type,
           bs.participant_id as participant_id,
           source_text_tissue_type as sample_tumor_status,
           source_text_anatomical_site as anatomic_site,
           age_at_event_days as participant_age_at_collection,
           bs.external_aliquot_id as alternate_sample_id
    from biospecimen bs
    join participant pt on pt.kf_id = bs.participant_id
    where bs.kf_id in ({str(sample_list)[1:-1]})
    """
    return query


def build_sample_table(output_table, db_url, sample_list):
    """Build the sample table

    Build the sample manifest

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param sample_list: sample IDs to have in the sample manifest
    :type sample_list: list
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :return: sample table
    :rtype: pandas.DataFrame
    """
    output_table.logger.info("Building sample table")
    output_table.logger.info("connecting to database")
    conn = psycopg2.connect(db_url)
    output_table.logger.info("Querying for manifest of samples")
    sample_table = pd.read_sql(sample_query(sample_list), conn)
    conn.close()
    output_table.logger.info("Converting KF enums to CDS enums")
    sample_table["type"] = output_table.name
    sample_table["sample_description"] = ""
    sample_table["sample_tumor_status"] = sample_table[
        "sample_tumor_status"
    ].apply(lambda x: status_map.get(x))
    # Default anatomical site to "Brain NOS"
    sample_table["anatomic_site"] = sample_table["sample_tumor_status"].apply(
        lambda x: anatomical_site_map.get(x, "Brain, NOS")
    )
    sample_table["sample_type"] = sample_table.apply(
        lambda row: sample_type(output_table, row), axis=1
    )
    # Set the column order and sort on key column
    sample_table = sample_table.rename(
        columns={"participant_id": "participant.participant_id"}
    )
    return sample_table
