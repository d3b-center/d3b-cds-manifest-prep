from d3b_cavatica_tools.utils.logging import get_logger

logger = get_logger(__name__, testing_mode=False)


status_map = {"Tumor": "tumor", "Normal": "normal", "Non-Tumor": "normal"}


def sample_type(row):
    tissue = row["sample_type"]
    status = row["sample_tumor_status"]
    if status == "tumor":
        if tissue == "Derived Cell Line":
            return "Cell Lines"
        elif tissue == "Solid Tissue":
            return "Tumor"
        elif tissue == "Tumor":
            return "Tumor"
        else:
            logger.error(f"Type unknown. Tissue: {tissue}, Status: {status}")
    elif status == "normal":
        if tissue == "Solid Tissue":
            return "Solid Tissue Normal"
        elif tissue == "Peripheral Whole Blood":
            return "Blood Derived Normal"
        elif tissue == "Saliva":
            return "Saliva"
        elif tissue == "Not Reported":
            return "Not Reported"


# map tumor status to anatomical site
anatomical_site_map = {
    "tumor": "Central Nervous System",
    "normal": "Not Reported",
}
