from cds.common.queries import participant_query

import pandas as pd
import psycopg2

gender_map = {
    "Female": "Female",
    "Male": "Male",
    "Not Reported": "Not Reported",
    "Not Available": "Not reported",
}
race_map = {
    "White": "White",
    "Black or African American": "Black or African American",
    "Native Hawaiian or Other Pacific Islander": "Native Hawaiian or other Pacific Islander",  # noqa
    "Asian": "Asian",
    "American Indian or Alaska Native": "American Indian or Alaska Native",
    "Reported Unknown": "Unknown",
    "Not Available": "Not reported",
    # may cause a validation issue b/c "Other" is not in the race value set,
    # but "Other, Specify" is in the value set:
    # https://cde.nlm.nih.gov/deView?tinyId=mygNSAd6TL
    "More Than One Race": "Other",
    "Other": "Other",
    "Not Reported": "Not Reported",
}
ethnicity_map = {
    "Not Hispanic or Latino": "Not Hispanic or Latino",
    "Hispanic or Latino": "Hispanic or Latino",
    "Reported Unknown": "Unknown",
    "Not Available": "Not reported",
    "Not Reported": "Not reported",
}


def build_participant_table(output_table, db_url, participant_list):
    """Build the participant table

    Build the participant manifest

    :param db_url: database url to KF prd postgres
    :type db_url: str
    :param participant_list: participant IDs to have in the participant manifest
    :type participant_list: list
    :param submission_package_dir: directory to save the output manifest
    :type submission_package_dir: str
    :return: participant table
    :rtype: pandas.DataFrame
    """
    output_table.logger.info("Building participant table")
    output_table.logger.info("connecting to database")
    conn = psycopg2.connect(db_url)

    output_table.logger.info("Querying for manifest of participants")
    participant_table = pd.read_sql(participant_query(participant_list), conn)
    output_table.logger.info("Converting KF enums to CDS enums")

    # rename the study ID column
    participant_table = participant_table.rename(
        columns={"study_id": "study.study_id"}
    )

    participant_table["type"] = "participant"
    participant_table["gender"] = participant_table["gender"].apply(
        lambda x: gender_map.get(x)
    )
    participant_table["race"] = participant_table["race"].apply(
        lambda x: race_map.get(x)
    )
    participant_table["ethnicity"] = participant_table["ethnicity"].apply(
        lambda x: ethnicity_map.get(x)
    )
    breakpoint()
    # Set the column order and sort on key column
    participant_table = order_columns(participant_table).sort_values(
        "participant_id"
    )
    output_table.logger.info("saving sample manifest to file")
    participant_table.to_csv(
        f"{submission_package_dir}/participant.csv", index=False
    )
    return participant_table
