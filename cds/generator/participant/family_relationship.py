import pandas as pd
import psycopg2


def family_relationship_query(participant_list):
    query = f"""
    select distinct
        fr.participant1_id as "participant.participant_id",
        fr.kf_id as family_relationship_id,
        fr.participant2_id as related_to_participant_id,
        fr.participant1_to_participant2_relation as relationship,
        pt.family_id
    from participant pt
    join family_relationship fr on pt.kf_id = fr.participant1_id
    where fr.participant1_id in ({str(participant_list)[1:-1]})
    or fr.participant2_id in ({str(participant_list)[1:-1]})
    """
    return query


relationship_map = {
    "Father": "Father",
    "Mother": "Mother",
    "Son": "Other",
    "Daughter": "Other",
}


def build_family_relationship_table(output_table, db_url, participant_list):
    output_table.logger.info("connecting to database")
    conn = psycopg2.connect(db_url)

    output_table.logger.info("Querying for manifest of family relationships")
    family_relationship_table = pd.read_sql(
        family_relationship_query(participant_list), conn
    )
    conn.close()
    output_table.logger.info("Converting KF enums to CDS enums")

    family_relationship_table["type"] = output_table.name
    family_relationship_table["relationship"] = family_relationship_table[
        "relationship"
    ].apply(lambda x: relationship_map.get(x))
    return family_relationship_table
