def diagnosis_query(sample_list, sample_tissue_type=None):
    query = f"""
    select distinct bs.participant_id, bs.kf_id as sample_id,
    dx.source_text_diagnosis as primary_diagnosis
from biospecimen bs
    join biospecimen_diagnosis bsdx on bs.kf_id = bsdx.biospecimen_id
    join diagnosis dx on bsdx.diagnosis_id = dx.kf_id
where bs.kf_id in ({str(sample_list)[1:-1]})
    """
    if sample_tissue_type:
        query = (
            query + f"and bs.source_text_tissue_type = '{sample_tissue_type}'"
        )
    return query


def histologies_query():
    """build query for getting contents of histologies table

    :return: query that gets contents of the histologies table
    :rtype: str
    """
    query = """
    SELECT *
    FROM prod_reporting.openpedcan_histologies
    ORDER BY "Kids_First_Biospecimen_ID"
    """
    return query
