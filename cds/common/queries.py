"""
SQL queries used to build manifests of things in the CCDI Genomics Bucket
"""


def participant_query(participant_list):
    query = f"""
    select kf_id as participant_id, gender, race, ethnicity
    from participant
    where kf_id in ({str(participant_list)[1:-1]})
    """
    return query


def sample_query(sample_list):
    query = f"""
    select bs.kf_id as sample_id,
           composition as sample_type,
           bs.participant_id as participant_id,
           source_text_tissue_type as sample_tumor_status,
           source_text_anatomical_site as sample_anatomical_site,
           age_at_event_days as sample_age_at_collection
    from biospecimen bs
    join participant p on p.kf_id = bs.participant_id
    where bs.kf_id in ({str(sample_list)[1:-1]})
    """
    return query


def file_kf_query(file_list):
    query = f"""
    select kf_id,
           file_format as file_type,
           controlled_access,
           url
    from genomic_file gf
    join file_metadata.indexd_scrape idx on gf.latest_did = idx.did
    where kf_id in ({str(file_list)[1:-1]})
    """
    return query


def file_bucket_query(bucket_name):
    query = f"""
    with files as (
        select aws.s3path as file_url_in_cds,
            aws.file_name,
            aws.size as file_size,
            replace(
                aws.s3path,
                '{bucket_name}',
                ''
            ) kf_s3path
        from file_metadata.aws_scrape aws
        where aws.bucket = '{bucket_name.rstrip("/")}'
    )
    select files.*,
        hashes.md5 md5sum
    from files
    join file_metadata.hashes hashes on hashes.s3path = files.kf_s3path
    """
    return query


def file_query(file_list, bucket_name):
    query = f"""
    with file_info as ({file_bucket_query(bucket_name)}),
    kf_info as ({file_kf_query(file_list)})
    select kf_id as file_id,
        file_name,
        file_type,
        file_size,
        md5sum,
        file_url_in_cds,
        controlled_access
    from kf_info
    join file_info on kf_info.url = file_info.kf_s3path
    """
    return query


def file_genome_query(file_list):
    query = f"""
        select kf_id as file_id, reference_genome, is_harmonized
        from genomic_file
        where kf_id in ({str(file_list)[1:-1]})
        """
    return query


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
