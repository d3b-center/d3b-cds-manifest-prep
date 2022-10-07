"""
SQL queries used to build manifests of things in the CCDI Genomics Bucket
"""
pnoc_sql = """
    SELECT aws_scrape.s3path,
           hashes.md5,
           gf.kf_id as gf_id,
           gf.is_harmonized,
           gf.file_format,
           gf.data_type,
           gf.controlled_access,
           gf.reference_genome,
           pt.external_id as external_participant_id,
           pt.kf_id as pt_id,
           bs.external_aliquot_id,
           bs.external_sample_id,
           bs.kf_id as bs_id
     from file_metadata.aws_scrape aws_scrape
left JOIN file_metadata.hashes hashes
       ON aws_scrape.s3path = hashes.s3path
left join file_metadata.indexd_scrape idx_scrape
       on idx_scrape.url = aws_scrape.s3path
left join genomic_file gf
       on gf.latest_did = idx_scrape.did
left join biospecimen_genomic_file bg
       on gf.kf_id = bg.genomic_file_id
left join biospecimen bs
       on bs.kf_id = bg.biospecimen_id
left join participant pt
       on pt.kf_id = bs.participant_id
    where aws_scrape.bucket in (
            'kf-study-us-east-1-prd-sd-8y99qzjj',
            'kf-study-us-east-1-prd-sd-m3dbxd12'
          )
          and aws_scrape.s3path not like '%uploads/%'
    """

already_transferred_sql = """
with target_hashes as (
    select hashes.s3path as target_s3path,
        replace(
            hashes.s3path,
            'cds-246-phs002517-sequencefiles-p30-fy20/',
            ''
        ) source_s3path,
        md5 target_md5,
        sha256 target_sha256, 
        hash_timestamp target_hash_timestamp
    from file_metadata.hashes hashes
    where hashes.s3path like '%cds-246-phs002517-sequencefiles-p30-fy20%'
)
select target_hashes.target_s3path as s3path,
           source_hashes.md5,
           gf.kf_id as gf_id,
           gf.is_harmonized,
           gf.file_format,
           gf.data_type,
           gf.controlled_access,
           gf.reference_genome,
           pt.external_id as external_participant_id,
           pt.kf_id as pt_id,
           bs.external_aliquot_id,
           bs.external_sample_id,
           bs.kf_id as bs_id
from target_hashes
    left join file_metadata.hashes source_hashes
        on target_hashes.source_s3path = source_hashes.s3path
    left join file_metadata.aws_scrape s3scrape
        on target_hashes.source_s3path = s3scrape.s3path
    left join  file_metadata.indexd_scrape idx_scrape
        on s3scrape.s3path = idx_scrape.url
    left join genomic_file gf
        on idx_scrape.did = gf.latest_did
    left join biospecimen_genomic_file bsgf
        on gf.kf_id = bsgf.genomic_file_id
    left join biospecimen bs
        on bs.kf_id = bsgf.biospecimen_id
    left join participant pt
        on pt.kf_id = bs.participant_id
    """

all_scrapes_sql = """
select s3path, size
from file_metadata.aws_scrape
where bucket in (
        'kf-study-us-east-1-prd-sd-8y99qzjj',
        'kf-study-us-east-1-prd-sd-bhjxbdqk',
        'kf-study-us-east-1-prd-sd-m3dbxd12'
    )
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


def sequencing_query(sample_list):
    query = f"""
    select distinct bsgf.biospecimen_id,
        segf.sequencing_experiment_id,
        segf.genomic_file_id, 
        se.external_id,
        se.experiment_strategy,
        se.is_paired_end as se_paired_end,
        se.platform,
        se.instrument_model, 
        gf.reference_genome
    from biospecimen_genomic_file bsgf
        join genomic_file gf on gf.kf_id = bsgf.genomic_file_id
        join sequencing_experiment_genomic_file segf on gf.kf_id = segf.genomic_file_id
        join sequencing_experiment se on se.kf_id = segf.sequencing_experiment_id
    where bsgf.biospecimen_id in ({str(sample_list)[1:-1]})
    """
    return query


def diagnosis_query(participant_list):
    query = f"""
    select kf_id as diagnosis_id,
           external_id as primary_diagnosis,
           participant_id
    from diagnosis
    where participant_id in ({str(participant_list)[1:-1]})
          and source_text_diagnosis not in ('Other', 'Not Available', 'Not Reported', 'No tumor')
    """  # noqa
    return query


def sequencing_query2(sample_list):
    query = f"""
    select distinct bsgf.biospecimen_id,
        segf.sequencing_experiment_id,
        segf.genomic_file_id, 
        se.external_id,
        se.experiment_strategy,
        se.is_paired_end as se_paired_end,
        se.platform,
        se.instrument_model, 
        gf.reference_genome, 
        se.max_insert_size, 
        se.mean_insert_size, se.mean_depth, 
        se.total_reads, se.mean_read_length, se.sequencing_center_id, sc.name
    from biospecimen_genomic_file bsgf
        join genomic_file gf on gf.kf_id = bsgf.genomic_file_id
        join sequencing_experiment_genomic_file segf on gf.kf_id = segf.genomic_file_id
        join sequencing_experiment se on se.kf_id = segf.sequencing_experiment_id
        join sequencing_center sc on sc.kf_id = se.sequencing_center_id
    where bsgf.biospecimen_id in ({str(sample_list)[1:-1]})
    """
    return query
