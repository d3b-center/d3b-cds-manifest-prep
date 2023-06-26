"""
SQL queries used to build manifests of things in the CCDI Genomics Bucket
"""


def file_kf_query(file_list):
    query = f"""
    select kf_id,
           file_format as file_type,
           controlled_access,
           url,
           reference_genome,
           is_harmonized
    from genomic_file gf
    join file_metadata.indexd_scrape idx on gf.latest_did = idx.did
    where kf_id in ({str(file_list)[1:-1]})
    """
    return query


def file_sequencing_experiment_query(file_list):
    query = f"""
    select --distinct
    sg.genomic_file_id as file_id,
        se.external_id as library_id,
        se.experiment_strategy as library_strategy,
        se.platform as platform,
        se.instrument_model as instrument_model,
        se.is_paired_end as library_layout,
        se.experiment_strategy as library_source,
        se.total_reads as number_of_reads,
        se.mean_read_length as avg_read_length,
        se.mean_depth as coverage,
        gf.reference_genome as reference_genome_assembly,
        se.library_selection as library_selection
    from genomic_file gf
    join sequencing_experiment_genomic_file sg on sg.genomic_file_id = gf.kf_id
    join sequencing_experiment se on sg.sequencing_experiment_id = se.kf_id
    where sg.genomic_file_id in ({str(file_list)[1:-1]})
    """
    return query


def bg_sequencing_experiment_query(file_id, biospecimen_id):
    query = f"""
    select distinct
    sg.genomic_file_id as file_id,
        bg.biospecimen_id as sample_id,
        se.external_id as library_id,
        se.experiment_strategy as library_strategy,
        se.platform as platform,
        se.instrument_model as instrument_model,
        se.is_paired_end as library_layout,
        se.experiment_strategy as library_source,
        se.total_reads as number_of_reads,
        se.mean_read_length as avg_read_length,
        se.mean_depth as coverage,
        gf.reference_genome as reference_genome_assembly,
        se.library_selection as library_selection
    from genomic_file gf
    join biospecimen_genomic_file bg on bg.genomic_file_id = gf.kf_id
    join sequencing_experiment_genomic_file sg on sg.genomic_file_id = gf.kf_id
    join sequencing_experiment se on sg.sequencing_experiment_id = se.kf_id
    where sg.genomic_file_id = '{file_id}'
    AND bg.biospecimen_id = '{biospecimen_id}'
    """
    return query


def file_bucket_query(bucket_name):
    query = f"""
    with files as (
        select aws.s3path as file_url_in_cds,
            aws.file_name,
            aws.size as file_size
        from file_metadata.aws_scrape aws
        where aws.bucket = '{bucket_name.rstrip("/")}'
    )
    select files.*,
        hashes.md5 md5sum
    from files
    join file_metadata.hashes hashes on hashes.s3path = files.file_url_in_cds
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
    join file_info on kf_info.url = file_info.file_url_in_cds
    """
    return query


def file_genome_query(file_list):
    query = f"""
        select kf_id as file_id, reference_genome, is_harmonized
        from genomic_file
        where kf_id in ({str(file_list)[1:-1]})
        """
    return query
