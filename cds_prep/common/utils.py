"""
Tools related to building the manifest of files in the ccdi genomics bucket
"""
from itertools import chain

import pandas as pd
from d3b_cavatica_tools.utils.logging import get_logger
from kf_lib_data_ingest.common.constants import COMMON, GENOMIC_FILE
from kf_utils.dataservice.scrape import yield_entities_from_kfids
from tqdm import tqdm

logger = get_logger(__name__, testing_mode=False)

kf_api_url = "https://kf-api-dataservice.kidsfirstdrc.org"


def ret_list(series, unique_vals=True):
    """
    List the unique values in a column without None values. If there should be
    one unique value, returns the value as a list, otherwise returns a list of
    a list of the unique values

    :param series: column you want values from
    :type series: pd.Series
    :param unique_vals: should the colunm have only one unique value, defaults
                        to True
    :type unique_vals: bool, optional
    :return: list of values in the column
    :rtype: list
    """
    shortlist = {i for i in series.to_list() if i}
    if len(shortlist) == 0:
        shortlist = set([None])
    if unique_vals:
        if len(shortlist) > 1:
            logger.warning("more than one unique value in column")
            logger.warning("Attempting to remove KF N/A enums from list")
            shortlist = [
                i
                for i in shortlist
                if i
                not in [
                    COMMON.NOT_APPLICABLE,
                    COMMON.NOT_AVAILABLE,
                    COMMON.NOT_REPORTED,
                ]
            ]
            if set(shortlist) == {"hg19-viral", "hg19"}:
                # Handle a weird case where a file is registered as having these
                # two reference genomes. this should just be hg19 per Bo
                shortlist = ["hg19"]
            if len(shortlist) > 1:
                logger.error("list is still too long")
                logger.error(shortlist)
                logger.error("returning values as list")
                return list([shortlist])
        return list(shortlist)
    return [list(shortlist)]


def harmonized_path(x):
    """
    Test if a path is one that is harmonized

    :param x: file path (e.g. an s3path)
    :type x: str
    :return: does the path have "harmonized" in it
    :rtype: bool
    """
    if "harmonized" in x:
        return True
    else:
        return False


def isNaN(num):
    """
    Test if a thing is NaN

    :param num: Thing to test
    :return: Result of whether the thing is NaN
    :rtype: bool
    """
    return num != num


def build_unique_file_manifest(df):
    """
    For every file in the dataframe, produce a single row of the unique values
    for that value. This is so that files that have mulitple specimens,
    participants, etc, can be munged into one row.

    :param df: _description_
    :type df: _type_
    :return: _description_
    :rtype: _type_
    """
    dfl = []
    for f in tqdm(df["s3path"].unique()):
        rows = df[df["s3path"] == f]
        if len(rows) == 1:
            dfl.append(rows)
        else:
            try:
                unique_df = pd.DataFrame(
                    {
                        "s3path": f,
                        "md5": ret_list(rows["md5"], unique_vals=True),
                        "gf_id": ret_list(rows["gf_id"], unique_vals=False),
                        "is_harmonized": ret_list(
                            rows["is_harmonized"], unique_vals=True
                        ),
                        "file_format": ret_list(
                            rows["file_format"], unique_vals=True
                        ),
                        "data_type": ret_list(
                            rows["data_type"], unique_vals=True
                        ),
                        "controlled_access": ret_list(
                            rows["controlled_access"], unique_vals=True
                        ),
                        "reference_genome": ret_list(
                            rows["reference_genome"], unique_vals=True
                        ),
                        "external_participant_id": ret_list(
                            rows["external_participant_id"],
                            unique_vals=False,
                        ),
                        "pt_id": ret_list(rows["pt_id"], unique_vals=False),
                        "external_aliquot_id": ret_list(
                            rows["external_aliquot_id"], unique_vals=False
                        ),
                        "external_sample_id": ret_list(
                            rows["external_sample_id"], unique_vals=False
                        ),
                        "bs_id": ret_list(rows["bs_id"], unique_vals=False),
                    }
                )
                dfl.append(unique_df)
            except ValueError:
                logger.warning(f)
    return pd.concat(dfl)


def take_first(x):
    if not x:
        logger.warning(f"{type(None)} discovered")
        return None
    elif isinstance(x, str):
        return x
    elif isinstance(x, list):
        return x[0]
    else:
        logger.error(f"idk what {x} is but it's {type(x)}")


genome_reference_map = {
    "['hg19_with_GenBank_Viral_Genomes', 'GRCh38']": "hg19",
    "hg19": "hg19",
    "GRCh38": "GRCh38",
    "['RefSeq_Build_73', 'hg19', 'GRCh38']": "hg19",
    "None": "hg19",
    "Not Reported": "hg19",
    "GRCh37": "GRCh37",
    "['RefSeq_Build_73', 'GRCh38']": "hg19",
    "['hg19', 'GRCh38']": "hg19",
    "hg19_with_GenBank_Viral_Genomes": "hg19",
    "Not Applicable": "hg19",
}


def build_unique_genomic_manifest(df):
    """
    For every file in the dataframe, produce a single row of the unique values
    for that value. This is so that files that have mulitple specimens,
    participants, etc, can be munged into one row.

    :param df: _description_
    :type df: _type_
    :return: _description_
    :rtype: _type_
    """
    dfl = []
    for f in tqdm(df["biospecimen_id"].unique()):
        rows = df[df["biospecimen_id"] == f]
        if len(rows) == 1:
            dfl.append(rows)
        else:
            try:
                unique_df = pd.DataFrame(
                    {
                        "biospecimen_id": f,
                        "sequencing_experiment_id": ret_list(
                            rows["sequencing_experiment_id"], unique_vals=False
                        ),
                        "external_id": ret_list(
                            rows["external_id"], unique_vals=False
                        ),
                        "experiment_strategy": ret_list(
                            rows["experiment_strategy"], unique_vals=True
                        ),
                        "se_paired_end": ret_list(
                            rows["se_paired_end"], unique_vals=True
                        ),
                        "platform": ret_list(
                            rows["platform"], unique_vals=True
                        ),
                        "instrument_model": ret_list(
                            rows["instrument_model"], unique_vals=True
                        ),
                        "reference_genome": ret_list(
                            rows["reference_genome"], unique_vals=True
                        ),
                    }
                )
                unique_df["sequencing_experiment_id"] = unique_df[
                    "sequencing_experiment_id"
                ].apply(take_first)
                unique_df["external_id"] = unique_df["external_id"].apply(
                    take_first
                )
                unique_df["instrument_model"] = unique_df[
                    "instrument_model"
                ].apply(take_first)
                dfl.append(unique_df)
            except ValueError:
                logger.warning(f)
    return pd.concat(dfl)


def fetch_missing_samples(missing_samples):
    logger.info(
        "getting the specimen information for files missing specimen info"
    )
    return {
        e["kf_id"]: {
            "external_aliquot_id": e["external_aliquot_id"],
            "external_sample_id": e["external_sample_id"],
            "pt_id": e["_links"]["participant"].rpartition("/")[2],
        }
        for e in yield_entities_from_kfids(
            kf_api_url, list(missing_samples), show_progress=True
        )
    }


def fetch_missing_participants(missing_participants):
    logger.info(
        "getting the participant information for files missing participant info"
    )
    return {
        e["kf_id"]: {
            "external_participant_id": e["external_id"],
        }
        for e in yield_entities_from_kfids(
            kf_api_url, list(missing_participants), show_progress=True
        )
    }


def controlled_access_handler(row):
    open_access = None if pd.isna(row["open_access"]) else row["open_access"]
    controlled_access = row["controlled_access"]
    if open_access:
        return False
    elif isinstance(controlled_access, bool):
        return controlled_access
    else:
        return True


def curate_columns(df):
    df["bs_id"] = df.apply(
        lambda row: [row["bs_id"]] if row["bs_id"] else row["bs_id_cavatica"],
        axis=1,
    )
    df["controlled_access"] = df.apply(
        controlled_access_handler,
        axis=1,
    )
    df["reference_genome"] = df.apply(
        lambda row: row["reference_genome"]
        if isNaN(row["file_name_cavatica"])
        else "GRCh38",
        axis=1,
    )
    df["is_harmonized"] = df.apply(
        lambda row: row["is_harmonized"]
        if row["is_harmonized"]
        else harmonized_path(row["s3path"]),
        axis=1,
    )
    # get sample ids for files missing them
    missing_samples = df[df["external_sample_id"].isna()]["bs_id"].to_list()
    missing_samples = {i for i in missing_samples if not isNaN(i)}
    samples = fetch_missing_samples(missing_samples)
    df["external_aliquot_id"] = df.apply(
        lambda row: row["external_aliquot_id"]
        if row["external_aliquot_id"]
        else [samples.get(row["bs_id"]).get("external_aliquot_id")],
        axis=1,
    )
    df["external_sample_id"] = df.apply(
        lambda row: row["external_sample_id"]
        if row["external_sample_id"]
        else [samples.get(row["bs_id"]).get("external_sample_id")],
        axis=1,
    )
    df["pt_id"] = df.apply(
        lambda row: row["pt_id"]
        if row["pt_id"]
        else [samples.get(row["bs_id"]).get("pt_id")],
        axis=1,
    )
    missing_participants = df[df["external_participant_id"].isna()][
        "pt_id"
    ].to_list()
    missing_participants = {
        i for i in list(chain(*missing_participants)) if not isNaN(i)
    }
    participants = fetch_missing_participants(missing_participants)
    df["external_participant_id"] = df.apply(
        lambda row: row["external_participant_id"]
        if row["external_participant_id"]
        else [
            participants.get(p).get("external_participant_id")
            for p in row["pt_id"]
        ],
        axis=1,
    )
    return df


# * File format and file type handlers
FILE_EXT_FORMAT_MAP = {
    ".fq": GENOMIC_FILE.FORMAT.FASTQ,
    ".fastq": GENOMIC_FILE.FORMAT.FASTQ,
    ".fq.gz": GENOMIC_FILE.FORMAT.FASTQ,
    ".fastq.gz": GENOMIC_FILE.FORMAT.FASTQ,
    ".bam": GENOMIC_FILE.FORMAT.BAM,
    ".hgv.bam": GENOMIC_FILE.FORMAT.BAM,
    ".cram": GENOMIC_FILE.FORMAT.CRAM,
    ".bam.bai": GENOMIC_FILE.FORMAT.BAI,
    ".bai": GENOMIC_FILE.FORMAT.BAI,
    ".cram.crai": GENOMIC_FILE.FORMAT.CRAI,
    ".crai": GENOMIC_FILE.FORMAT.CRAI,
    ".g.vcf.gz": GENOMIC_FILE.FORMAT.GVCF,
    ".g.vcf.gz.tbi": GENOMIC_FILE.FORMAT.TBI,
    ".vcf.gz": GENOMIC_FILE.FORMAT.VCF,
    ".vcf": GENOMIC_FILE.FORMAT.VCF,
    ".vcf.gz.tbi": GENOMIC_FILE.FORMAT.TBI,
    ".peddy.html": GENOMIC_FILE.FORMAT.HTML,
    ".maf": GENOMIC_FILE.FORMAT.MAF,
    ".txt": "txt",
    ".png": "png",
    ".tsv": "tsv",
    ".jpg": "jpg",
    ".pdf": GENOMIC_FILE.FORMAT.PDF,
    "rsem.genes.results.gz": "tsv",
    "rsem.isoforms.results.gz": "tsv",
    "call.seg": "seg",
    "controlfreec.seg": "seg",
    "call.cns": "cns",
}

DATA_TYPES = {
    GENOMIC_FILE.FORMAT.FASTQ: GENOMIC_FILE.DATA_TYPE.UNALIGNED_READS,
    GENOMIC_FILE.FORMAT.BAM: GENOMIC_FILE.DATA_TYPE.ALIGNED_READS,
    GENOMIC_FILE.FORMAT.CRAM: GENOMIC_FILE.DATA_TYPE.ALIGNED_READS,
    GENOMIC_FILE.FORMAT.BAI: GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX,
    GENOMIC_FILE.FORMAT.CRAI: GENOMIC_FILE.DATA_TYPE.ALIGNED_READS_INDEX,
    GENOMIC_FILE.FORMAT.VCF: GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS,
    GENOMIC_FILE.FORMAT.GVCF: GENOMIC_FILE.DATA_TYPE.GVCF,
    GENOMIC_FILE.FORMAT.HTML: COMMON.OTHER,
    # Different TBI types share the same format in FILE_EXT_FORMAT_MAP above
    ".g.vcf.gz.tbi": GENOMIC_FILE.DATA_TYPE.GVCF_INDEX,
    ".vcf.gz.tbi": GENOMIC_FILE.DATA_TYPE.VARIANT_CALLS_INDEX,
    ".annoFuse_filter.tsv": GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS,
    "STAR.fusion_predictions.abridged.coding_effect.tsv": GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS,  # noqa
    "arriba.fusions.tsv": GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS,
    "arriba.fusions.pdf": GENOMIC_FILE.DATA_TYPE.GENE_FUSIONS,
    "rsem.genes.results.gz": GENOMIC_FILE.DATA_TYPE.GENE_EXPRESSION,
    "rsem.isoforms.results.gz": GENOMIC_FILE.DATA_TYPE.GENE_EXPRESSION,
    "call.seg": GENOMIC_FILE.DATA_TYPE.SOMATIC_COPY_NUMBER_VARIATIONS,
    "controlfreec.seg": GENOMIC_FILE.DATA_TYPE.SOMATIC_COPY_NUMBER_VARIATIONS,
    "call.cns": GENOMIC_FILE.DATA_TYPE.SOMATIC_COPY_NUMBER_VARIATIONS,
    "consensus_somatic.public.maf": "Masked Consensus Somatic Mutation",
    "mutect2_somatic.norm.annot.protected.maf": "Annotated Somatic Mutation",
    "mutect2_somatic.norm.annot.public.maf": "Masked Somatic Mutation",
    "controlfreec.info.txt": "Tumor Ploidy and Purity",
    "mutect2_somatic.vep.maf": "Annotated Somatic Mutations",
    "controlfreec.CNVs.p.value.txt": "Somatic Copy Number Variation",
    "vardict_somatic.norm.annot.public.maf": "Masked Somatic Mutation",
    "vardict_somatic.norm.annot.protected.maf": "Annotated Somatic Mutation",
    "lancet_somatic.norm.annot.public.maf": "Masked Somatic Mutation",
    "consensus_somatic.protected.maf": "Consensus Somatic Mutation",
    "controlfreec.ratio.log2.png": "Somatic Copy Number Ratio",
    "lancet_somatic.norm.annot.protected.maf": "Annotated Somatic Mutation",
    "strelka2_somatic.norm.annot.protected.maf": "Annotated Somatic Mutation",
    "strelka2_somatic.norm.annot.public.maf": "Masked Somatic Mutation",
    "strelka2_somatic.vep.maf": "Annotated Somatic Mutations",
    "lancet_somatic.vep.maf": "Annotated Somatic Mutations",
    "vardict_somatic.vep.maf": "Annotated Somatic Mutations",
    "consensus_somatic.vep.maf": "Annotated Somatic Mutations",
    "txt": "Variant Summary",
}


def file_ext(x):
    """
    Get genomic file extension
    """
    matches = [
        file_ext for file_ext in FILE_EXT_FORMAT_MAP if x.endswith(file_ext)
    ]
    if matches:
        file_ext = max(matches, key=len)
    else:
        file_ext = None
    return file_ext


def file_format(x):
    """
    Get genomic file format by looking genomic file ext up in
    FILE_EXT_FORMAT_MAP dict
    """
    return FILE_EXT_FORMAT_MAP.get(file_ext(x))


def data_type(x):
    """
    Get genomic file data type by looking up file format in DATA_TYPES.
    However, some types share formats, so then use the file extension itself
    to do the data type lookup.
    """
    matches = [file_ext for file_ext in DATA_TYPES if x.endswith(file_ext)]
    if matches:
        match = max(matches, key=len)
        return DATA_TYPES.get(match)
    else:
        return DATA_TYPES.get(file_format(x)) or DATA_TYPES.get(file_ext(x))


file_format_condense_map = {
    "Not Reported": COMMON.NOT_REPORTED,
    "Other": COMMON.OTHER,
    "['gVCF', 'gvcf']": GENOMIC_FILE.FORMAT.GVCF,
    "bai": GENOMIC_FILE.FORMAT.BAI,
    "bam": GENOMIC_FILE.FORMAT.BAM,
    "cns": "cns",
    "crai": GENOMIC_FILE.FORMAT.CRAI,
    "cram": GENOMIC_FILE.FORMAT.CRAM,
    "csv": "csv",
    "fastq": GENOMIC_FILE.FORMAT.FASTQ,
    "gVCF": GENOMIC_FILE.FORMAT.GVCF,
    "gpr": GENOMIC_FILE.FORMAT.GPR,
    "gvcf": GENOMIC_FILE.FORMAT.GVCF,
    "html": GENOMIC_FILE.FORMAT.HTML,
    "idat": GENOMIC_FILE.FORMAT.IDAT,
    "jpg": "jpg",
    "maf": GENOMIC_FILE.FORMAT.MAF,
    "pdf": GENOMIC_FILE.FORMAT.PDF,
    "ped": "ped",
    "png": "png",
    "rsem": "rsem",
    "seg": "seg",
    "seq": "seg",
    "tar": "tar",
    "tbi": GENOMIC_FILE.FORMAT.TBI,
    "tsv": "tsv",
    "txt": "txt",
    "vcf": GENOMIC_FILE.FORMAT.VCF,
}

data_type_condense_map = {
    "['Aligned Reads Index', 'Genome Aligned Read Index']": "Genome Aligned Read Index",
    "['Annotated Somatic Mutation Index', 'Variant Calls Index']": "Annotated Somatic Mutation Index",
    "['Consensus Somatic Mutation Index', 'Variant Calls Index']": "Consensus Somatic Mutation Index",
    "['Gene Expression', 'Gene Expression Quantification']": "Gene Expression Quantification",
    "['Genome Aligned Read', 'Aligned Reads']": "Genome Aligned Read",
    "['Isoform Expression Quantification', 'Isoform Expression']": "Isoform Expression Quantification",
    "['Pre-pass Somatic Structural Variation Index', 'Variant Calls Index']": "Pre-pass Somatic Structural Variation Index",
    "['Raw Gene Fusion', 'Gene Fusions']": "Raw Gene Fusion",
    "['Variant Calls Index', 'Masked Consensus Somatic Mutation Index']": "Masked Consensus Somatic Mutation Index",
    "['Variant Calls Index', 'Masked Somatic Mutation Index']": "Masked Somatic Mutation Index",
    "['Variant Calls', 'Annotated Somatic Mutation']": "Annotated Somatic Mutation",
    "['Variant Calls', 'Consensus Somatic Mutation']": "Consensus Somatic Mutation",
    "['Variant Calls', 'Masked Consensus Somatic Mutation']": "Masked Consensus Somatic Mutation",
    "['Variant Calls', 'Masked Somatic Mutation']": "Masked Somatic Mutation",
    "['Variant Calls', 'Pre-pass Somatic Structural Variation']": "Pre-pass Somatic Structural Variation",
    "['gVCF Index', 'Genomic Variant Index']": "gVCF Index",
    "['gVCF', 'Genomic Variant']": "gVCF",
    "['Aligned Reads', 'Genome Aligned Read']": "Genome Aligned Read",
    "['Genomic Variant', 'gVCF']": "Genomic Variant",
    "['Variant Calls Index', 'Annotated Somatic Mutation Index']": "Annotated Somatic Mutation Index",
    "['Variant Calls Index', 'Consensus Somatic Mutation Index']": "Consensus Somatic Mutation Index",
    "['Variant Calls Index', 'Pre-pass Somatic Structural Variation Index']": "Pre-pass Somatic Structural Variation Index",
    "['Masked Somatic Mutation Index', 'Variant Calls Index']": "Masked Somatic Mutation Index",
    "['Annotated Somatic Mutation', 'Variant Calls']": "Annotated Somatic Mutation",
    "['Annotated Somatic Mutation', 'Variant Calls']"
    "['Consensus Somatic Mutation', 'Variant Calls']": "Consensus Somatic Mutation",
    "['Gene Expression Quantification', 'Gene Expression']": "Gene Expression Quantification",
    "['Genome Aligned Read Index', 'Aligned Reads Index']": "Genome Aligned Read Index",
    "['Masked Consensus Somatic Mutation', 'Variant Calls']": "Masked Consensus Somatic Mutation",
    "['Masked Somatic Mutation', 'Variant Calls']": "Masked Somatic Mutation",
    "['Pre-pass Somatic Structural Variation', 'Variant Calls']": "Pre-pass Somatic Structural Variation",
    "['Consensus Somatic Mutation', 'Variant Calls']": "Consensus Somatic Mutation",
}
