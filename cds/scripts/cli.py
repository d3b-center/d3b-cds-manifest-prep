"""
Copies contents of prd dataservice to local dataservice for a particular study
"""
from cds.common.constants import (
    all_generator_list,
    submission_package_default_dir,
)
from cds.generator.generate import generate_submission_package
from cds.qc.qc import qc_submission_package

import click
import pandas as pd


@click.group()
@click.option(
    "-c",
    "--postgres_connection_url",
    type=str,
    required=True,
    help="Connection URL to KF Postgres",
)
@click.option(
    "-d",
    "--submission_packager_dir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=False,
    default=submission_package_default_dir,
    show_default=True,
    help="Location of directory that has the submision package.",
)
@click.pass_context
def cds(ctx, postgres_connection_url, submission_packager_dir):
    """
    Tools related to handling CDS manifest generation and QC.

    Our submission package to CDS has five documents. See the
    CDS_submission_metadata_template for more information about the elements
    in each file:

    \b
    1. file_sample_participant_map.csv
    2. participant.csv
    3. sample.csv
    4. diagnosis.csv
    5. file.csv
    \f
    :param ctx: context to allow different commands to interact with eachother
    :type ctx: click.Context
    :param postgres_connection_url: postgres connection url. takes the form
    postgresql://[user]:[password]@[host]:[port]/[database_name]
    :type postgres_connection_url: str
    :param submission_packager_dir: Directory of submission package. Ends in
    trailing backslash, e.g. `path/to/dir/`
    :type submission_packager_dir: str
    """
    ctx.ensure_object(dict)
    ctx.obj["postgres_connection_url"] = postgres_connection_url
    ctx.obj["submission_packager_dir"] = submission_packager_dir
    pass


@cds.command("generate")
@click.option(
    "-f",
    "--seed_file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    help="CSV file that maps all the files, samples, and participants to "
    + "use to generate the manifests.",
)
@click.option(
    "-g",
    "--table_to_generate",
    "generator",
    multiple=True,
    type=click.Choice(
        all_generator_list,
        case_sensitive=False,
    ),
    default=["all"],
    show_default=True,
)
@click.pass_context
def generate_submission(ctx, seed_file, generator):
    """Copy the kf_ids from the ids in the given file from
    the source dataservice to the target dataservice
    """
    generate_submission_package(
        ctx.obj["postgres_connection_url"],
        ctx.obj["submission_packager_dir"],
        seed_file,
        generator,
    )


@cds.command("qc")
@click.pass_context
def qc_submission(ctx):
    """QC a CDS Submission Package"""
    qc_submission_package(
        ctx.obj["postgres_connection_url"], ctx.obj["submission_packager_dir"]
    )


if __name__ == "__main__":
    cds()  # pylint: disable=no-value-for-parameter
