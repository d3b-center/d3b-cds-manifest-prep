"""
Copies contents of prd dataservice to local dataservice for a particular study
"""
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
@click.pass_context
def cds(ctx, postgres_connection_url):
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
    """
    ctx.ensure_object(dict)
    ctx.obj["postgres_connection_url"] = postgres_connection_url
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
@click.pass_context
def generate_submission(ctx, file, kf_id):
    """Copy the kf_ids from the ids in the given file from
    the source dataservice to the target dataservice
    """
    breakpoint()


@cds.command("qc")
@click.option(
    "-d",
    "--submision_directory",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=True,
    help="Location of directory that has the submision package.",
)
@click.pass_context
def qc_submission(ctx, submision_directory):
    """QC a CDS Submission Package"""
    qc_submission_package(
        ctx.obj["postgres_connection_url"], submision_directory
    )


if __name__ == "__main__":
    cds()  # pylint: disable=no-value-for-parameter
