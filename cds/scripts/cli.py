"""
Copies contents of prd dataservice to local dataservice for a particular study
"""
from cds.common.constants import (
    all_generator_list,
    default_postgres_url,
    file_sample_participant_map_default,
    submission_package_default_dir,
    template_default,
)
from cds.data.fetch_histologies import fetch_histologies_file
from cds.generator.generate import generate_submission_package
from cds.qc.qc import qc_submission_package

import click
import pkg_resources


@click.group()
@click.version_option(package_name="d3b-cds-manifest-tools")
@click.option(
    "-c",
    "--postgres_connection_url",
    type=str,
    required=True,
    default=default_postgres_url,
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
    default=pkg_resources.resource_filename(
        "cds", file_sample_participant_map_default
    ),
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
@click.option(
    "-t",
    "--template_file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    default=pkg_resources.resource_filename("cds", template_default),
    help="Excel template for the submission",
)
@click.pass_context
def generate_submission(ctx, seed_file, generator, template_file):
    """
    Generate a CDS submission manifest or manifests using a seed
    file_sample_participant mapping.
    """
    generate_submission_package(
        ctx.obj["postgres_connection_url"],
        ctx.obj["submission_packager_dir"],
        seed_file,
        generator,
        template_file,
    )


@cds.command("qc")
@click.pass_context
def qc_submission(ctx):
    """QC a CDS Submission Package"""
    qc_submission_package(
        ctx.obj["postgres_connection_url"], ctx.obj["submission_packager_dir"]
    )


@cds.command("gen_histologies")
@click.option(
    "-o",
    "--output_filename",
    type=click.File(mode="w"),
    required=True,
    help="Filename of where to save the histologies file",
)
@click.pass_context
def regenerate_histologies_data(ctx, output_filename):
    """Regenerate histologies data"""
    fetch_histologies_file(ctx.obj["postgres_connection_url"], output_filename)


if __name__ == "__main__":
    cds()  # pylint: disable=no-value-for-parameter
