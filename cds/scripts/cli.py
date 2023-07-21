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
from cds.io.io import write_to_excel
from cds.qc.qc import qc_submission_package

import click
import pkg_resources

CONTEXT_SETTINGS = {
    "help_option_names": ["-h", "--help"],
    "max_content_width": 9999,  # workaround for https://github.com/pallets/click/issues/486
}

option_pg_url = click.option(
    "-c",
    "--postgres_connection_url",
    type=str,
    required=True,
    default=default_postgres_url,
    help="Connection URL to KF Postgres",
)

option_submission_dir = click.option(
    "-d",
    "--submission_package_dir",
    type=click.Path(exists=True, dir_okay=True, file_okay=False),
    required=False,
    default=submission_package_default_dir,
    show_default=True,
    help="Location of directory that has the submision package.",
)
option_template_file = click.option(
    "-t",
    "--template_file",
    type=click.Path(exists=True, dir_okay=False),
    required=True,
    default=pkg_resources.resource_filename("cds", template_default),
    help="Excel template for the submission",
)


@click.group(context_settings=CONTEXT_SETTINGS)
@click.version_option(package_name="d3b-cds-manifest-tools")
def cds():
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
    pass


@cds.command("generate")
@option_pg_url
@option_submission_dir
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
        all_generator_list + ["all"],
        case_sensitive=False,
    ),
    default=["all"],
    show_default=True,
)
@option_template_file
def generate_submission(
    postgres_connection_url,
    submission_package_dir,
    seed_file,
    generator,
    template_file,
):
    """
    Generate a CDS submission manifest or manifests using a seed
    file_sample_participant mapping.
    """
    generate_submission_package(
        postgres_connection_url,
        submission_package_dir,
        seed_file,
        generator,
        template_file,
    )


@cds.command("build_excel")
@option_submission_dir
@option_template_file
def build_excel_submission(submission_package_dir, template_file):
    write_to_excel(
        submission_package_dir=submission_package_dir,
        submission_template_file=template_file,
    )


@cds.command("qc")
@option_pg_url
@option_submission_dir
def qc_submission(postgres_connection_url, submission_package_dir):
    """QC a CDS Submission Package"""
    qc_submission_package(postgres_connection_url, submission_package_dir)


@cds.command("gen_histologies")
@option_pg_url
@click.option(
    "-o",
    "--output_filename",
    type=click.File(mode="w"),
    required=True,
    help="Filename of where to save the histologies file",
)
def regenerate_histologies_data(postgres_connection_url, output_filename):
    """Regenerate histologies data"""
    fetch_histologies_file(postgres_connection_url, output_filename)


if __name__ == "__main__":
    cds()  # pylint: disable=no-value-for-parameter
