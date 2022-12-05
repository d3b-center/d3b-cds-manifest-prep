from os import path

from setuptools import find_packages, setup

# requirements from requirements.txt
root_dir = path.dirname(path.abspath(__file__))
with open(path.join(root_dir, "requirements.txt"), "r") as f:
    requirements = f.read().splitlines()

# long description from README
with open(path.join(root_dir, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name="d3b-cds-manifest-tools",
    use_scm_version={
        "local_scheme": "dirty-tag",
        "version_scheme": "post-release",
    },
    setup_requires=["setuptools_scm"],
    description="D3B CDS Manifest Tools",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/d3b-center/d3b-cds-manifest-prep",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "cds = cds.scripts.cli:cds",
        ],
    },
    python_requires=">=3.6, <4",
    install_requires=requirements,
    include_package_data=True,
    package_data={"": ["data/*.xlsx", "data/*.tsv", "data/*.csv"]},
)
