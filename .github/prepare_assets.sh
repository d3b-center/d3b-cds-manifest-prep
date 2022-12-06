#!/bin/bash
set -e

ZIP_NAME="data/zip/submission_package_${1}.zip"

ZIP_DATA_DIR="submission_package_v${1}"

mkdir "${ZIP_DATA_DIR}"

cp -r data/submission_packet/ "${ZIP_DATA_DIR}" 
cp CHANGELOG.md "${ZIP_DATA_DIR}"

zip -r "${ZIP_NAME}" "${ZIP_DATA_DIR}/" -x "${ZIP_DATA_DIR}/submission_packet/README.md"

# Add new zip file to release assets
echo "${ZIP_NAME}" > .github/release_assets.txt