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

# Add new version to bug report form
awk -v version_id="${1}" '/new_versions_here/ { print; print "        - \x22" version_id "\x22"; next }1' .github/ISSUE_TEMPLATE/submission_pkg_bug_report.yml > tmp_submission_bug_report
cp tmp_submission_bug_report .github/ISSUE_TEMPLATE/submission_pkg_bug_report.yml