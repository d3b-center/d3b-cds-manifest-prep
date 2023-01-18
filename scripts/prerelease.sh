#!/bin/bash
set -e

mkdir -p tmp

# Add new version to bug report form
awk -v version_id="${1}" '/new_versions_here/ { print; print "        - \x22" version_id "\x22"; next }1' .github/ISSUE_TEMPLATE/submission_pkg_bug_report.yml > tmp/submission_bug_report
cp tmp/submission_bug_report .github/ISSUE_TEMPLATE/submission_pkg_bug_report.yml

# Update the readme
sed -E "s/[0-9]+\.[0-9]+\.[0-9]+/${1}/g" README.md > tmp/readme
cp tmp/readme README.md

rm -rf tmp