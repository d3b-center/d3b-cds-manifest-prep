#!/bin/bash
set -e

ZIP_NAME="data/zip/submission_package_${1}.zip"

zip -r "${ZIP_NAME}"  data/submission_packet/*

echo "${ZIP_NAME}" > .github/release_assets.txt
