# d3b-cds-manifest-prep

## Installation

### Install latest release: 

```python
pip install git+https://github.com/d3b-center/d3b-cds-manifest-prep.git@latest-release
```

### Install Most Recent Development Version


```python
pip install git+https://github.com/d3b-center/d3b-cds-manifest-prep.git
```

## Usage

### Generate CDS Submission Manifest

*n.b. `cds generate --help` to get detailed descriptions of options*

#### Generate a submission packet

```sh
cds -c $DATABASE_URL generate -f data/file_sample_participant_map.csv
```

This uses the environment variable `$DATABASE_URL`. This is expected to be a 
postgres connection url to kidsfirst postgres dataservice.

The `-f` option points to a file_sample_participant mapping.

#### generate only a specific manifest in the submission packet

```sh
cds -c $DATABASE_URL generate -f data/file_sample_participant_map.csv -g sample
```

This will generate only the sample manifest.

#### generate specific manifests in the submission packet

```sh
cds -c $DATABASE_URL generate -f data/file_sample_participant_map.csv -g sample -g participant
```

This will generate both the sample and participant manifests.

### QC a submission manifest

```sh
cds -c $DATABASE_URL -d data/submission_packet/ qc
```