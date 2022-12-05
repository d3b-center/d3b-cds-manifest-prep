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

## Data

The most recently generated manifest can be found at [`data/submission_packet`](data/submission_packet)

## Usage

### Generate CDS Submission Manifest

*n.b. `cds generate --help` to get detailed descriptions of options*

#### Generate a submission packet

```sh
cds -c $DATABASE_URL generate
```

This uses the environment variable `$DATABASE_URL`. This is expected to be a 
postgres connection url to kidsfirst postgres dataservice.

By default, the file-sample-participant mapping bundled in this package 
[here](cds/data/file_sample_participant_map.csv) is used to generate the 
manifests.

#### Generate a submission packet using a specific file-sample-participant map

The `-f` option specifies the seed file-sample-participant map to use.

```sh
cds -c $DATABASE_URL generate -f path/to/file_sample_participant_map.csv
```

#### generate only a specific manifest in the submission packet

```sh
cds -c $DATABASE_URL generate -g sample
```

This will generate only the sample manifest.

#### generate specific manifests in the submission packet

```sh
cds -c $DATABASE_URL generate -g sample -g participant
```

This will generate both the sample and participant manifests.

### QC a submission manifest

```sh
cds -c $DATABASE_URL -d data/submission_packet/ qc
```