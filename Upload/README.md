# Data ETL

## Extract: Fetch EPA air quality data

Pull hourly 2.5 PM data (hourly_88101_YEAR.zip) and hourly RH/DP data
files (hourly_RH_DP_YEAR.zip) from
https://aqs.epa.gov/aqsweb/airdata/download_files.html

Put the files in the datasets subdirectory and unzip.

This has been automated with `gmake extract`

## Transform: Cleanse, denormalize and convert to JSON

The transform of CSV files to JSON documents is done in three main steps:

1. transform_cleanse.py
2. sort_csv.sh
3. merge_denormalize.py

This has been automated with `gmake transform`

## Load: Load JSON files to Cosmos DB

TBD.
