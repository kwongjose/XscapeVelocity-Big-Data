#!/usr/bin/env python3
"""
Transform and cleanse EPA air quality data file.
  - Remove unneeded fields
  - Combine State Code, County Code, Site Num as siteCode
    - report inconsistent siteCode/longitude,latitude
  - Combine Date and Time as dateTimeGMT and dateTimeLocal
  - Rename fields with database name
"""

import sys
from csv import DictReader, DictWriter
from typing import Any, Dict

SUPPORTED_PARAMS = [
    "Dew Point",
    "Relative Humidity",
    "PM2.5 - Local Conditions",
]
OUTFIELDS = [
    "siteCode",
    "dateTimeGMT",
    "dateTimeLocal",
    "poc",
    "latitude",
    "longitude",
    "datum",
    "parameterName",
    "sampleMeasurement",
    "unitOfMeasure",
    "mdl",
    "uncertainty",
    "qualifier",
    "dateLastChange",
]


def cleanse_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Transform one csv row"""

    # Intentionally dropped: Parameter Code, Method Type, Method Code,
    # Method Name, State Name, County Name
    rename_dict = {
        "POC": "poc",
        "Latitude": "latitude",
        "Longitude": "longitude",
        "Datum": "datum",
        "Parameter Name": "parameterName",
        "Sample Measurement": "sampleMeasurement",
        "Units of Measure": "unitOfMeasure",
        "MDL": "mdl",
        "Uncertainty": "uncertainty",
        "Qualifier": "qualifier",
        "Date of Last Change": "dateLastChange",
    }
    new_row = {rename_dict[k]: v for k, v in row.items() if k in rename_dict}
    new_row["siteCode"] = f"{row['State Code']}-{row['County Code']}-{row['Site Num']}"
    new_row["dateTimeLocal"] = f"{row['Date Local']}T{row['Time Local']}"
    new_row["dateTimeGMT"] = f"{row['Date GMT']}T{row['Time GMT']}"
    new_row["parameterName"] = new_row["parameterName"].rstrip()
    return new_row


location = {}


def passes_quality_check(row: Dict[str, Any]) -> bool:
    """Detect quality problems with data"""
    checks = True
    site_code = row.get("siteCode")
    longitude = row.get("longitude")
    latitude = row.get("latitude")
    if not site_code or not longitude or not latitude:
        print("Missing siteCode, longitude or latitude", file=sys.stderr)
        return False
    if location.get(site_code):
        checks = location[site_code] == (longitude, latitude)
        if not checks:
            print(f"Inconsistent location for {site_code}", file=sys.stderr)
    else:
        location[site_code] = (longitude, latitude)
    if row.get("parameterName") not in SUPPORTED_PARAMS:
        print(
            f"{site_code} parameter {row['parameterName']} not known", file=sys.stderr
        )
        checks = False
    return checks


def main():
    """Transform air quality csv file"""
    writer = DictWriter(sys.stdout, OUTFIELDS)
    writer.writeheader()

    for raw in DictReader(sys.stdin):
        clean = cleanse_row(raw)
        if passes_quality_check(clean):
            writer.writerow(clean)


if __name__ == "__main__":
    main()
