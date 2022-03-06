#!/usr/bin/env python3
"""
Open pm and rh csv files for time period

Until end of file:
  - Read pm csv until a new dateTimeGMT is found
  - Read rh csv until a new dateTimeGMT is found
  - Join data on sitecode, dateTimeGMT
  - Write denormalized record
"""

import argparse
import json
import os
import sys
import uuid
from csv import DictReader
from typing import Any, Dict, List, TextIO


def get_args():
    """Get command line arguments"""
    parser = argparse.ArgumentParser(
        description="Merge PM and RH air quality datasets",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )

    parser.add_argument(
        "--pm",
        help="Particulate matter CSV file",
        metavar="str",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--rh",
        help="Relative humidity CSV file",
        metavar="str",
        type=str,
        required=True,
    )
    parser.add_argument(
        "--output",
        help="Output file for merged JSON",
        metavar="str",
        type=str,
        required=False,
    )

    args = parser.parse_args()

    if not os.path.isfile(args.pm):
        parser.error(f"--pm {args.pm} not readable")
    if not os.path.isfile(args.rh):
        parser.error(f"--pm {args.rh} not readable")

    return args


def read_data_block(reader: DictReader, data: List[Dict[str, Any]]):
    """Read data up to next dateTimeGMT change"""
    date_time = data[0] if data else ""
    for row in reader:
        data.append(row)
        if not date_time:
            date_time = row.get("dateTimeGMT")
        if date_time != row.get("dateTimeGMT"):
            break
    return data


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize data by applying prefix non-join fields"""
    join = ["siteCode", "dateTimeGMT", "dateTimeLocal"]
    location = ["longitude", "latitude"]
    out_row = {}

    for key in join:
        out_row[key] = row[key]
    out_row["location"] = {
        "type": "Point",
        "coordinates": [row["longitude"], row["latitude"]],
    }

    prefix = ""
    if row["parameterName"] == "PM2.5 - Local Conditions":
        prefix = "pm_"
    elif row["parameterName"] == "Relative Humidity":
        prefix = "rh_"
    elif row["parameterName"] == "Dew Point":
        prefix = "dp_"
    else:
        # data has already been cleansed, should not happen
        print(
            f"Bad parameter for {row['siteCode']}: {row['parameterName']}",
            file=sys.stderr,
        )
        raise ValueError

    for key, value in row.items():
        if key not in join + location:
            out_row[f"{prefix}{key}"] = value

    return out_row


def location_matches(loc1: Dict[str, Any], loc2: Dict[str, Any]) -> bool:
    """Test two locations for equality using epsilon"""
    epsilon = 0.00001
    if loc1["type"] != loc2["type"]:
        return False
    if abs(float(loc1["coordinates"][0]) - float(loc2["coordinates"][0])) > epsilon:
        return False
    if abs(float(loc1["coordinates"][1]) - float(loc2["coordinates"][1])) > epsilon:
        return False
    return True


bad_locations = {}


def merge_rh_row(row: Dict[str, Any], out_data: Dict[str, Any]):
    """Merge one RH row into output only if PM measurement exists"""
    date_time = row["dateTimeGMT"]
    site_code = row["siteCode"]
    location = row["location"]

    out_row = {}
    if out_data.get(date_time):
        out_row = out_data.get(date_time).get(site_code)
    if out_row:
        if not location_matches(location, out_row["location"]):
            if not bad_locations.get(site_code):
                print(f"Inconsistent location for {site_code}", file=sys.stderr)
                bad_locations[site_code] = True
        for key, value in row.items():
            if key[0:3] in ["rh_", "dp_"]:
                out_row[key] = value


def store_pm_data(pm_data: List[Dict[str, Any]], out_data: Dict[str, Any]):
    """Merge PM data into output"""
    if not pm_data:
        return
    date_time = pm_data[0]["dateTimeGMT"]
    while pm_data:
        if pm_data[0]["dateTimeGMT"] != date_time:
            break
        row = pm_data.pop(0)
        normal_row = normalize_row(row)
        if out_data.get(row["dateTimeGMT"]):
            out_data[row["dateTimeGMT"]][row["siteCode"]] = normal_row
        else:
            out_data[row["dateTimeGMT"]] = {row["siteCode"]: normal_row}


def store_rh_data(rh_data: List[Dict[str, Any]], out_data: Dict[str, Any]):
    """Merge RH data into output"""
    if not rh_data:
        return
    date_time = rh_data[0]["dateTimeGMT"]
    while rh_data:
        if rh_data[0]["dateTimeGMT"] != date_time or not out_data.get(
                rh_data[0]["dateTimeGMT"]
        ):
            break
        row = rh_data.pop(0)
        merge_rh_row(normalize_row(row), out_data)


def merge_data(
    pm_data: List[Dict[str, Any]], rh_data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """Merge PM and RH data by time and location"""
    out_data = {}
    store_pm_data(pm_data, out_data)
    store_rh_data(rh_data, out_data)
    return out_data


def write_json(out_fh: TextIO, out_data: Dict[str, Any]):
    """Write json object to output"""
    for site_date in out_data.values():
        for row in site_date.values():
            row["id"] = str(uuid.uuid4())
            json.dump(row, out_fh, indent=4)
            out_fh.write(",")


def normalize_data(pm_reader: DictReader, rh_reader: DictReader, out_fh: TextIO):
    """Read pm and rh data, write merged json"""
    pm_data = []
    rh_data = []
    out_data = {}
    out_fh.write("[")
    while True:
        read_data_block(pm_reader, pm_data)
        if not pm_data:
            break
        pm_date_time = pm_data[0]["dateTimeGMT"]
        read_data_block(rh_reader, rh_data)
        # align rh data to pm
        while rh_data and rh_data[0]["dateTimeGMT"] < pm_date_time:
            last_row = rh_data[-1]
            rh_data.clear()
            rh_data.append(last_row)
            read_data_block(rh_reader, rh_data)
        out_data = merge_data(pm_data, rh_data)
        if not out_data:
            break
        write_json(out_fh, out_data)
    out_fh.write("]")


def main():
    """Merge particulate matter and relative humidity air quality datasets"""
    args = get_args()
    with (
        open(args.output, "wt", encoding="utf-8")
        if args.output
        else sys.stdout as out_fh,
        open(args.pm, "rt", encoding="utf-8") as pm_fh,
        open(args.rh, "rt", encoding="utf-8") as rh_fh,
    ):
        normalize_data(DictReader(pm_fh), DictReader(rh_fh), out_fh)


if __name__ == "__main__":
    main()
