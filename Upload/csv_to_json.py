#!/usr/bin/env python3
"""
Convert datasets from CSV format into JSON,
dropping unneeded fields and combining others.
"""

import argparse
import csv
import json
import uuid

def get_args():
    """Get command line arguments"""
    parser = argparse.ArgumentParser(
        description='Convert datasets from CSV to JSON',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parser.add_argument('csv', metavar='CSV file', help='Input CSV filename')
    parser.add_argument('json', metavar='JSON file', help='Output JSON filename')
    return parser.parse_args()

def make_json(csv_file_path, json_file_path):
    """Read CSV, write JSON"""
    with open(csv_file_path, mode="r", encoding='utf-8-sig') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            json_file.write('[')
            for index, row in enumerate(csv_reader):
                new_row = rename_columns(delete_columns(row))
                new_row['id'] = str(uuid.uuid4())
                if index != 0:
                    json_file.write(',')
                json.dump(new_row, json_file, indent=4)
            json_file.write(']')

def delete_columns(row):
    """Drop unneeded fields"""
    remove_list = [
        'Parameter Code',
        'Method Type',
        'Method Code',
        'Method Name',
        'State Name',
        'County Name'
    ]
    return {k: v for k, v in row.items() if k not in remove_list}

def rename_columns(row):
    """Map CVS fields to JSON labels"""
    print(row)
    rename_dict = {
        'POC': 'poc',
        'Datum': 'datum',
        'Parameter Name': 'parameterName',
        'Date Local': 'dateLocal',
        'Time Local': 'timeLocal',
        'Date GMT': 'dateGMT',
        'Time GMT': 'timeGMT',
        'Sample Measurement': 'sampleMeasurement',
        'Units of Measure': 'unitOfMeasure',
        'MDL': 'mdl',
        'Uncertainty': 'uncertainty',
        'Qualifier': 'qualifier',
        'Date of Last Change': 'dateLastChange'
    }
    new_row = {rename_dict[k]: v for k, v in row.items() if k in rename_dict}
    new_row['siteCode'] = f'{row["State Code"]}-{row["County Code"]}-{row["Site Num"]}'
    new_row['location'] = {
        'type': 'Point',
        'coordinates': [row['Longitude'], row['Latitude']]
    }
    return new_row

def main():
    """Convert CSV datasets to JSON"""
    args = get_args()
    make_json(args.csv, args.json)

if __name__ == '__main__':
    main()