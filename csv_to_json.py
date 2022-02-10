import csv
import json

def make_json(csv_file_path, json_file_path):
    data = {}
    with open(csv_file_path, encoding='utf-8') as csv_file:
        csv_reader = csv.DictReader(csv_file)
        with open(json_file_path, 'w', encoding='utf-8') as json_file:
            for row in csv_reader:
                new_row = rename_columns(delete_columns(row))
                json.dump(new_row,json_file, indent=4)

def delete_columns(row):
    remove_list = ['Parameter Code','Method Type','Method Code','Method Name','State Name','County Name']
    [row.pop(key) for key in remove_list]
    return row
        
def rename_columns(row):
    rename_dict = {
        'State Code':'stateCode',
        'County Code':'countyCode',
        'Site Num':'siteNum',
        'POC':'poc',
        'Latitude':'latitude',
        'Longitude':'longitude',
        'Datum':'datum',
        'Parameter Name':'parameterName',
        'Date Local':'dateLocal',
        'Time Local':'timeLocal',
        'Date GMT':'dateGMT',
        'Time GMT':'timeGMT',
        'Sample Measurement':'sampleMeasurement',
        'Units of Measure':'unitOfMeasure',
        'MDL':'mdl',
        'Uncertainty':'uncertainty',
        'Qualifier':'qualifier',
        'Date of Last Change':'dateLastChange'
    }
    new_row = {rename_dict[old_column]: value for old_column, value in row.items()}
    return new_row

def main():
    csv_file_path = r"file_path.csv"
    json_file_path = r"file_path.json"
    make_json(csv_file_path, json_file_path)

if __name__ == '__main__':
    main()