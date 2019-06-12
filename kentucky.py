#!/usr/bin/env python3

import requests as rq
import time
import json
import util
import nameparser
import pandas as pd

from collections import OrderedDict

URL = 'https://ky.joportal.com/License/Search'

def fetch_plumbers(page):

    params = {
        'Division': 103, 'LicenseType': [13, 14],
        'multiselect_LicenseType': [13, 14], 
        'LicenseNumber': '', 'BusinessName': '', 
        'LastName': '', 'FirstName': '', 'County': '' }

    nd = int(time.time() * 1000)
    payload = {
        '_search': 'false', 'nd': nd,
        'PageSize': 50, 'PageNumber': page,
        'OrderBy': 'Number', 'OrderByDirection': 'asc' }

    resp = rq.post(URL, params=params, data=payload)
    data = resp.json()

    return data.get('rows', [])

def scrape_plumbers():
    for p in range(1, 250):
        print('Fetching page', p)
        yield from fetch_plumbers(p)

def write_json(filename, data):
    with open(filename, 'w', encoding='utf8') as fp:
        json.dump(data, fp, indent=2)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['FullName'])

    data['City'] = record['City']
    data['Last Name'] = name.last
    data['First Name'] = name.first
    data['State'] = record['CountyState']
    data['License Number'] = record['Number']
    data['License Status'] = record['Status']
    data['Full Name'] = record['FullName']
    data['License Type'] = record['Type']
    data['Expiration Date'] = record['ExpirationDate']
    data['Application Date'] = record['ApplicationDate']
    data['Expiration Date'] = record['ExpirationDate']
    data['Renewal Date'] = record['RenewalDate']
    data['License Type'] = record['Type']

    return data

def export_csv():
    data = util.read_json('kentucky.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./kentucky.csv', index=None)

def main():
    pass

if __name__ == '__main__':
    main()
