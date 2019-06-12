#!/usr/bin/env python3

# License
# Name
# City
# County
# Classification
# Expires

import pandas as pd
import requests as rq
import json
import util
import nameparser
import datetime as dt

from bs4 import BeautifulSoup
from collections import OrderedDict

URL  = 'http://www.wvlabor.com/new_searches/plumber_RESULTS.cfm'
KEYS = ('PLNumber', 'Name', 'City', 'County', 'Classification', 'Expires')

def parse_row(row):
    data = {}
    cols = row.find_all('td')
    for k, d in zip(KEYS, cols):
        data[k] = d.get_text(strip=True)
    return data

def extract_plumbers(html):
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('tr')
    for row in rows[1:]:
        yield parse_row(row)

def write_json(filename, data):
    with open(filename, 'w', encoding='utf8') as fp:
        json.dump(data, fp, indent=2)

def fetch_plumbers():
    
    params = {
        'PageNum_WVNUMBER': 1,
        'wvnumber': '',
        'contractor_name': '',
        'city_name': '',
        'County': '',
        'Submit3': 'Search+Plumbers'
    }

    for p in range(1, 264):
        params['PageNum_WVNUMBER'] = p
        resp = rq.get(URL, params=params)
        html = resp.text
        yield from extract_plumbers(html)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['Name'])
    exp = dt.datetime.strptime(record['Expires'], '%Y-%m-%d')

    # missing classification
    data['Last Name'] = name.last
    data['First Name'] = name.first
    data['City'] = record['City']
    data['State'] = 'West Virginia'
    data['License Status'] = 'Expired' if exp < dt.datetime.now() else 'Active'
    data['License Number'] = record['PLNumber']

    data['Expiration Date'] = record['Expires']
    data['Classification'] = record['Classification']
    data['County'] = record['County']

    return data

def export_csv():
    data = util.read_json('westvirginia.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./westvirginia.csv', index=None)

def main():

    filename = 'westvirginia.json'
    results = []
    cont = 1

    for record in fetch_plumbers():
        results.append(record)
        print('[{}/6289] {}'.format(cont, record['PLNumber']))
        write_json(filename, results)
        cont += 1
            
if __name__ == '__main__':
    main()

