#!/usr/bin/env python3

import pandas as pd
import requests as rq
import re
import util

from bs4 import BeautifulSoup
from collections import OrderedDict

URL = 'http://search.msboc.us/ConsolidatedResults.cfm'
KEYS = ('Type', 'Company', 'License', 'Address', 'City', 'State', 'Zip', 'Phone')

def fetch_records_page():

    params = { 'ContractorType': '', 'maxrecords': 250,
        'varDataSource': 'BOC', 'Keyword': '', 'ClassCode': 'S107', 
	'Co_Name': '', 'Lic': '', 'LicBegin': '', 'LicEnd': '', 
	'City': '', 'State': '', 'ZipCodes__county': '', 'Zip': '', 
	'Dba_name': '', 'Mnrty_cat': '', 'Expir_date_Begin': '', 
        'Expir_date_End': '', 'Expired': '', 'Print_cert': '', 
        'OrderBy': 'Co_Name', 'GeneralLiabilityInsuranceActive': '', 
        'searchType': '', 'Advanced': 1, 'SearchStatus': '', 
        'issue_date_Begin': '', 'issue_date_End': '', 
        'enter_date_Begin': '', 'enter_date_end': '', 
        'qualname': '', 'startrow': 1 }

    resp = rq.get(URL, params=params)
    html = resp.text
    return html

def parse_records_page(html):
    
    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('tr', class_=re.compile('^TR'))
    for tr in rows:
    	yield parse_row(tr)

def parse_row(soup):

    data = {}
    cols = soup.find_all('td', recursive=False)
    for k, td in zip(KEYS, cols[1:]):
    	data[k] = td.get_text(strip=True)

    anchor = cols[0].find('a')
    url = 'http://search.msboc.us/'
    data['href'] = url + anchor['href']
    print(anchor['href'])

    return data

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)

    data['File'] = record['href']
    data['Company'] = record['Company']
    data['License Number'] = record['License']
    data['Street Address 1'] = record['Address']
    data['City'] = record['City']
    data['State'] = record['State']
    data['Zip Code'] = record['Zip']
    data['Phone'] = record['Phone']

    data['Full Name'] = record['Name']
    data['County'] = record['County']

    data['Issue Date'] = record['Issue']
    data['Expiration Date'] = record['Expiration']

    data['Fax Number'] = record['Fax']
    data['DBA Name'] = record['DBA Name']

    return data

def scrape_plumbers():
    html = fetch_records_page()
    yield from parse_records_page(html)

def next_td_text(elem):
    if elem is None: return 
    td = elem.find_next_sibling('td')
    return td.get_text(strip=True) if td else ''

def scrape_details(record):
    
    url = record['href']

    resp = rq.get(url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')

    elem = soup.find('td', string='Miss. County')
    record['County'] = next_td_text(elem)

    elem = soup.find('td', string='Fax')
    record['Fax'] = next_td_text(elem)

    elem = soup.find('td', string='DBA Name')
    record['DBA Name'] = next_td_text(elem)

    elem = soup.find('td', string='Expiration Date')
    record['Expiration'] = next_td_text(elem)

    elem = soup.find('td', string='First Issue')
    record['Issue'] = next_td_text(elem)

    elem = soup.find('td', string='PLUMBING')
    record['Name'] = next_td_text(elem)

    return record

def unique_record(record):
    return tuple( record[k] for k in KEYS )

def export_csv():
    data = util.read_json('mississippi_ex.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('mississippi.csv', index=None)

def main():
    filename = 'mississippi_ex.json'
    results = util.read_json(filename)
    scraped = set( unique_record(x) for x in results )
    for record in scrape_plumbers():
        if unique_record(record) not in scraped:
            print('Scraping details', record['License'])
            details = scrape_details(record)
            results.append(details)
            util.write_json(filename, results)
        else:
            print('Already scraped', record['License'])
    util.write_json(filename, results)

if __name__ == '__main__':
    main()

