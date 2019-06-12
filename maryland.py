#!/usr/bin/env python3

import requests as rq
import pandas as pd
import util
import datetime as dt
import nameparser

from bs4 import BeautifulSoup
from collections import OrderedDict

from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException

URL = 'https://www.dllr.state.md.us/cgi-bin/ElectronicLicensing/OP_Search/OP_search.cgi?calling_app=PLM::PLM_personal_location'
KEYS = ('Name', 'City', 'State', 'Zip', 'Expiration', 'Category', 'Insured', 'License')

# Serach plumbers by zip code
def fetch_plumbers(driver: webdriver.Chrome, zip_):

    driver.get(URL)

    # Enter zip code
    elem = driver.find_element_by_name('zip')
    elem.send_keys(zip_)

    # Click submit button
    elem = driver.find_element_by_name('Submit')
    elem.click()

    html = driver.page_source
    return html

def extract_plumbers(html):

    soup = BeautifulSoup(html, 'html.parser')
    table = soup.find('table')
    if table is None: return

    rows = table.tbody.find_all('tr', recursive=False)
    for tr in rows[1:]:
        yield parse_row(tr)

def find_element_by_xpath(driver, path):
    try:
        return driver.find_element_by_xpath(path)
    except NoSuchElementException: 
        return None

def parse_row(soup):

    data = dict.fromkeys(KEYS)
    cols = soup.find_all('td')
    for k, td in zip(KEYS, cols):
        data[k] = td.get_text(strip=True)

    return data

def scrape_plumbers_location(driver, code):

    html = fetch_plumbers(driver, code)

    while True:
        for data in extract_plumbers(html):
            data['Zip'] = code
            yield data

        path = '//input[@value=" Next 50 "]'
        elem = find_element_by_xpath(driver, path)
        if elem is None: break
        elem.click()


def scrape_plumbers(driver):

    df = pd.read_csv('./data/md_zip.csv')

    for code in df.zip:
        print('Scraping Zip Code', code)
        yield from scrape_plumbers_location(driver, code)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['Name'])

    try:
        exp = dt.datetime.strptime(record['Expiration'], '%Y-%m-%d')
        data['License Status'] = 'Expired' \
                if exp < dt.datetime.now() else 'Active'
    except ValueError:
        data['License Status'] = None

    data['Last Name'] = name.last
    data['First Name'] = name.first
    data['City'] = record['City']
    data['State'] = record['State']
    data['Zip Code'] = record['Zip']
    data['License Number'] = record['License']
    data['Full Name'] = record['Name']
    data['License Type'] = record['Category']
    data['Insured'] = record['Insured']

    data['Expiration Date'] = record['Expiration']

    return data

def export_csv():
    data = util.read_json('maryland_ex.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./maryland.csv', index=None)

def main():

    driver = webdriver.Chrome()
    results = []

    for pl in scrape_plumbers(driver):
        results.append(pl)
        if len(results) % 200 == 0:
            util.write_json('./maryland_ex.json', results)
            
    util.write_json('./maryland_ex.json', results)

if __name__ == '__main__':
    main()

