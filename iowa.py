#!/usr/bin/env python3

import pandas as pd
import re
import util
import nameparser

from collections import OrderedDict
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

URL = 'https://dphregprograms.iowa.gov/PublicPortal/Iowa/IDPH/publicSearch/publicSearch.jsp'
KEYS = ('License', 'Name', 'Program', 'City')

def prepare_scrape(driver: webdriver.Chrome):

    driver.get(URL)

    elem = Select(driver.find_element_by_name('program'))
    elem.select_by_value('PMSB')
    # Wait for page

    elem = Select(driver.find_element_by_name('status'))
    elem.select_by_value('75')
    # Wait for page

    elem = driver.find_element_by_id('btn_search')
    driver.execute_script("arguments[0].click();", elem)
    # Wait for page

def click_page_button(driver: webdriver.Chrome, page):

    query = '//div[@id="paginateContainer"]//select'
    elem = Select(driver.find_element_by_xpath(query))
    elem.select_by_value(str(page))

def parse_row(row):

    data = dict()
    cols = row.find_all('td', recursive=False)
    for k, td in zip(KEYS, cols[1:]):
        data[k] = td.get_text(strip=True)

    anchor = row.find('a', attrs={ 'onclick': True })
    script = anchor['onclick']

    # Data that will be used to extract details later
    match = re.search(r'(\d+),\s(\d+)', script)
    data['_folderRSN'] = match.group(1)
    data['_pRSN'] = match.group(2)

    return data

def extract_plumbers(html):

    soup = BeautifulSoup(html, 'html.parser')
    elem = soup.find('table', id='resulttable')
    rows = elem.tbody.find_all('tr', recursive=False)

    for tr in rows:
        yield parse_row(tr)

def scrape_plumbers(driver):

    prepare_scrape(driver)
    page = 1

    while True:
        print('Scraping page', page)
        html = driver.page_source
        yield from extract_plumbers(html)

        if page >= 530: break
        page += 1

        click_page_button(driver, page)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['Name'])

    data['City'] = record['City']
    data['Last Name'] = name.last
    data['First Name'] = name.first
    data['License Number'] = record['License']
    data['Full Name'] = record['Name']
    data['License Type'] = record['Program']

    data['License Status'] = record.get('Status', '')
    data['Issue Date'] = record.get('Issue', '')
    data['Expiration Date'] = record.get('Expiration', '')
    data['License Type'] = record.get('Type', '')
    data['Speciality'] = record.get('Speciality', '')

    return data

def export_csv():
    data = util.read_json('iowa_ex.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./iowa.csv', index=None)

def scrape_details(driver, record):


    payload = { 'folderRSN': record['_folderRSN'], 'pRSN': record['_pRSN'] }
    url = 'https://dphregprograms.iowa.gov/PublicPortal/Iowa/IDPH/publicSearch/publicDetail.jsp'

    try:
        s = util.session_from_driver(driver)
        resp = s.post(url, data=payload)
        html = resp.text

        soup = BeautifulSoup(html, 'html.parser')
        td = soup.select('#license_detail td')

        record['Status'] = td[3].get_text(strip=True)
        record['Issue'] = td[4].get_text(strip=True)
        record['Expiration'] = td[5].get_text(strip=True)

        td = soup.select('#folder_freeform_div table td')
        record['Type'] = td[0].get_text(strip=True)
        record['Speciality'] = td[1].get_text(strip=True)
    except Exception: 
        pass

    return record

def unique_record(record):
    return tuple( record[k] for k in KEYS )

def main():

    filename = 'iowa_ex.json'
    results = util.read_json(filename)
    scraped = set( unique_record(x) for x in results )

    driver = webdriver.Chrome()
    
    for record in scrape_plumbers(driver):
        if unique_record(record) not in scraped:
            print('Scraping details', record['Name'])
            details = scrape_details(driver, record)
            results.append(details)
            util.write_json(filename, results)
        else:
            print('Already scraped', record['License'])

    util.write_json('iowa_ex.json', results)

if __name__ == '__main__':
    main()

