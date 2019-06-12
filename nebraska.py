#!/usr/bin/env python3

import requests as rq
import pandas as pd
import util

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from collections import OrderedDict

URL = 'https://dol.nebraska.gov/conreg/Search'
KEYS = ( 'Option', 'Registered', 'Expires' )

def prepare_scrape(driver: webdriver.Chrome):

    driver.get(URL)

    # Select Plumbing
    elem = Select(driver.find_element_by_id('AdvancedSearch_NAICSCode'))
    elem.select_by_value('23')

    # Click submit button
    elem = driver.find_element_by_xpath('//button[@type="submit"]')
    elem.click()

def scrape_plumbers(driver):

    # tricky stuff
    url = 'https://dol.nebraska.gov/conreg/Search/AdvancedSearch'
    params = '?page=1&resultsPerPage=1000'
    driver.get(url + params)
    html = driver.page_source
    yield from extract_plumbers(html)

def extract_plumbers(html):

    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.find_all('tr', class_='fieldset-outline')

    for tr in rows:
        yield parse_row(tr)

def parse_row(soup):

    data = dict.fromkeys(KEYS)
    cols = soup.find_all('td', recursive=False)

    first = cols[0]
    text = first.get_text('|', strip=True)
    info = text.split('|')
    data['Company'] = info[0]
    data['Address'] = '. '.join(info[1:])

    for k, td in zip(KEYS, cols[1:]):
        data[k] = td.get_text(strip=True)

    anchor = soup.find('a')
    url = 'https://dol.nebraska.gov'
    data['href'] = url + anchor['href']

    return data

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    address = record['Address'].split('. ')

    data['File'] = record['href']
    data['Company'] = record['Company']
    data['Street Address 1'] = address[0]
    data['Street Address 2'] = address[1]
    data['Certificate Expires'] = record['Expires']
    data['Certificate Registered'] = record['Registered']

    data['Contractor Name'] = record.get('Contractor', '')
    data['Corporation Name'] = record.get('Corporation', '')
    data['Entity'] = record.get('Entity', '')
    data['City'] = record.get('City', '')
    data['State'] = record.get('State', '')
    data['Zip Code'] = record.get('Zip', '')
    data['Phone'] = record.get('Phone', '')
    data['Registration Number'] = record.get('Registration Number', '')
    data['Employees'] = record.get('Employees', '')
    data['Worker Compensation Status'] = record.get('Worker Compensation Status', '')

    return data

def export_csv():
    data = util.read_json('nebraska_ex.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./nebraska.csv', index=None)

def scrape_details(record):

    url = record['href']

    try:
        resp = rq.get(url)
        resp.raise_for_status()

    except Exception:
        return record

    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')
    rows = soup.select('#printPage table td')
    
    record['Contractor'] = rows[0].get_text(strip=True)
    record['Corporation'] = rows[1].get_text(strip=True)
    record['Entity'] = rows[2].get_text(strip=True)
    record['City'] = rows[4].get_text(strip=True)
    record['State'] = rows[5].get_text(strip=True)
    record['Zip'] = rows[6].get_text(strip=True)
    record['Phone'] = rows[7].get_text(strip=True)
    record['Registration Number'] = rows[8].get_text(strip=True)
    record['Employees'] = rows[11].get_text(strip=True)
    record['Worker Compensation Status'] = rows[12].get_text(strip=True)

    return record

def unique_record(record):
    return record['href']

def main():

    # driver = webdriver.Chrome()
    # prepare_scrape(driver)

    filename = 'nebraska_ex.json'
    records = util.read_json('./nebraska.json')
    results = util.read_json(filename)
    scraped = set( unique_record(x) for x in results )

    for pl in records:

        if unique_record(pl) in scraped: 
            print('Already scraped', pl['Company'])
            continue

        print('Scraping details', pl['Company'])
        details = scrape_details(pl)
        results.append(details)
        util.write_json(filename, results)

    util.write_json(filename, results)

if __name__ == '__main__':
    main()

