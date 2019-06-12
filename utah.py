#!/usr/bin/env python3

import requests as rq
import pandas as pd
import re
import util
import sys
import time
import nameparser

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from collections import OrderedDict

URL = 'https://secure.utah.gov/llv/search/index.html'
KEYS = ('Name', 'City', 'Profession', 'License', 'Status')

def wait_for_elem(driver, by, query, timeout=20):
    # event = EC.visibility_of
    event = EC.element_to_be_clickable((by, query))
    wait = WebDriverWait(driver, timeout)
    elem = wait.until(event)
    return elem

def prepare_scrape(driver: webdriver.Chrome):

    driver.get(URL)

    # Click on Plumbers
    elem = wait_for_elem(driver, By.NAME, 'item247')
    elem.click()

    # Click on Apprentice Plumber
    elem = wait_for_elem(driver, By.NAME, 'item247_1')
    elem.click()

    # Click on Journeyman Plumber
    elem = wait_for_elem(driver, By.NAME, 'item247_2')
    elem.click()

    # Click on Master Plumbers
    elem = wait_for_elem(driver, By.NAME, 'item247_3')
    elem.click()

    # Click search button
    query = '#toggleMulticolumn + p > input[value="Search"]'
    elem = wait_for_elem(driver, By.CSS_SELECTOR, query)
    elem.click()

def fetch_plumbers(driver, page):

    url = 'https://secure.utah.gov/llv/search/search.html?currentPage={}'
    url = url.format(page)
    driver.get(url)
    html = driver.page_source
    return html

def parse_row(soup):

    data = dict.fromkeys(KEYS)
    cols = soup.find_all('td', recursive=False)

    for k, td in zip(KEYS, cols):
        data[k] = td.get_text(strip=True)

    url = 'https://secure.utah.gov/llv/search/'
    anchor = soup.find('a')
    data['href'] = url + anchor['href']

    return data

def extract_plumbers(html):
    
    soup = BeautifulSoup(html, 'html.parser')
    elem = soup.find('table', class_='resultsTable')
    rows = elem.tbody.find_all('tr', class_=re.compile(r'^bg_'))
    print('ROWS', len(rows))

    for tr in rows:
        yield parse_row(tr)

def scrape_plumbers(driver, start=1):

    for p in range(start, 352):
        print('Fetching page', p)
        html = fetch_plumbers(driver, p)
        yield from extract_plumbers(html)
        time.sleep(1)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['Name'])

    data['File'] = record['href']
    data['Last Name'] = name.last
    data['First Name'] = name.first
    data['City'] = record['City']
    data['License Number'] = record['License']
    data['License Status'] = record['Status']
    data['Full Name'] = record['Name']

    data['Profession'] = record['Profession']
    data['License Type'] = record['Type']

    data['Stree Address 1'] = record['Address']
    data['Obtained'] = record['Obtained']

    data['Issue Date'] = record['Issue']
    data['Expiration Date'] = record['Expiration']

    return data

def export_csv():
    data = util.read_json('utah_ex.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./utah.csv', index=None)

def find_by_string(soup, string):
    elem = soup.find('td', string=string)
    td = elem.find_next_sibling('td')
    return td.get_text(strip=True)

def scrape_details(record):

    url = record['href']

    resp = rq.get(url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')

    record['Address'] = find_by_string(soup, 'City, State, Zip, Country:')
    record['Profession'] = find_by_string(soup, 'Profession:')
    record['Type'] = find_by_string(soup, 'License Type:')
    record['Obtained'] = find_by_string(soup, 'Obtained By:')
    record['Issue'] = find_by_string(soup, 'Original Issue Date:')
    record['Expiration'] = find_by_string(soup, 'Expiration Date:')

    return record

def unique_record(record):
    return record['href']

def main():

    # driver = webdriver.Chrome()
    # prepare_scrape(driver)

    filename = 'utah_ex.json'

    records = util.read_json('utah.json')
    results = util.read_json(filename)
    scraped = set( unique_record(x) for x in results )

    try:
        for record in records:
            if unique_record(record) in scraped:
                print('Already scraped', record['License'])
                continue

            print('Scraping details', record['License'])
            details = scrape_details(record)
            results.append(details)
            util.write_json(filename, results)

    except Exception: 
        print('Some bullshit error...')
    except KeyboardInterrupt: 
        pass
    finally:
        util.write_json(filename, results)

if __name__ == '__main__':
    main()

