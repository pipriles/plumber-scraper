#!/usr/bin/env python3

import pandas as pd
import requests as rq
import json
import util
import nameparser

from bs4 import BeautifulSoup
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

URL = 'https://nhlicenses.nh.gov/verification/Search.aspx'
KEYS = ('Name', 'Profession', 'Type', 'License', 'Status')

def wait_for_elem(driver, query, timeout=20):
    # event = EC.visibility_of
    event = EC.element_to_be_clickable((By.CSS_SELECTOR, query))
    wait = WebDriverWait(driver, timeout)
    elem = wait.until(event)
    return elem

def prepare_scrape(driver):

    driver.get(URL)

    # query = 't_web_lookup__profession_name'
    # elem = driver.find_element_by_name(query)
    # elem = Select(elem)
    # elem.select_by_value('Plumbers')
    # Wait for page

    query = 't_web_lookup__license_type_name'
    elem = driver.find_element_by_name(query)
    elem = Select(elem)
    elem.select_by_value('Master Plumber')

    query = '//input[@name="sch_button"]'
    elem = driver.find_element_by_xpath(query)
    elem.click()
    # Wait for page

def parse_row(soup):
    data = {}
    cols = soup.find_all('td', recursive=False)
    for k, d in zip(KEYS, cols):
        data[k] = d.get_text(strip=True)

    anchor = soup.find('a')
    url = 'https://nhlicenses.nh.gov/verification/'
    data['href'] = url + anchor['href']
    print(data['href'])

    return data

def click_page_button(driver: webdriver.Chrome, page):

    query = '//table[@id="datagrid_results"]//a[text()={}]'

    try:
        q = query.format(page)
        elem = driver.find_element_by_xpath(q)
        elem.click()

    except NoSuchElementException:
        print('Button not found, moving on ...')
        q = query.format('"..."')
        elem = driver.find_elements_by_xpath(q)[-1]
        elem.click()

def extract_plumbers(html):

    soup = BeautifulSoup(html, 'html.parser')
    elem = soup.find('table', id='datagrid_results')
    rows = elem.tbody.find_all('tr', recursive=False)
    print(len(rows))

    for row in rows[1:-1]:
        yield parse_row(row)

def write_json(filename, data):
    with open(filename, 'w', encoding='utf8') as fp:
        json.dump(data, fp, indent=2)

def scrape_plumbers(driver):

    prepare_scrape(driver)
    html = driver.page_source
    yield from extract_plumbers(html)

    for p in range(2, 75):
        print('Clicking page', p)
        click_page_button(driver, p)
        html = driver.page_source
        yield from extract_plumbers(html)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['Name'])

    data['File'] = record['href']
    data['Last Name'] = name.last
    data['First Name'] = name.first
    data['License Number'] = record['License']
    data['License Status'] = record['Status']

    data['Full Name'] = record['Name']
    data['License Type'] = record['Type']

    data['Issue Date'] = record.get('Issue', '')
    data['Expiration Date'] = record.get('Expiration', '')

    data['Full Name'] = record['Name']

    return data

def scrape_details(driver, record):

    url = record['href']
    s = util.session_from_driver(driver)

    try:
        resp = s.get(url)
        html = resp.text

        soup = BeautifulSoup(html, 'html.parser')

        elem = soup.find(id='_ctl37__ctl1_issue_date')
        record['Issue'] = elem.get_text(strip=True) if elem else None

        elem = soup.find(id='_ctl37__ctl1_expiration_date')
        record['Expiration'] = elem.get_text(strip=True) if elem else None

    except Exception:
        print('Error extracting details!')

    return record

def unique_record(record):
    return tuple( record[k] for k in KEYS )

def export_csv():
    data = util.read_json('newhamshire_ex.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('newhamshire.csv', index=None)

def main():
    filename = 'newhamshire_ex.json'
    results = util.read_json(filename)
    scraped = set( unique_record(x) for x in results )
    driver = webdriver.Chrome()

    for record in scrape_plumbers(driver):
        if unique_record(record) not in scraped:
            print('Scraping details', record['License'])
            details = scrape_details(driver, record)
            results.append(details)
            write_json(filename, results)
        else:
            print('Already scraped', record['License'])
    write_json(filename, results)

if __name__ == '__main__':
    main()

