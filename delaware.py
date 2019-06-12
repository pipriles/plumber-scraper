#!/usr/bin/env python3

import pandas as pd
import json
import util
import nameparser
import multiprocessing as mp

from bs4 import BeautifulSoup
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException

URL = 'https://dpronline.delaware.gov/mylicense%20weblookup/Search.aspx'
KEYS = ('Name', 'License', 'Profession', 'Type', 'Status')

def wait_for_elem(driver, query, timeout=20):
    # event = EC.visibility_of
    event = EC.element_to_be_clickable((By.CSS_SELECTOR, query))
    wait = WebDriverWait(driver, timeout)
    elem = wait.until(event)
    return elem

def prepare_scrape(driver):

    driver.get(URL)

    query = 't_web_lookup__profession_name'
    elem = driver.find_element_by_name(query)
    elem = Select(elem)
    elem.select_by_value('Plumbing/HVACR')
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

    data = dict.fromkeys(KEYS)
    cols = soup.find_all('td')
    for k, d in zip(KEYS, cols):
        data[k] = d.get_text(strip=True)

    anchor = soup.find('a')
    url = 'https://dpronline.delaware.gov/mylicense%20weblookup/'
    data['href'] = url + anchor['href']

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
        elem = driver.find_element_by_xpath(q)
        elem.click()

def extract_plumbers(html):

    soup = BeautifulSoup(html, 'html.parser')
    elem = soup.find('table', id='datagrid_results')
    rows = elem.find_all('tr')
    for row in rows[1:-1]:
        yield parse_row(row)

def write_json(filename, data):
    with open(filename, 'w', encoding='utf8') as fp:
        json.dump(data, fp, indent=2)

def scrape_plumbers(driver):

    prepare_scrape(driver)
    html = driver.page_source
    yield from extract_plumbers(html)

    for p in range(2, 49):
        print('Clicking page', p)
        click_page_button(driver, p)
        html = driver.page_source
        yield from extract_plumbers(html)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['Name'])

    # missing profession and license type
    data['File'] = record['href']
    data['Last Name'] = name.last
    data['First Name'] = name.first
    data['License Number'] = record['License']
    data['License Status'] = record['Status']
    data['State'] = 'Delaware'
    data['License Type'] = record['Type']
    data['Profession'] = record['Profession']

    data['Full Name'] = record['Name']

    data['City'] = record['City']
    data['State'] = record['State']
    data['Zip Code'] = record['Zip']

    data['Issue Date'] = record['Issue']
    data['Expiration Date'] = record['Expiration']

    return data

def export_csv():
    data = util.read_json('delaware_details.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./delaware.csv', index=None)

def scrape_details(driver, record):

    url = record['href']

    s = util.session_from_driver(driver)
    resp = s.get(url)
    html = resp.text

    soup = BeautifulSoup(html, 'html.parser')

    elem = soup.find(id='_ctl21__ctl1_issue_date')
    record['Issue'] = elem.get_text(strip=True)

    elem = soup.find(id='_ctl21__ctl1_expiration_date')
    record['Expiration'] = elem.get_text(strip=True)

    elem = soup.find(id='_ctl26__ctl1_addr_city')
    record['City'] = elem.get_text(strip=True)

    elem = soup.find(id='_ctl26__ctl1_addr_state')
    record['State'] = elem.get_text(strip=True)

    elem = soup.find(id='_ctl26__ctl1_addr_zipcode')
    record['Zip'] = elem.get_text(strip=True)

    elem = soup.find(id='_ctl26__ctl1_addr_country')
    record['Country'] = elem.get_text(strip=True)

    return record

def unique_record(record):
    return tuple( record[k] for k in KEYS )

def main():
    filename = 'delaware_details.json'
    driver = webdriver.Chrome()
    results = util.read_json(filename)
    scraped = set([ unique_record(x) for x in results ])

    for record in scrape_plumbers(driver):
        if unique_record(record) not in scraped:
            print('Scraping details', record['License'])
            details = scrape_details(driver, record)
            results.append(details)
        else:
            print('Already scraped', record['License'])
        write_json(filename, results)

if __name__ == '__main__':
    main()
    
