#!/usr/bin/env python3

# 23 Journeyman Plumber
# 24 Master Plumber

# Last 48439

import re
import util
import nameparser

from bs4 import BeautifulSoup
from collections import OrderedDict
from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException

URL = 'https://vo.licensing.hpc.texas.gov/datamart/selSearchType.do'
FILENAME = 'texas_ex.json'

def prepare_scrape(driver):

    driver.get(URL)

    # Select by county
    query = '//form[@name="BaseForm"]//a[text()="Search by County"]'
    elem = driver.find_element_by_xpath(query)
    elem.click()

    select_by_name(driver, 'boardId', '456')
    select_by_name(driver, 'licTypeId', '4561')

    query = 'continue'
    elem = driver.find_element_by_name(query)
    elem.click()

def select_by_name(driver, name, value):

    elem = Select(driver.find_element_by_name(name))
    elem.select_by_value(value)

def county_options(driver):

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    elem = soup.find(attrs={ 'name': 'countyAgencyKey' })
    opts = elem.find_all('option')

    return [ x['value'] for x in opts if x['value'] ]

def page_state(driver):

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    elems = soup.select('form[name="BaseForm"] tr td b > a')
    return tuple( a.get_text(strip=True) for a in elems )

def click_next_page(driver):

    try:
        prev = page_state(driver)
        print('Clicking next page')

        elem = driver.find_element_by_name('nextPage')
        elem.click()

        now = page_state(driver)
        return now != prev

    except NoSuchElementException:
        return False

def click_next_record(driver):

    try:
        elem = driver.find_element_by_name('nextRow')
        elem.click()
        return True

    except NoSuchElementException:
        return False

def scrape_county(driver, ct):

    print('Scraping county', ct)
    select_by_name(driver, 'countyAgencyKey', ct)

    # Click search
    elem = driver.find_element_by_name('search')
    elem.click()

    try:
        # Click first plumber
        elem = driver.find_element_by_css_selector('span.item a')
        elem.click()

    except NoSuchElementException: 
        # If there is no plumbers go back
        elem = driver.find_element_by_name('back')
        elem.click()
        return

    while True:
        # Extract plumber
        yield scrape_details(driver)

        if not click_next_record(driver):
            break

    # Click to go back
    elem = driver.find_element_by_name('newcriteria')
    elem.click()

def scrape_counties(driver):

    cts = county_options(driver)

    # Start from ...
    # cts = cts[cts.index('48439'):]

    for ct in cts:
        yield from scrape_county(driver, ct)

def scrape_details(driver):

    data = dict()

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    elem = soup.find('input', attrs={'name': 'licNumber'})
    data['License'] = elem['value']

    elems = soup.find_all('td', attrs={'class': 'dataView'})
    keys = [ 'Name', 'Type', 'Status', 
            'Expiration', 'Certification of Insurance' ]

    for k, e in zip(keys, elems):
        data[k] = e.get_text(strip=True)

    elems = soup.select('span.item table')
    keys = [ 'Address', 'Phone' ]

    for k, e in zip(keys, elems):
        data[k] = e.get_text(', ', strip=True)

    return data

def record_id(record):
    keys = [ 'License', 'Name', 'Type' ]
    return tuple( record[k] for k in keys )

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)
    name = nameparser.HumanName(record['Name'])

    keys = ['City', 'State', 'County', 'Zip Code']
    addr = [ x.strip() for x in record['Address'].split(',') ]

    for k, value in zip(keys, addr):
        data[k] = value

    data['First Name'] = name.first
    data['Last Name'] = name.last

    data['License Number'] = record['License']
    data['License Status'] = record['Status']

    data['Street Address 1'] = record.get('Address', '')
    data['Phone'] = record.get('Phone', '')

    data['Full Name'] = record['Name']
    data['License Type'] = record['Type']

    data['Expiration Date'] = record['Expiration']
    data['Certification of Insurance'] = \
            record['Certification of Insurance']

    return data

def export_csv():
    data = util.read_json('texas.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./texas.csv', index=None)

def main():

    driver = webdriver.Chrome()

    results = util.read_json(FILENAME)
    scraped = set( record_id(x) for x in results )

    print('Preparing scrape')
    prepare_scrape(driver)

    # print('Select Journeyman')
    # select_by_name(driver, 'rankId', '23')

    print('Select Master Plumber')
    select_by_name(driver, 'rankId', '24')

    print('Select 10')
    select_by_name(driver, 'pageSize', '10')

    for record in scrape_counties(driver):
        if record_id(record) not in scraped:
            print('Scraped', record['License'])
            results.append(record)
            util.write_json(FILENAME, results)
        else:
            print('Already scraped', record['License'])

if __name__ == '__main__':
    main()

