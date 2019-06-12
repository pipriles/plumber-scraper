#!/usr/bin/env python3

import pandas as pd
import requests as rq
import util
import re

from bs4 import BeautifulSoup
from collections import OrderedDict

from selenium import webdriver
from selenium.webdriver.support.ui import Select
from selenium.common.exceptions import NoSuchElementException

URL = 'https://dltweb.dlt.ri.gov/profregsonline/LicenseSearch'
FILENAME = 'rhodeisland.json'

def prepare_scrape(driver):

    driver.get(URL)

    # Select Plumber
    elem = Select(driver.find_element_by_id('MainContent_cboTrade'))
    elem.select_by_value('Plumber')

    # Click search button
    elem = driver.find_element_by_id('MainContent_btnSearch')
    elem.click()

def click_plumber(driver, index):

    id_ = 'MainContent_gvLicenseSearchResults_lbLicenseDetails_'
    path = '//a[starts-with(@id, "{}")]'.format(id_)

    elems = driver.find_elements_by_xpath(path)
    elems[index].click()

def click_page(driver, page):

    try:
        path = '//a[text()="{}"]'.format(page)
        elem = driver.find_element_by_xpath(path)
        elem.click()

    except NoSuchElementException:
        query = '#MainContent_gvLicenseSearchResults table a'
        elems = driver.find_elements_by_css_selector(query)

        # click next pages button
        label = elems[-1].get_property('innerText')
        if label == '...': 
            elems[-1].click()

def find_field_value(soup, field):

    elem = soup.find(string=field)
    if elem is None: return ''
    span = elem.find_parent('div').span
    return span.get_text(strip=True)

def extract_details(driver):

    data = dict()
    keys = [ 'License', 'Status', 'Code', 'Issue' ]

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    data['Name'] = find_field_value(soup, 'Name')
    data['Address'] = find_field_value(soup, 'Address')
    data['Home Phone'] = find_field_value(soup, 'Home Phone')
    data['Business Phone'] = find_field_value(soup, 'Business Phone')
    data['Company'] = find_field_value(soup, 'Company')
    data['Expiration Date'] = find_field_value(soup, 'Expiration Date')
    data['Insurance Name'] = find_field_value(soup, 'Insurance Name')
    data['Insurance Termination Date'] = \
            find_field_value(soup, 'Insurance Termination Date')

    elem = soup.find(id='MainContent_gvLicenses')
    cols = elem.find_all('td')

    for k, td in zip(keys, cols):
        data[k] = td.get_text(strip=True)

    return data

def pl_license(driver, index):

    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')

    id_ = r'^MainContent_gvLicenseSearchResults_lbLicenseDetails_'
    elem = soup.find_all(id=re.compile(id_))

    return elem[index].get_text(strip=True)

def scrape_current_page(driver, scraped):

    for index in range(8):
        pl = pl_license(driver, index)
        if pl in scraped:
            print('Already scraped', pl)
            continue

        click_plumber(driver, index)
        yield extract_details(driver)

def scrape_plumbers(driver):

    records = util.read_json(FILENAME)
    scraped = set( x['License'] for x in records )

    for p in range(2, 259):
        yield from scrape_current_page(driver, scraped)
        print('Clicking page', p)
        click_page(driver, p)

def format_record(record):

    data = OrderedDict.fromkeys(util.COLUMNS)

    data['Name'] = record['Name']
    data['Street Address 1'] = record['Address']

    data['Phone'] = record['Home Phone']
    data['Business Phone'] = record['Business Phone']

    data['Company'] = record['Company']

    data['License Number'] = record['License']
    data['License Status'] = record['Status']
    data['License Code'] = record['Code']

    data['Issue Date'] = record['Issue']
    data['Expiration Date'] = record['Expiration Date']

    data['Insurance Name'] = record['Insurance Name']
    data['Insurance Termination date'] = \
            record['Insurance Termination Date']
    
    return data

def export_csv():
    data = util.read_json('rhodeisland.json')
    df = pd.DataFrame([ format_record(x) for x in data ])
    df.to_csv('./rhodeisland.csv', index=None)

def main():

    driver = webdriver.Chrome()
    prepare_scrape(driver)

    results = util.read_json(FILENAME)

    for pl in scrape_plumbers(driver):
        print(pl['License'])
        results.append(pl)
        util.write_json(FILENAME, results)

if __name__ == '__main__':
    main()

