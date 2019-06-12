#!/usr/bin/env python3

import json
import requests as rq
import pandas as pd

from selenium import webdriver

COLUMNS = [
    'File',
    'Last Name',
    'First Name',
    'Street Address 1',
    'Street Address 2',
    'City',
    'State',
    'Zip Code',
    'Phone',
    'Email',
    'License Number',
    'License Status',
    'Company Domain',
    'Company'
]

def read_json(filename):
    try:
        data = []
        with open(filename, 'r', encoding='utf8') as fp:
            data = json.load(fp)
    except FileNotFoundError: pass
    return data

def write_json(filename, data):
    with open(filename, 'w', encoding='utf8') as fp:
        json.dump(data, fp, indent=2)

def session_from_driver(driver):
    s = rq.Session()
    for ck in driver.get_cookies():
        s.cookies.set(ck['name'], ck['value'])
    return s

