from urllib import parse
import requests
import time
import os.path
from os import path
import argparse
import csv
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException

import time

target_url = 'https://art-bd.shinyapps.io/Ontario_Health_Unit_IDEA_model/'

async def test(source):
    output = './'

    try:
        source_index = get_permitted_sources().index(source) + 1
    except ValueError:
        print('Invalid source parameter {}'.format(source))
        print('Choose from {}'.format(get_permitted_sources().join('\n')))
        return

    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(10)
    driver.get(target_url)

    try:
        button = driver.find_element_by_id(get_id_for_source(source))
    except NoSuchElementException:
        button = driver.find_element_by_link_text(get_text_for_source(source))
    # Button href is set by a script so have to wait
    time.sleep(5)
    data_endpoint = button.get_attribute('href')
    if not data_endpoint:
        raise Exception("couldn't find button")

    # Download the file
    data = requests.get(data_endpoint, allow_redirects=True)

    lines = data.content.decode("utf-8").split('\n')
    reader = csv.reader(lines)
    parsed_csv = list(reader)
    df = pd.DataFrame(data=parsed_csv[1:], columns=parsed_csv[0])
    return df

def get_id_for_source(source):
    return {'on':'downloadONData', 'health_unit':'downloadData'}[source]

def get_text_for_source(source):
    return {'on':'Download Ontario Projections', 'health_unit':'Download Health Unit Projections'}[source]

def get_permitted_sources():
    return ['on', 'health_unit']
