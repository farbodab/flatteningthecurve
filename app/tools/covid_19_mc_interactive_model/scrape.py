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
import time

target_url = 'https://pechlilab.shinyapps.io/output/'

async def test(source):
    output = './'

    try:
        source_index = get_permitted_sources().index(source) + 1
    except ValueError:
        print('Invalid source parameter {}'.format(source))
        print('Choose from {}'.format('\n'.join(get_permitted_sources())))
        return

    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options)
    driver.implicitly_wait(10)
    driver.get(target_url)

    inputButton = driver.find_element_by_tag_name('input[value="{}"]'.format(source_index)) 
    inputButton.click()
    modelButton = driver.find_element_by_id('DisplayModel1')
    modelButton.click()

    time.sleep(5)
    button = driver.find_element_by_id('Download')
    data_endpoint = button.get_attribute('href')

    # Download the file
    data = requests.get(data_endpoint, allow_redirects=True)

    lines = data.content.decode("utf-8").split('\n')
    reader = csv.reader(lines)
    parsed_csv = list(reader)
    columns = parsed_csv[0]
    columns[0]='date'
    df = pd.DataFrame(data=parsed_csv[1:], columns=columns)
    return df
    
def get_permitted_sources():
    return ['base_sk', 'base_on', 'base_italy', 'expanded_sk', 'expanded_on_expected', 'expanded_italy', 'base_on_n', 'base_on_e', 'base_on_w', 'base_on_c', 'base_toronto']

#asyncio.get_event_loop().run_until_complete(main())