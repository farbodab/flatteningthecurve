import asyncio
from pyppeteer import launch
from bs4 import BeautifulSoup
from urllib import parse
import requests
import time
import os.path
from os import path
import argparse
import csv
import pandas as pd

target_url = 'https://art-bd.shinyapps.io/Ontario_Health_Unit_IDEA_model/'

async def test(source):
    output = './'

    try:
        source_index = get_permitted_sources().index(source) + 1
    except ValueError:
        print('Invalid source parameter {}'.format(source))
        print('Choose from {}'.format(get_permitted_sources().join('\n')))
        return

    # Launch a headless browser
    browser = await launch()
    page = await browser.newPage()
    await page.goto(target_url)

    # TO DO: This could be better, rather than waiting an arbitrary length for the page content to load
    await page.waitFor(5000)

    page_body = await page.evaluate('''() => { return document.body.innerHTML }''')

    # Get the href target from the download button
    soup = BeautifulSoup(page_body, 'html.parser')
    data_endpoint = soup.find(id=get_id_for_source(source)).get('href')

    # Construct the download URL
    data_url = target_url + data_endpoint
    query = parse.parse_qs(parse.urlparse(data_url).query)['w'][0]
    file_url = target_url + '_w_' + query + '/' + data_endpoint

    # Download the file
    data = requests.get(file_url, allow_redirects=True)

    # Save
    '''if not os.path.exists(output):
        os.mkdir(output) 
    output_file = output + '/' + source + '_' + time.strftime('%Y-%m-%d %Hh%Mm%Ss') + '.csv'
    # Create the output path if it doesn't exist
    open(output_file, 'wb').write(data.content)'''
    lines = data.content.decode("utf-8").split('\n')
    reader = csv.reader(lines)
    parsed_csv = list(reader)
    df = pd.DataFrame(data=parsed_csv[1:], columns=parsed_csv[0])
    return df

def get_id_for_source(source):
    return {'on':'downloadONData', 'health_unit':'downloadData'}[source]

def get_permitted_sources():
    return ['on', 'health_unit']
