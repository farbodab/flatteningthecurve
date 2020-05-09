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

target_url = 'https://pechlilab.shinyapps.io/output/'

async def test(source):
    output = './'

    try:
        source_index = get_permitted_sources().index(source) + 1
    except ValueError:
        print('Invalid source parameter {}'.format(source))
        print('Choose from {}'.format('\n'.join(get_permitted_sources())))
        return

    # Launch a headless browser
    browser = await launch()
    page = await browser.newPage()
    await page.goto(target_url)

    # TO DO: This could be better, rather than waiting an arbitrary length for the page content to load
    await page.waitFor(5000)
    await page.click('input[value="{}"]'.format(source_index))
    await page.click('#DisplayModel1')
    await page.waitFor(3000)

    page_body = await page.evaluate('''() => { return document.body.innerHTML }''')

    # Get the href target from the download button
    soup = BeautifulSoup(page_body, 'html.parser')
    data_endpoint = soup.find(id='Download').get('href')

    # Construct the download URL
    data_url = target_url + data_endpoint
    query = parse.parse_qs(parse.urlparse(data_url).query)['w'][0]
    file_url = target_url + '_w_' + query + '/' + data_endpoint

    # Download the file
    data = requests.get(file_url, allow_redirects=True)

    # Save
    # Create the output path if it doesn't exist
    #if not os.path.exists(output):
        #os.mkdir(output)
    #output_file = output + '/' + source + '_' + time.strftime('%Y-%m-%d %Hh%Mm%Ss') + '.csv'
    #open(output_file, 'wb').write(data.content)

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