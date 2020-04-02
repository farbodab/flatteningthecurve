import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from app import db
import os
import sys
from itertools import islice
from datetime import datetime
from gspread_dataframe import get_as_dataframe, set_with_dataframe
from app import api
from flask import current_app as app
import json

scopes = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

'''{
    'name':'Results Date',
    'data':api.routes.get_date,
    'region': 'province',
    'endpoint':'/covid/results/date'
},'''
collections = [
    {
        'name':'Results',
        'data':api.vis.get_results,
        'region': 'region',
        'endpoint':'/covid/results'
    },
    {
        'name':'PHU',
        'region':'region',
        'data':api.vis.get_phus,
        'endpoint':'/covid/phu'
    },
    {
        'name':'Growth',
        'region':'country',
        'data':api.vis.get_growth,
        'endpoint':'/covid/growth'
    },
    {
        'name':'Growth_Recent',
        'region':'country',
        'data':api.vis.get_growth_recent,
        'endpoint':'/covid/growth_recent'
    },
    {
        'name':'Test Results',
        'region':'type',
        'data':api.vis.get_testresults,
        'endpoint':'/covid/testresults'
    },
    {
        'name':'ICU Capacity',
        'data':api.vis.get_icu_capacity,
        'endpoint':'/covid/get_icu_capacity'
    }
]

def parseVal(val):
    if isinstance(val,pd.Timestamp):
        return datetime.strftime(val.to_pydatetime(), '%Y-%m-%d %H:%M:%S')
    else:
        return val

def getSheet(sheetName, sh, rows, cols):
    worksheet_list = [x.title for x in sh.worksheets()]
    if sheetName in worksheet_list:
        return  sh.worksheet(sheetName)
    else:
        worksheet = sh.add_worksheet(title=sheetName, rows=rows, cols=cols)
        return worksheet

def updateCollection(dataSource, sh):
    try:
        df = None
        '''
        # With the api
        with app.test_request_context(dataSource['endpoint']):
            res = dataSource['data']()
            json_data = json.loads(res.data.decode('utf-8'))
            #print("RESULTS", json_data)
            keys = []
            values = []
            region = []
            for key in json_data:
                #print("KEY", key)
                if key == 'status':
                    continue

                temp_keys = []
                temp_values = []
                for x in json_data[key]:
                    try:
                        val = int(x)
                        temp_keys.append(val)
                    except:
                        temp_keys.append(x)
                    temp_values.append(json_data[key][x])
                zipped = sorted(zip(temp_keys, temp_values))
                keys += [x for x, y in zipped]
                values += [y for x, y in zipped]
                region += [key]*len(zipped)

            data = {}
            data[dataSource['region']] = region
            data['date'] = keys
            data['value'] = values
            df = pd.DataFrame(data, columns=[dataSource['region'], 'date', 'value'])'''
        df = dataSource['data']()

        sheet = getSheet(dataSource['name'], sh, df.shape[0], df.shape[1])
        if True:
            print("Update collection", dataSource['name'])
            set_with_dataframe(sheet, df, row=1, col=1, include_index=False, include_column_header=True, resize=True, allow_formulas=True)

    except:
        print("Failed to update google sheet", dataSource['name'], sys.exc_info())

def dumpTablesToSheets():
    creds = ServiceAccountCredentials.from_json_keyfile_name('googleapi_client_secret.json', scopes)
    client = gspread.authorize(creds)
    sh = client.open("COVID-19 Data")

    for index, x in enumerate(collections):
        updateCollection(x, sh)
