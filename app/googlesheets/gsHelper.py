import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd
from app import db
import os
import sys
from itertools import islice
from datetime import datetime
from gspread_dataframe import get_as_dataframe, set_with_dataframe

scopes = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

collections = [
    {
        'name':'Covid Tests',
        'table': 'covidtests',
        'endpoint':'/data/covidtests'
    },
    {
        'name':'Covid',
        'table': 'covid',
        'endpoint':'/data/covid'
    },
    {
        'name':'International Data',
        'table': 'internationaldata',
        'endpoint':'/data/internationaldata'
    },
    {
        'name':'ICU Capacity',
        'table': 'icucapacity',
        'endpoint':'/data/icucapacity'
    },
    {
        'name':'PHU Capacity',
        'table': 'phucapacity',
        'endpoint':'/data/phucapacity'
    },
    {
        'name':'Source',
        'table': 'source',
        'endpoint':'/data/source'
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

def updateSheet(dataSource, sh):
    try:
        df = pd.read_sql_table(dataSource['table'], db.engine)
        sheet = getSheet(dataSource['name'], sh, df.shape[0], df.shape[1])
        # Update only when row count exceeds sheet row count
        # Not this doesn't account for potentially empty rows at end of sheet
        if df.shape[0] >= sheet.row_count:
            print("Updating google sheet", dataSource['name'])
            set_with_dataframe(sheet, df, row=1, col=1, include_index=False, include_column_header=True, resize=True, allow_formulas=True)

        # Row by row, slower but less data
        #max_rows = len(sheet.get_all_values())
        # Add header if not exists
        #if max_rows <= 0:
        #    print("Inserting header")
        #    sheet.insert_row(df.columns.values.tolist(), index=1)


        # Start at rows after sheet length
        #for index, row in islice(df.iterrows(), max_rows-1, None): 
        #for index, row in df.iterrows():
            #values = [parseVal(x) for x in row.values.tolist()]
            #sheet.insert_row(values, index+2)
            #print("Insert row index ", index+2)
 
    except:
        print("Failed to update google sheet", dataSource['name'], sys.exc_info())

def dumpTablesToSheets():
    creds = ServiceAccountCredentials.from_json_keyfile_name('googleapi_client_secret.json', scopes)
    client = gspread.authorize(creds)
    sh = client.open("COVID-19 Data")

    for x in collections:
        updateSheet(x, sh)
