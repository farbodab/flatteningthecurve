
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

collections = [
    {'name':'Results','data':api.vis.get_results},
    {'name':'PHU','data':api.vis.get_phus},
    {'name':'Growth','data':api.vis.get_growth},
    {'name':'Growth_Recent','data':api.vis.get_growth_recent},
    {'name':'Test Results','data':api.vis.get_testresults},
    {'name':'ICU Capacity','data':api.vis.get_icu_capacity},
    {'name':'ICU Capacity Province','data':api.vis.get_icu_capacity_province},
    {'name':'ICU Case Status Province','data':api.vis.get_icu_case_status_province},
    {'name':'Canada Mobility','data':api.vis.get_mobility},
    {'name':'Canada Mobility Transportation','data':api.vis.get_mobility_transportation},
    {'name':'Canada Testing','data':api.vis.get_tested},
    {'name':'Canada Deaths','data':api.vis.get_deaths},
    {'name':'Average Daily Cases (7-day rolling)','data':api.vis.get_cases_rolling_average},
    {'name':'Average Daily Deaths (7-day rolling)','data':api.vis.get_deaths_rolling_average},
    {'name':'Daily Deaths','data':api.vis.get_daily_deaths},
    {'name':'Top Causes','data':api.vis.get_top_causes}
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
        df = dataSource['data']()
        sheet = getSheet(dataSource['name'], sh, df.shape[0], df.shape[1])
        print("Update collection", dataSource['name'])
        set_with_dataframe(sheet, df, row=1, col=1, include_index=False, include_column_header=True, resize=True, allow_formulas=True)

    except:
        print("Failed to update google sheet", dataSource['name'], sys.exc_info())

def exportToSheets():
    creds = ServiceAccountCredentials.from_json_keyfile_name('googleapi_client_secret.json', scopes)
    client = gspread.authorize(creds)
    sh = None
    if os.getenv('FLASK_CONFIG') == 'production':
        print("Using Production Sheet COVID-19 Data")
        sh = client.open("COVID-19 Data")
    else:
        print("Using Dev Sheet COVID-19 Dev")
        sh = client.open("COVID-19 Dev")

    for index, x in enumerate(collections):
        updateCollection(x, sh)
