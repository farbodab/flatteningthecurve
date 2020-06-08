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
from dotenv import load_dotenv
load_dotenv()

scopes = ['https://www.googleapis.com/auth/spreadsheets', "https://www.googleapis.com/auth/drive.file", "https://www.googleapis.com/auth/drive"]

def parseVal(val):
    if isinstance(val,pd.Timestamp):
        return datetime.strftime(val.to_pydatetime(), '%Y-%m-%d %H:%M:%S')
    else:
        return val

def getOrCreateSheet(sheetName, sh, rows, cols):
    worksheet_list = [x.title for x in sh.worksheets()]
    if sheetName in worksheet_list:
        return  sh.worksheet(sheetName)
    else:
        worksheet = sh.add_worksheet(title=sheetName, rows=rows, cols=cols)
        return worksheet

def updateCollection(dataSource, sh):
    try:
        df = None
        if 'function' in dataSource:
            df = dataSource['function']()
        elif 'table' in dataSource:
            df = pd.read_sql_table(dataSource['table'], db.engine)
        sheet = getOrCreateSheet(dataSource['name'], sh, df.shape[0], df.shape[1])
        print("Update collection", dataSource['name'])
        set_with_dataframe(sheet, df, row=1, col=1, include_index=False, include_column_header=True, resize=True, allow_formulas=True)

    except:
        print("Failed to update google sheet", dataSource['name'], sys.exc_info())

def getVizDocument():
    items = {
      "type": os.getenv('type'),
      "project_id": os.getenv('project_id'),
      "private_key_id": os.getenv('private_key_id'),
      "private_key": os.getenv('private_key'),
      "client_id": os.getenv('client_id'),
      "client_email": os.getenv('client_email'),
      "auth_uri": os.getenv('auth_uri'),
      "token_uri": os.getenv('token_uri'),
      "auth_provider_x509_cert_url": os.getenv('auth_provider_x509_cert_url'),
      "client_x509_cert_url": os.getenv('client_x509_cert_url')
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(items, scopes)
    client = gspread.authorize(creds)
    if os.getenv('FLASK_CONFIG') == 'production':
        return client.open("COVID-19 Data")
    else:
        return client.open("COVID-19 Dev")

def exportToSheets(collections):
    doc = getVizDocument()

    for index, x in enumerate(collections):
        updateCollection(x, doc)

def getVizSheet(sheetName):
    doc = getVizDocument()
    sheet = doc.worksheet(sheetName)
    return sheet

def getSheet(document, sheetName):

    items = {
      "type": os.environ['type'],
      "project_id": os.environ['project_id'],
      "private_key_id": os.environ['private_key_id'],
      "private_key": os.environ['private_key'],
      "client_id": os.environ['client_id'],
      "auth_uri": os.environ['auth_uri'],
      "token_uri": os.environ['token_uri'],
      "auth_provider_x509_cert_url": os.environ['auth_provider_x509_cert_url'],
      "client_x509_cert_url": os.environ['client_x509_cert_url']
    }
    creds = ServiceAccountCredentials.from_json_keyfile_dict(items, scopes)
    client = gspread.authorize(creds)
    sh = client.open(document)
    sheet = sh.worksheet(sheetName)
    return sheet

def readSheet(document, sheetName, lazy=False):
    sheet = getSheet(document, sheetName)

    if lazy:
        for line in range(3,sheet.col_count):
            yield sheet.row_values(line)
    else:
        for line in sheet.get_all_values()[1:]:
            yield line
