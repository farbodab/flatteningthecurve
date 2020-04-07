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
from kaggle.api.kaggle_api_extended import KaggleApi
from datetime import date

def createMetadata(api, dirname, dataset, title):
    path = os.path.join(dirname, 'dataset-metadata.json')
    if os.path.exists(path):
        print('Metadata exists')
        return

    print('Creating metadata')

    api.dataset_initialize(dirname)

    fin = open(path, 'rt')
    data = fin.read()
    data = data.replace('INSERT_TITLE_HERE', title).replace('INSERT_SLUG_HERE', dataset)
    fin.close()

    fout = open(path, 'wt')
    fout.write(data)
    fout.close()

def exportToKaggle(data, dataset, title):
    print('Exporting dataset', dataset)
    api = KaggleApi()
    api.authenticate()

    dirname = os.path.dirname(__file__)
    dirname = os.path.join(dirname, 'datasets')
    if not os.path.exists(dirname):
        os.mkdir(dirname)
    dirname = os.path.join(dirname, dataset)
    if not os.path.exists(dirname):
        os.mkdir(dirname)

    for x in data:
        path = os.path.join(dirname, x['filename'])
        if 'table' in x:
            df = pd.read_sql_table(x['table'], db.engine)
            print("Exporting table to", x['filename'])
        elif 'function' in x:
            df = x['function']()
            print("Exporting function to", x['filename'])

        df.to_csv(path, index=False)

    createMetadata(api, dirname, dataset, title)

    sets = [x.ref.split('/')[-1] for x in api.dataset_list(user='jameshooks')]

    if dataset in sets:
        # Send to kaggle
        today = date.today()
        dt = today.strftime("%B %d, %Y")
        api.dataset_create_version(dirname, dt)
        print('Update dataset {}'.format(dataset))
    else:
        api.dataset_create_new(dirname, public=True)
        print('Create new dataset {}'.format(dataset))
    print('Dataset complete!')
