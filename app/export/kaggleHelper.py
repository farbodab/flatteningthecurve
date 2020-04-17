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
from app.api.vis import *

# Put all datasets here
collections = [
    {'table': 'canadamortality', 'filename':'canada_mortality.csv'},
    {'table': 'canadarecovered', 'filename':'canada_recovered.csv'},
    {'table': 'phucapacity', 'filename':'icu_capacity_on.csv'},
    {'table': 'icucapacity', 'filename':'icucapacity.csv'},
    {'table': 'internationalmortality', 'filename':'international_mortality.csv'},
    {'table': 'internationalrecovered', 'filename':'international_recovered.csv'},
    {'table': 'npiinterventions', 'filename':'npi_canada.csv'},
    {'table': 'npiinterventions_usa', 'filename':'npi_usa.csv'},
    {'table': 'covid', 'filename':'test_data_canada.csv'},
    {'table': 'internationaldata', 'filename':'test_data_intl.csv'},
    {'table': 'covidtests', 'filename':'test_data_on.csv'},
    {'table': 'governmentresponse', 'filename':'governmentresponse.csv'},
    {'function': get_mobility, 'filename':'vis_canada_mobility.csv'},
    {'function': get_mobility_transportation, 'filename':'vis_canada_mobility_transportation.csv'},
    {'function': get_growth_recent, 'filename':'vis_growthrecent.csv'},
    {'function': get_icu_capacity, 'filename':'vis_icucapacity.csv'},
    {'function': get_icu_capacity_province, 'filename':'vis_icucapacityprovince.csv'},
    {'function': get_icu_case_status_province, 'filename':'vis_icucasestatusprovince.csv'},
    {'function': get_phus, 'filename':'vis_phu.csv'},
    {'function': get_results, 'filename':'vis_results.csv'},
    {'function': get_testresults, 'filename':'vis_testresults.csv'},
] 

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


def exportToKaggle():
    exportDataset(collections, 'covid19-challenges', 'HowsMyFlattening COVID-19 Challenges')

def exportDataset(data, dataset, title):
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

    sets = [x.ref.split('/')[-1] for x in api.dataset_list(user='HowsMyFlattening')]
    print("NEW SET", dataset, "EXISTING", sets)

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
