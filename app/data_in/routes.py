from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app.data_in import bp
from datetime import datetime
import requests
from pathlib import Path

def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)


def get_public_csv(source_name, table_name, type, url):
    source_dir = 'data/public/raw/'
    today = datetime.today().strftime('%Y-%m-%d')
    file_name = table_name + '_' + today + '.' + type
    save_dir = source_dir + source_name + '/' + table_name
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    save_path =  save_dir + '/' + file_name
    try:
        download_url(url, save_path)
        return f"{save_path} downloaded"
    except Exception as e:
        f"Failed to get {save_path}"
        return e

def get_public_ontario_gov_conposcovidloc():
    item = {'source_name':'ontario_gov', 'table_name':'conposcovidloc',  'type': 'csv','url':"https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_ontario_gov_covidtesting():
    item = {'source_name':'ontario_gov', 'table_name':'covidtesting',  'type': 'csv','url':"https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/ed270bb8-340b-41f9-a7c6-e8ef587e6d11/download/covidtesting.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_howsmyflattening_npi_canada():
    item = {'source_name':'howsmyflattening', 'table_name':'npi_canada',  'type': 'csv','url':"https://raw.githubusercontent.com/jajsmith/COVID19NonPharmaceuticalInterventions/master/npi_canada.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_open_data_working_group_cases():
    item = {'source_name':'open_data_working_group', 'table_name':'cases',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_open_data_working_group_mortality():
    item = {'source_name':'open_data_working_group', 'table_name':'mortality',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_open_data_working_recovered_cumulative():
    item = {'source_name':'open_data_working_group', 'table_name':'recovered_cumulative',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/recovered_cumulative.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_open_data_working_testing_cumulative():
    item = {'source_name':'open_data_working_group', 'table_name':'testing_cumulative',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/testing_cumulative.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_google_global_mobility_report():
    item = {'source_name':'google', 'table_name':'global_mobility_report',  'type': 'csv','url':"https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_apple_applemobilitytrends():
    item = {'source_name':'apple', 'table_name':'applemobilitytrends',  'type': 'csv','url':"https://covid19-static.cdn-apple.com/covid19-mobility-data/2010HotfixDev30/v3/en-us/applemobilitytrends-2020-06-25.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_oxcgrt_oxcgrt_latest():
    item = {'source_name':'oxcgrt', 'table_name':'oxcgrt_latest',  'type': 'csv','url':"https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_jhu_time_series_covid19_confirmed_global():
    item = {'source_name':'jhu', 'table_name':'time_series_covid19_confirmed_global',  'type': 'csv','url':"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_jhu_time_series_covid19_deaths_global():
    item = {'source_name':'jhu', 'table_name':'time_series_covid19_deaths_global',  'type': 'csv','url':"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_jhu_time_series_covid19_recovered_global():
    item = {'source_name':'jhu', 'table_name':'time_series_covid19_recovered_global',  'type': 'csv','url':"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_owid_covid_testing_all_observations():
    item = {'source_name':'owid', 'table_name':'covid_testing_all_observations',  'type': 'csv','url':"https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-all-observations.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'

def get_public_keystone_strategy_complete_npis_inherited_policies():
    item = {'source_name':'keystone_strategy', 'table_name':'complete_npis_inherited_policies',  'type': 'csv','url':"https://raw.githubusercontent.com/Keystone-Strategy/covid19-intervention-data/master/complete_npis_inherited_policies.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return 'True'
