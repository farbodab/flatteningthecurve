from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app import db
from app.models import *
from app.data_transform import bp
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import glob, os
import numpy as np
from scipy import stats as sps
from scipy.interpolate import interp1d
from sqlalchemy import sql
import subprocess

def get_file_path(data, step='processed', today=datetime.today().strftime('%Y-%m-%d')):
    source_dir = 'data/' + data['classification'] + '/' + step + '/'
    if data['type'] != '':
        file_name = data['table_name'] + '_' + today + '.' + data['type']
    else:
        file_name = data['table_name'] + '_' + today
    save_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_path =  save_dir + '/' + file_name
    return file_path, save_dir

def transform_public_cases_ontario_confirmed_positive_cases():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'conposcovidloc',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_cases_canada_confirmed_positive_cases():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'cases',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_cases_canada_confirmed_mortality_cases():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'mortality',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'cases', 'table_name':'canada_confirmed_mortality_cases',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_cases_canada_recovered_aggregated():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'recovered_cumulative',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'cases', 'table_name':'canada_recovered_aggregated',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_cases_international_cases_aggregated():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_confirmed_global',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'cases', 'table_name':'international_cases_aggregated',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_cases_international_mortality_aggregated():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_deaths_global',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'cases', 'table_name':'international_mortality_aggregated',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_cases_international_recovered_aggregated():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_recovered_global',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'cases', 'table_name':'international_recovered_aggregated',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_testing_international_testing_aggregated():
    data = {'classification':'public', 'source_name':'owid', 'table_name':'covid_testing_all_observations',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'testing', 'table_name':'international_testing_aggregated',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_testing_canada_testing_aggregated():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'testing_cumulative',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'testing', 'table_name':'canada_testing_aggregated',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_interventions_canada_non_pharmaceutical_interventions():
    data = {'classification':'public', 'source_name':'howsmyflattening', 'table_name':'npi_canada',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'interventions', 'table_name':'canada_non_pharmaceutical_interventions',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_interventions_international_non_pharmaceutical_interventions():
    data = {'classification':'public', 'source_name':'oxcgrt', 'table_name':'oxcgrt_latest',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'interventions', 'table_name':'international_non_pharmaceutical_interventions',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_mobility_apple():
    data = {'classification':'public', 'source_name':'apple', 'table_name':'applemobilitytrends',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'mobility', 'table_name':'apple',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

def transform_public_mobility_google():
    data = {'classification':'public', 'source_name':'google', 'table_name':'global_mobility_report',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'mobility', 'table_name':'google',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public')
def transform_confidential_moh_iphis():
    data = {'classification':'restricted', 'source_name':'moh', 'table_name':'iphis',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'confidential', 'source_name':'moh', 'table_name':'iphis',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            print(file)
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
            for column in ["case_reported_date", "client_death_date"]:
                df[column] = pd.to_datetime(df[column])
            cases = df.groupby(['fsa','case_reported_date']).pseudo_id.count().reset_index()
            cases.rename(columns={'pseudo_id':'cases'},inplace=True)
            cases.rename(columns={'case_reported_date':'date'},inplace=True)
            deaths = df.groupby(['fsa','client_death_date']).pseudo_id.count().reset_index()
            deaths.rename(columns={'pseudo_id':'deaths'},inplace=True)
            deaths.rename(columns={'client_death_date':'date'},inplace=True)
            first_date = min(min(cases["date"]),min(deaths["date"]))
            last_date = max(max(cases["date"]),max(deaths["date"]))
            date_list = [first_date + timedelta(days=x) for x in range((last_date-first_date).days+1)]
            combined_df = pd.merge(cases, deaths, on=['date','fsa'],how="outer")
            combined_df.fillna(0, inplace=True)
            headers = ["date", "fsa", "cases", "deaths"]
            rows = []
            for fsa in set(combined_df["fsa"].values):
                for day in date_list:
                    temp = combined_df[combined_df["fsa"]==fsa]
                    row = temp[temp["date"] == day]
                    if len(row) == 0:
                        rows.append([day, fsa, 0.0, 0.0])
                    else:
                        rows.append([day, fsa, row["cases"].values[0], row["deaths"].values[0]])
            combined_df = pd.DataFrame(rows, columns=headers)
            combined_df.sort_values("date", inplace=True)
            combined_df["cumulative_deaths"] = combined_df.groupby('fsa')['deaths'].cumsum()
            combined_df["cumulative_cases"] = combined_df.groupby('fsa')['cases'].cumsum()

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            combined_df.to_csv(save_file, index=False)

def transform_public_socioeconomic_ontario_211_call_reports():
    data = {'classification':'confidential', 'source_name':'211', 'table_name':'call_reports',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    def convert_date(date):
        return datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y/%m/%d')
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'socioeconomic', 'table_name':'ontario_211_call_reports',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                ont_data = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
            ont_data["call_date_and_time_start"] = ont_data["call_date_and_time_start"].apply(convert_date)
            ont_data = ont_data.loc[pd.to_datetime(ont_data['call_date_and_time_start']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            day_count = {}
            for index,row in ont_data.iterrows():
                date = row['call_date_and_time_start']
                callid = row['call_report_num']
                day_count[date] = day_count.get(date,[])+[callid]
            ont_data = ont_data.drop_duplicates(subset='call_report_num', keep="first")
            ont_data = ont_data.groupby(['call_date_and_time_start']).count()
            ont_data = ont_data[['call_report_num']]
            ont_data['call_report_num_7_day_rolling_average'] = ont_data.rolling(window=7).mean()
            ont_data = ont_data.reset_index()
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            ont_data.to_csv(save_file, index=False)

def transform_public_socioeconomic_ontario_211_call_reports_by_age():
    data = {'classification':'confidential', 'source_name':'211', 'table_name':'call_reports',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    def convert_date(date):
        return datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y/%m/%d')
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'socioeconomic', 'table_name':'ontario_211_call_reports_by_age',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                ont_data = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
            ont_data["call_date_and_time_start"] = ont_data["call_date_and_time_start"].apply(convert_date)
            ont_data = ont_data.loc[pd.to_datetime(ont_data['call_date_and_time_start']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            ont_data = ont_data.groupby(['call_date_and_time_start', 'age_of_inquirer']).count().reset_index()


            Path(save_dir).mkdir(parents=True, exist_ok=True)
            ont_data.to_csv(save_file, index=False)

def transform_public_socioeconomic_ontario_211_call_per_type_of_need():
    data = {'classification':'confidential', 'source_name':'211', 'table_name':'met_and_unmet_needs',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    def convert_date(date):
        return datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y/%m/%d')
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'socioeconomic', 'table_name':'ontario_211_call_per_type_of_need',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                ont_data = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
            ont_data["date_of_call"] = ont_data["date_of_call"].apply(convert_date)
            ont_data = ont_data.loc[pd.to_datetime(ont_data['date_of_call']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            ont_data = ont_data.drop_duplicates()
            ont_data = ont_data.groupby(['date_of_call', 'airs_need_category']).count()
            ont_data.reset_index(inplace=True)
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            ont_data.to_csv(save_file, index=False)

def transform_public_capacity_ontario_icu_capacity():
    data = {'classification':'restricted', 'source_name':'ccso', 'table_name':'ccis',  'type': 'csv'}
    load_file, load_dir = get_file_path(data)
    df = pd.read_sql_table('icucapacity', db.engine)
    maxdate = df.iloc[df['date'].idxmax()]['date']
    files = glob.glob(load_dir+"/*."+data['type'])
    fields = ['region', 'lhin', 'critical_care_beds', 'critical_care_patients', 'vented_beds', 'vented_patients', 'confirmed_positive', 'confirmed_positive_ventilator']
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        date = datetime.strptime(date, '%Y-%m-%d')
        if date > maxdate:
            df = pd.read_csv(file)
            df = df.loc[(df.icu_type != 'Neonatal') & (df.icu_type != 'Paediatric')]
            df = df.groupby(['region','lhin']).sum().reset_index()
            for index,row in df.iterrows():
                c = ICUCapacity(date=date)
                for header in fields:
                    setattr(c,header,row[header])
                db.session.add(c)
                db.session.commit()
    df = pd.read_sql_table('icucapacity', db.engine)
    data_out = {'classification':'public', 'source_name':'capacity', 'table_name':'ontario_icu_capacity',  'type': 'csv'}
    save_file, save_dir = get_file_path(data_out, 'transformed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

@bp.cli.command('rt')
def transform_public_rt_canada_bettencourt_and_ribeiro_approach():
    data = {'classification':'public', 'source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'}
    load_file, load_dir = get_file_path(data, 'transformed')
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.'+data['type'])[0]
        data_out = {'classification':'public', 'source_name':'rt', 'table_name':'canada_bettencourt_and_ribeiro_approach',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'transformed', date)
        if not os.path.isfile(save_file):
            try:
                cases_df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            cases_df = cases_df.loc[cases_df.province == 'Ontario']
            replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
            "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
             "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
             "Huron Perth":"Huron County Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
              "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
              "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
              "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
              "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
              "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit",
              "Haldimand-Norfolk": "Haldimand-Norfolk Health Unit", "Leeds Grenville and Lanark": "Leeds, Grenville and Lanark District Health Unit", "Renfrew": "Renfrew County and District Health Unit",
              "Thunder Bay": "Thunder Bay District Health Unit", "Southwestern":"Southwestern Public Health Unit"}

            cases_df.health_region = cases_df.health_region.replace(replace)
            cases_df['date_report'] = pd.to_datetime(cases_df['date_report'])
            province_df = cases_df.groupby(['province', 'date_report'])['case_id'].count()
            province_df.index.rename(['health_region', 'date_report'], inplace=True)
            hr_df = cases_df.groupby(['health_region', 'date_report'])['case_id'].count()
            canada_df = pd.concat((province_df, hr_df))

            def prepare_cases(cases):
                # modification - Isha Berry et al.'s data already come in daily
                #new_cases = cases.diff()
                new_cases = cases

                smoothed = new_cases.rolling(7,
                    win_type='gaussian',
                    min_periods=1,
                    # Alf: switching to right-aligned instead of centred to prevent leakage of
                    # information from the future
                    #center=True).mean(std=2).round()
                    center=False).mean(std=2).round()

                zeros = smoothed.index[smoothed.eq(0)]
                if len(zeros) == 0:
                    idx_start = 0
                else:
                    last_zero = zeros.max()
                    idx_start = smoothed.index.get_loc(last_zero) + 1
                smoothed = smoothed.iloc[idx_start:]
                original = new_cases.loc[smoothed.index]
                return original, smoothed

            # We create an array for every possible value of Rt
            R_T_MAX = 12
            r_t_range = np.linspace(0, R_T_MAX, R_T_MAX*100+1)

            # Gamma is 1/serial interval
            # https://wwwnc.cdc.gov/eid/article/26/6/20-0357_article
            GAMMA = 1/4

            def get_posteriors(sr, window=7, min_periods=1):
                lam = sr[:-1].values * np.exp(GAMMA * (r_t_range[:, None] - 1))

                # Note: if you want to have a Uniform prior you can use the following line instead.
                # I chose the gamma distribution because of our prior knowledge of the likely value
                # of R_t.

                # prior0 = np.full(len(r_t_range), np.log(1/len(r_t_range)))
                prior0 = np.log(sps.gamma(a=3).pdf(r_t_range) + 1e-14)

                likelihoods = pd.DataFrame(
                    # Short-hand way of concatenating the prior and likelihoods
                    data = np.c_[prior0, sps.poisson.logpmf(sr[1:].values, lam)],
                    index = r_t_range,
                    columns = sr.index)

                # Perform a rolling sum of log likelihoods. This is the equivalent
                # of multiplying the original distributions. Exponentiate to move
                # out of log.
                posteriors = likelihoods.rolling(window,
                                                 axis=1,
                                                 min_periods=min_periods).sum()
                posteriors = np.exp(posteriors)

                # Normalize to 1.0
                posteriors = posteriors.div(posteriors.sum(axis=0), axis=1)

                return posteriors

            def highest_density_interval(pmf, p=.95):
                # If we pass a DataFrame, just call this recursively on the columns
                if(isinstance(pmf, pd.DataFrame)):
                    return pd.DataFrame([highest_density_interval(pmf[col]) for col in pmf],
                                        index=pmf.columns)

                cumsum = np.cumsum(pmf.values)
                best = None
                for i, value in enumerate(cumsum):
                    for j, high_value in enumerate(cumsum[i+1:]):
                        if (high_value-value > p) and (not best or j<best[1]-best[0]):
                            best = (i, i+j+1)
                            break

                low = pmf.index[best[0]]
                high = pmf.index[best[1]]
                return pd.Series([low, high], index=['Low', 'High'])


            target_regions = []
            for reg, cases in canada_df.groupby(level='health_region'):
                if cases.max() >= 30:
                    target_regions.append(reg)
            provinces_to_process = canada_df.loc[target_regions]

            results = None
            for prov_name, cases in provinces_to_process.groupby(level='health_region'):
                new, smoothed = prepare_cases(cases)
                try:
                    posteriors = get_posteriors(smoothed)
                except Exception as e:
                    print(e)
                    continue
                hdis = highest_density_interval(posteriors)
                most_likely = posteriors.idxmax().rename('ML')
                result = pd.concat([most_likely, hdis], axis=1).reset_index(level=['health_region', 'date_report'])
                if results is None:
                    results = result
                else:
                    results = results.append(result)

            results['PHU'] = results['health_region']
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            results.to_csv(save_file, index=False)

@bp.cli.command('r')
def get_r():
    data = {'classification':'public', 'source_name':'rt', 'table_name':'canada_cori_approach',  'type': 'csv'}
    load_file, load_dir = get_file_path(data, 'processed')
    load_file = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv"
    save_file, save_dir = get_file_path(data, 'transformed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    out = subprocess.check_output(f"Rscript.exe --vanilla C:/HMF/flattening-the-curve-backend/app/tools/r/Rt_ontario.r {load_file} {save_file}")
