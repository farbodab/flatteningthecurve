from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app import db
from app.models import *
from app.data_transform import bp
from datetime import datetime, timedelta
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import glob
import os
from sqlalchemy import sql

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
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
            for column in ["case_reported_date", "client_death_date"]:
                df[column] = pd.to_datetime(df[column])
            cases = df.groupby(['fsa','case_reported_date']).agg({'pseudo_id':'sum'}).reset_index()
            cases.rename(columns={'pseudo_id':'cases'},inplace=True)
            cases.rename(columns={'case_reported_date':'date'},inplace=True)
            deaths = df.groupby(['fsa','client_death_date']).agg({'pseudo_id':'sum'}).reset_index()
            deaths.rename(columns={'pseudo_id':'deaths'},inplace=True)
            deaths.rename(columns={'client_death_date':'date'},inplace=True)
            first_date = min(min(cases["date"]),min(deaths["date"]))
            last_date = max(max(cases["date"]),max(deaths["date"]))
            date_list = [first_date + timedelta(days=x) for x in range((last_date-first_date).days+1)]
            combined_df = pd.merge(cases, deaths, on=['date','fsa'],how="outer")
            combined_df.fillna(0, inplace=True)
            headers = ["date", "fsa", "cases", "deaths"]
            rows = []
            for FSA in set(combined_df["fsa"].values):
                for day in date_list:
                    temp = combined_df[combined_df["fsa"]==FSA]
                    row = temp[temp["date"] == day]
                    if len(row) == 0:
                        rows.append([day, FSA, 0.0, 0.0])
                    else:
                        rows.append([day, FSA, row["cases"].values[0], row["deaths"].values[0]])
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
