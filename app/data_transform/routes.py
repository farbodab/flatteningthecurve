from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app import db
from app.models import *
from app.data_transform import bp
from datetime import datetime, timedelta
from datetime import date as datte
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import glob, os
import numpy as np
from scipy import stats as sps
from scipy.interpolate import interp1d
from sqlalchemy import sql
import subprocess
from dateutil import rrule

PHU = {'the_district_of_algoma':'The District of Algoma Health Unit',
 'brant_county':'Brant County Health Unit',
 'durham_regional':'Durham Regional Health Unit',
 'grey_bruce':'Grey Bruce Health Unit',
 'haldimand_norfolk':'Haldimand-Norfolk Health Unit',
 'haliburton_kawartha_pine_ridge_district':'Haliburton, Kawartha, Pine Ridge District Health Unit',
 'halton_regional':'Halton Regional Health Unit',
 'city_of_hamilton':'City of Hamilton Health Unit',
 'hastings_and_prince_edward_counties':'Hastings and Prince Edward Counties Health Unit',
 'chatham_kent':'Chatham-Kent Health Unit',
 'kingston_frontenac_and_lennox_and_addington':'Kingston, Frontenac, and Lennox and Addington Health Unit',
 'lambton':'Lambton Health Unit',
 'leeds_grenville_and_lanark_district':'Leeds, Grenville and Lanark District Health Unit',
 'middlesex_london':'Middlesex-London Health Unit',
 'niagara_regional_area':'Niagara Regional Area Health Unit',
 'north_bay_parry_sound_district':'North Bay Parry Sound District Health Unit',
 'northwestern':'Northwestern Health Unit',
 'city_of_ottawa':'City of Ottawa Health Unit',
 'peel_regional':'Peel Regional Health Unit',
 'perth_district':'Perth District Health Unit',
 'peterborough_county_city':'Peterborough County–City Health Unit',
 'porcupine':'Porcupine Health Unit',
 'renfrew_county_and_district':'Renfrew County and District Health Unit',
 'the_eastern_ontario':'The Eastern Ontario Health Unit',
 'simcoe_muskoka_district':'Simcoe Muskoka District Health Unit',
 'sudbury_and_district':'Sudbury and District Health Unit',
 'thunder_bay_district':'Thunder Bay District Health Unit',
 'timiskaming':'Timiskaming Health Unit',
 'waterloo':'Waterloo Health Unit',
 'wellington_dufferin_guelph':'Wellington-Dufferin-Guelph Health Unit',
 'windsor_essex_county':'Windsor-Essex County Health Unit',
 'york_regional':'York Regional Health Unit',
 'southwestern':'Southwestern Public Health Unit',
 'city_of_toronto':'City of Toronto Health Unit',
 'huron_perth_county':'Huron Perth Public Health Unit'}

Replace = {'The District of Algoma Health Unit':'Algoma Public Health Unit',
 'Brant County Health Unit':'Brant County Health Unit',
 'Durham Regional Health Unit':'Durham Region Health Department',
 'Grey Bruce Health Unit':'Grey Bruce Health Unit',
 'Haldimand-Norfolk Health Unit':'Haldimand-Norfolk Health Unit',
 'Haliburton, Kawartha, Pine Ridge District Health Unit':'Haliburton, Kawartha, Pine Ridge District Health Unit',
 'Halton Regional Health Unit':'Halton Region Health Department',
 'City of Hamilton Health Unit':'Hamilton Public Health Services',
 'Hastings and Prince Edward Counties Health Unit':'Hastings and Prince Edward Counties Health Unit',
 'Huron Perth Public Health Unit':'Huron Perth District Health Unit',
 'Chatham-Kent Health Unit':'Chatham-Kent Health Unit',
 'Kingston, Frontenac, and Lennox and Addington Health Unit':'Kingston, Frontenac and Lennox & Addington Public Health',
 'Kingston, Frontenac and Lennox and Addington Health Unit': 'Kingston, Frontenac and Lennox & Addington Public Health',
 'Lambton Health Unit':'Lambton Public Health',
 'Leeds, Grenville and Lanark District Health Unit':'Leeds, Grenville and Lanark District Health Unit',
 'Middlesex-London Health Unit':'Middlesex-London Health Unit',
 'Niagara Regional Area Health Unit':'Niagara Region Public Health Department',
 'North Bay Parry Sound District Health Unit':'North Bay Parry Sound District Health Unit',
 'Northwestern Health Unit':'Northwestern Health Unit',
 'City of Ottawa Health Unit':'Ottawa Public Health',
 'Peel Regional Health Unit':'Peel Public Health',
 'Huron Perth Public Health Unit':'Huron Perth District Health Unit',
 'Peterborough County–City Health Unit':'Peterborough Public Health',
 'Porcupine Health Unit':'Porcupine Health Unit',
 'Renfrew County and District Health Unit':'Renfrew County and District Health Unit',
 'The Eastern Ontario Health Unit':'Eastern Ontario Health Unit',
 'Simcoe Muskoka District Health Unit':'Simcoe Muskoka District Health Unit',
 'Sudbury and District Health Unit':'Sudbury & District Health Unit',
 'Thunder Bay District Health Unit':'Thunder Bay District Health Unit',
 'Timiskaming Health Unit':'Timiskaming Health Unit',
 'Waterloo Health Unit':'Region of Waterloo, Public Health',
 'Wellington-Dufferin-Guelph Health Unit':'Wellington-Dufferin-Guelph Public Health',
 'Windsor-Essex County Health Unit':'Windsor-Essex County Health Unit',
 'York Regional Health Unit':'York Region Public Health Services',
 'Southwestern Public Health Unit':'Southwestern Public Health',
 'Southwestern Health Unit': 'Southwestern Public Health',
 'City of Toronto Health Unit':'Toronto Public Health',
 'Ontario': 'Ontario'}

POP = {
    "Algoma": "Algoma Public Health Unit",
    "Brant": "Brant County Health Unit",
    "Chatham-Kent": "Chatham-Kent Health Unit",
    "Durham":"Durham Region Health Department",
    "Eastern":"Eastern Ontario Health Unit",
    "Grey Bruce":"Grey Bruce Health Unit",
    "Haldimand-Norfolk":"Haldimand-Norfolk Health Unit",
    "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
    "Halton":"Halton Region Health Department",
    "Hamilton":"Hamilton Public Health Services",
    "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
    "Huron Perth":"Huron Perth District Health Unit",
    "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac and Lennox & Addington Public Health",
    "Lambton":"Lambton Public Health",
    "Leeds Grenville and Lanark":"Leeds, Grenville and Lanark District Health Unit",
    "Middlesex-London":"Middlesex-London Health Unit",
    "Niagara":"Niagara Region Public Health Department",
    "North Bay Parry Sound":"North Bay Parry Sound District Health Unit",
    "Northwestern":"Northwestern Health Unit",
    "Ottawa":"Ottawa Public Health",
    "Peel":"Peel Public Health",
    "Peterborough":"Peterborough Public Health",
    "Porcupine":"Porcupine Health Unit",
    "Renfrew":"Renfrew County and District Health Unit",
    "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
    "Southwestern":"Southwestern Public Health",
    "Sudbury":"Sudbury & District Health Unit",
    "Thunder Bay":"Thunder Bay District Health Unit",
    "Timiskaming":"Timiskaming Health Unit",
    "Toronto":"Toronto Public Health",
    "Waterloo":"Region of Waterloo, Public Health",
    "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Public Health",
    "Windsor-Essex":"Windsor-Essex County Health Unit",
    "York":"York Region Public Health Services",
}

def convert_date(date):
    return datetime.strptime(date, '%Y-%m-%d %H:%M:%S').strftime('%Y/%m/%d')

def parse_cad(string):
    if type(string) == float:
        return string
    cad = string[1:]
    cad = cad.split('.')[0]
    cad = ''.join(cad.split(','))
    try:
        return float(cad)
    except:
        return 0

def get_dir(data, today=datetime.today().strftime('%Y-%m-%d')):
    source_dir = 'data/' + data['classification'] + '/' + data['stage'] + '/'
    load_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_name = data['table_name'] + '_' + today + '.' + data['type']
    file_path =  load_dir + '/' + file_name
    return load_dir, file_path

def get_last_file(data):
    load_dir, file_path = get_dir(data)
    files = glob.glob(load_dir + "/*." + data['type'])
    files = [file.split('_')[-1] for file in files]
    files = [file.split('.csv')[0] for file in files]
    dates = [datetime.strptime(file, '%Y-%m-%d') for file in files]
    max_date = max(dates).strftime('%Y-%m-%d')
    load_dir, file_path = get_dir(data, max_date)
    return file_path

def get_file_path(data, today=datetime.today().strftime('%Y-%m-%d')):
    source_dir = 'data/' + data['classification'] + '/' + data['stage'] + '/'
    if data['type'] != '':
        file_name = data['table_name'] + '_' + today + '.' + data['type']
    else:
        file_name = data['table_name'] + '_' + today
    save_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_path =  save_dir + '/' + file_name
    return file_path, save_dir

def get_table(data, today=datetime.today().strftime('%Y-%m-%d')):
    load_file, load_dir = get_file_path(data_in)
    files = glob.glob(load_dir + "/*." + data_in['type'])
    if len(files) > 0:
        return pd.read_csv(files[0])
    else:
        return None

def transform(data_in, data_out):
    load_file, load_dir = get_file_path(data_in)
    files = glob.glob(load_dir + "/*." + data_in['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.' + data_in['type'])[0]
        save_file, save_dir = get_file_path(data_out, date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data_in['source_name']}/{data_in['table_name']}")
                print(e)
                raise e

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            yield df, save_file, date

def process_cases(row):
    if row["rolling_pop"] >= 10:
        return "High"
    elif row["rolling_pop"] >= 5:
        return "Medium"
    elif row["rolling_pop"] >= 1:
        return "Low"
    else:
        return "Very Low"

@bp.cli.command('public_cases_ontario_confirmed_positive_cases')
def transform_public_cases_ontario_confirmed_positive_cases():
    for df, save_file, date in transform(
        data_in={'classification':'public', 'stage': 'processed','source_name':'ontario_gov', 'table_name':'conposcovidloc',  'type': 'csv'},
        data_out={'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'}):
        pop = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/hr_map.csv")
        pop = pop.loc[pop.province == "Ontario"]
        pop['health_region'] = pop['health_region'].replace(POP)
        df = pd.merge(df,pop, left_on=['reporting_phu'], right_on=['health_region'], how='left')
        df.to_csv(save_file, index=False)

@bp.cli.command('public_cases_canada_confirmed_positive_cases')
def transform_public_cases_canada_confirmed_positive_cases():
    for df, save_file, date in transform(
        data_in={'classification':'public', 'stage': 'processed','source_name':'open_data_working_group', 'table_name':'cases',  'type': 'csv'},
        data_out={'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_cases_canada_confirmed_mortality_cases')
def transform_public_cases_canada_confirmed_mortality_cases():
    for df, save_file, date in transform(
        data_in={'classification':'public', 'stage': 'processed','source_name':'open_data_working_group', 'table_name':'mortality',  'type': 'csv'},
        data_out={'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'canada_confirmed_mortality_cases',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_cases_canada_recovered_aggregated')
def transform_public_cases_canada_recovered_aggregated():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'open_data_working_group', 'table_name':'recovered_cumulative',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'canada_recovered_aggregated',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_cases_international_cases_aggregated')
def transform_public_cases_international_cases_aggregated():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'jhu', 'table_name':'time_series_covid19_confirmed_global',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'international_cases_aggregated',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_cases_international_mortality_aggregated')
def transform_public_cases_international_mortality_aggregated():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'jhu', 'table_name':'time_series_covid19_deaths_global',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'international_mortality_aggregated',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_cases_international_recovered_aggregated')
def transform_public_cases_international_recovered_aggregated():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'jhu', 'table_name':'time_series_covid19_recovered_global',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'international_recovered_aggregated',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_testing_international_testing_aggregated')
def transform_public_testing_international_testing_aggregated():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'owid', 'table_name':'covid_testing_all_observations',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'testing', 'table_name':'international_testing_aggregated',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_testing_canada_testing_aggregated')
def transform_public_testing_canada_testing_aggregated():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'open_data_working_group', 'table_name':'testing_cumulative',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'testing', 'table_name':'canada_testing_aggregated',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_interventions_canada_non_pharmaceutical_interventions')
def transform_public_interventions_canada_non_pharmaceutical_interventions():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'howsmyflattening', 'table_name':'npi_canada',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'interventions', 'table_name':'canada_non_pharmaceutical_interventions',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_interventions_international_non_pharmaceutical_interventions')
def transform_public_interventions_international_non_pharmaceutical_interventions():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'oxcgrt', 'table_name':'oxcgrt_latest',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'interventions', 'table_name':'international_non_pharmaceutical_interventions',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_mobility_apple')
def transform_public_mobility_apple():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'apple', 'table_name':'applemobilitytrends',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'mobility', 'table_name':'apple',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('public_mobility_google')
def transform_public_mobility_google():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'processed','source_name':'google', 'table_name':'global_mobility_report',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'mobility', 'table_name':'google',  'type': 'csv'}):
        df.to_csv(save_file, index=False)

@bp.cli.command('confidential_moh_iphis')
def transform_confidential_moh_iphis():
    for df, save_file, date in transform(
        data_in = {'classification':'restricted', 'stage': 'processed','source_name':'moh', 'table_name':'iphis',  'type': 'csv'},
        data_out = {'classification':'confidential', 'stage': 'transformed','source_name':'moh', 'table_name':'iphis',  'type': 'csv'}):

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
        index = pd.date_range('2020-01-01', last_date)
        temps = []
        for fsa in set(combined_df["fsa"].values):
            temp = combined_df[combined_df["fsa"]==fsa]
            temp = temp.set_index('date')
            temp = temp.reindex(index)
            temp['fsa'] = fsa
            temp = temp.fillna(0)
            temp = temp.reset_index()
            temp.rename(columns={'index':'date'})
            temps.append(temp)
        combined_df = pd.concat(temps)
        combined_df = combined_df.reset_index(drop=True)
        combined_df["cumulative_deaths"] = combined_df.groupby('fsa')['deaths'].cumsum()
        combined_df["cumulative_cases"] = combined_df.groupby('fsa')['cases'].cumsum()
        combined_df.to_csv(save_file, index=False)

@bp.cli.command('public_socioeconomic_ontario_211_call_reports')
def transform_public_socioeconomic_ontario_211_call_reports():
    for df, save_file, date in transform(
        data_in = {'classification':'confidential', 'stage': 'processed','source_name':'211', 'table_name':'call_reports',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'socioeconomic', 'table_name':'ontario_211_call_reports',  'type': 'csv'}):
        ont_data = df
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
        ont_data.to_csv(save_file, index=False)

@bp.cli.command('public_socioeconomic_ontario_211_call_reports_by_age')
def transform_public_socioeconomic_ontario_211_call_reports_by_age():
    for df, save_file, date in transform(
        data_in = {'classification':'confidential', 'stage': 'processed','source_name':'211', 'table_name':'call_reports',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'socioeconomic', 'table_name':'ontario_211_call_reports_by_age',  'type': 'csv'}):
        ont_data = df
        ont_data["call_date_and_time_start"] = ont_data["call_date_and_time_start"].apply(convert_date)
        ont_data = ont_data.loc[pd.to_datetime(ont_data['call_date_and_time_start']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
        ont_data = ont_data.groupby(['call_date_and_time_start', 'age_of_inquirer']).count().reset_index()
        ont_data.sort_values(by=['call_date_and_time_start'], inplace=True)
        needs = sorted([x for x in set(list(ont_data['age_of_inquirer'].values)) if x!="Unknown" and x!='9-May' and x!='14-Oct'])
        need_before_values = {}
        need_after_values = {}
        tokeep = []
        for need in needs:
            tmp = ont_data.loc[ont_data['age_of_inquirer'] == need].copy()
            tmp = tmp[['call_date_and_time_start', 'call_report_num']]
            tmp['CallReportNum_7day_rollingaverage'] = tmp['call_report_num'].rolling(window=7).mean()
            group_x = pd.to_datetime(tmp["call_date_and_time_start"].values)
            group_y = tmp["CallReportNum_7day_rollingaverage"].values
            need_before_values[need] = [group_y[index] for index in range(len(group_y)) if group_x[index]<=pd.to_datetime(datetime.strptime("03/16/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            need_after_values[need] = [group_y[index] for index in range(len(group_y)) if group_x[index]>pd.to_datetime(datetime.strptime("03/16/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            if need in ["Adult", "Older Adult"]:
                tokeep.append((tmp["call_date_and_time_start"].values,need,tmp["call_report_num"].values,tmp["CallReportNum_7day_rollingaverage"].values,np.nanmean(need_before_values[need])))
        header = ["call_date_and_time_start","age_of_inquirer","call_report_num","CallReportNum_7day_rollingaverage","percentage_change_from_before_quarantine"]
        rows = []
        for tuples in tokeep:
            for index in range(len(tuples[0])):
                date = tuples[0][index]
                category = tuples[1]
                CallReportNum = tuples[2][index]
                CallReportNum_7day_rollingaverage = tuples[3][index]

                if pd.to_datetime(date) < pd.to_datetime(datetime.strptime("03/16/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d')):
                    percentage_change = np.nan
                else:
                    percentage_change = round(100*((CallReportNum_7day_rollingaverage-tuples[4])/tuples[4]))

                rows.append([date,category,CallReportNum,CallReportNum_7day_rollingaverage,percentage_change])
        final_out_csv = pd.DataFrame(rows,columns=header)
        final_out_csv.to_csv(save_file, index=False)

@bp.cli.command('public_socioeconomic_ontario_211_call_per_type_of_need')
def transform_public_socioeconomic_ontario_211_call_per_type_of_need():
    for df, save_file, date in transform(
        data_in = {'classification':'confidential', 'stage': 'processed','source_name':'211', 'table_name':'met_and_unmet_needs',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'socioeconomic', 'table_name':'ontario_211_call_per_type_of_need',  'type': 'csv'}):
        ont_data = df
        ont_data["date_of_call"] = ont_data["date_of_call"].apply(convert_date)
        ont_data = ont_data.loc[pd.to_datetime(ont_data['date_of_call']) >= pd.to_datetime(datetime.strptime("01/01/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
        ont_data = ont_data.drop_duplicates()
        ont_data = ont_data.groupby(['date_of_call', 'airs_need_category']).count()
        ont_data.reset_index(inplace=True)
        ont_data.sort_values(by=['date_of_call'], inplace=True)
        needs = set(list(ont_data['airs_need_category'].values))
        tokeep = []
        need_before_values = {}
        need_after_values = {}
        for need in needs:
            tmp = ont_data.loc[ont_data['airs_need_category'] == need].copy()
            tmp = tmp[['date_of_call', 'report_need_num']]
            tmp['ReportNeedNum_7day_rollingaverage'] = tmp['report_need_num'].rolling(window=7).mean()
            group_x = pd.to_datetime(tmp["date_of_call"].values)
            group_y = tmp["ReportNeedNum_7day_rollingaverage"].values

            need_before_values[need] = [group_y[index] for index in range(len(group_y)) if group_x[index]<=pd.to_datetime(datetime.strptime("03/16/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]
            need_after_values[need] = [group_y[index] for index in range(len(group_y)) if group_x[index]>pd.to_datetime(datetime.strptime("03/16/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d'))]

            if need in ["Food/Meals", "Income Support/Financial Assistance"]:
                tokeep.append((tmp["date_of_call"].values,need,tmp["report_need_num"].values,tmp["ReportNeedNum_7day_rollingaverage"].values,np.nanmean(need_before_values[need])))
        header = ["date_of_call","airs_need_category","report_need_num","ReportNeedNum_7day_rollingaverage","percentage_change_from_before_quarantine"]
        rows = []
        for tuples in tokeep:
            for index in range(len(tuples[0])):
                date = tuples[0][index]
                category = tuples[1]
                ReportNeedNum = tuples[2][index]
                ReportNeedNum_7day_rollingaverage = tuples[3][index]

                if pd.to_datetime(date) < pd.to_datetime(datetime.strptime("03/16/2020 00:01", '%m/%d/%Y %H:%M').strftime('%Y/%m/%d')):
                    percentage_change = np.nan
                else:
                    percentage_change = round(100*((ReportNeedNum_7day_rollingaverage-tuples[4])/tuples[4]))

                rows.append([date,category,ReportNeedNum,ReportNeedNum_7day_rollingaverage,percentage_change])
        final_out_csv = pd.DataFrame(rows,columns=header)
        final_out_csv.to_csv(save_file, index=False)

@bp.cli.command('public_capacity_ontario_lhin_icu_capacity')
def transform_public_capacity_ontario_lhin_icu_capacity():
    data = {'classification':'restricted', 'stage': 'processed','source_name':'ccso', 'table_name':'ccis',  'type': 'csv'}
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
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_lhin_icu_capacity',  'type': 'csv'}
    save_file, save_dir = get_file_path(data_out)
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

@bp.cli.command('public_rt_canada_bettencourt_and_ribeiro_approach')
def transform_public_rt_canada_bettencourt_and_ribeiro_approach():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'rt', 'table_name':'canada_bettencourt_and_ribeiro_approach',  'type': 'csv'}):
        cases_df = df
        cases_df = cases_df.loc[cases_df.province == 'Ontario']
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
        results.to_csv(save_file, index=False)

@bp.cli.command('public_rt_canada_cori_approach')
def transform_public_rt_canada_cori_approach():
    data = {'classification':'public', 'stage': 'transformed','source_name':'rt', 'table_name':'canada_cori_approach',  'type': 'csv'}
    load_file, load_dir = get_file_path(data, 'processed')
    load_file = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/timeseries_prov/cases_timeseries_prov.csv"
    save_file, save_dir = get_file_path(data, 'transformed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    output = subprocess.check_output(f"Rscript.exe --vanilla app/tools/r/Rt_ontario.r")

@bp.cli.command('public_capacity_ontario_phu_icu_capacity')
def transform_public_capacity_ontario_phu_icu_capacity():
    replace = {
    'Lakeridge Health Corporation':"Durham Regional Health Unit",
    "Alexandra Hospital":"Southwestern Public Health Unit",
    "Alexandra Marine and General Hospital":"Huron Perth Public Health Unit",
    "Sunnybrook Health Sciences Centre":"City of Toronto Health Unit",
    "Quinte Healthcare Corporation":"Hastings and Prince Edward Counties Health Unit",
    "Scarborough Health Network":"City of Toronto Health Unit",
    "William Osler Health Centre":"Peel Regional Health Unit",
    "Brant Community Healthcare System":"Brant County Health Unit",
    "Cambridge Memorial Hospital":"Waterloo Health Unit",
    "Brockville General Hospital":"Leeds, Grenville and Lanark District Health Unit",
    "St. Joseph's Healthcare System":"City of Hamilton Health Unit",
    "Chatham-Kent Health Alliance":"Chatham-Kent Health Unit",
    "London Health Sciences Centre":"Middlesex-London Health Unit",
    "Children's Hospital of Eastern Ontario - Ottawa Children's Treatment Centre":"City of Ottawa Health Unit",
    "The Ottawa Hospital":"City of Ottawa Health Unit",
    "Collingwood General and Marine Hospital":"Simcoe Muskoka District Health Unit",
    "Erie Shores Healthcare":"Windsor-Essex County Health Unit",
    "Hamilton Health Sciences":"City of Hamilton Health Unit",
    "Halton Healthcare Services Corporation":"Halton Regional Health Unit",
    "Georgian Bay General Hospital":"Simcoe Muskoka District Health Unit",
    "Perth and Smiths Falls District Hospital":"Leeds, Grenville and Lanark District Health Unit",
    "Niagara Health System":"Niagara Regional Area Health Unit",
    "Guelph General Hospital":"Wellington-Dufferin-Guelph Health Unit",
    "Hawkesbury and District General Hospital":"The Eastern Ontario Health Unit",
    "Hopital Montfort":"City of Ottawa Health Unit",
    "Peterborough Regional Health Centre":"Peterborough County–City Health Unit",
    "Muskoka Algonquin Healthcare":"Simcoe Muskoka District Health Unit",
    "Joseph Brant Hospital":"Halton Regional Health Unit",
    "Kingston Health Sciences Centre":"Kingston, Frontenac, and Lennox and Addington Health Unit",
    "Kirkland and District Hospital":"Timiskaming Health Unit",
    "Grand River Hospital Corporation":"Waterloo Health Unit",
    "Lake-of-the-Woods District Hospital":"Northwestern Health Unit",
    "Lennox and Addington County General Hospital":"Kingston, Frontenac, and Lennox and Addington Health Unit",
    "Mackenzie Health":"York Regional Health Unit",
    "Markham Stouffville Hospital":"York Regional Health Unit",
    "Cornwall Community Hospital":"The Eastern Ontario Health Unit",
    "Windsor Regional Hospital":"Windsor-Essex County Health Unit",
    "Toronto East Health Network":"City of Toronto Health Unit",
    "Trillium Health Partners":"Peel Regional Health Unit",
    "Sinai Health System":"City of Toronto Health Unit",
    "Norfolk General Hospital":"Haldimand-Norfolk Health Unit",
    "North Bay Regional Health Centre":"North Bay Parry Sound District Health Unit",
    "North York General Hospital":"City of Toronto Health Unit",
    "Northumberland Hills Hospital":"Haliburton, Kawartha, Pine Ridge District Health Unit",
    "Headwaters Health Care Centre":"Wellington-Dufferin-Guelph Health Unit",
    "Orillia Soldiers Memorial Hospital":"Simcoe Muskoka District Health Unit",
    "Grey Bruce Health Services":"Grey Bruce Health Unit",
    "Pembroke Regional Hospital Inc.":"Renfrew County and District Health Unit",
    "Queensway - Carleton Hospital":"City of Ottawa Health Unit",
    "Health Sciences North":"Sudbury and District Health Unit",
    "Renfrew Victoria Hospital":"Renfrew County and District Health Unit",
    "Ross Memorial Hospital":"Haliburton, Kawartha, Pine Ridge District Health Unit",
    "Royal Victoria Regional Health Centre":"Simcoe Muskoka District Health Unit",
    "Bluewater Health":"Lambton Health Unit",
    "Sault Area Hospital":"The District of Algoma Health Unit",
    "Sensenbrenner Hospital":"Porcupine Health Unit",
    "Southlake Regional Health Centre":"York Regional Health Unit Public Health",
    "Unity Health Toronto":"City of Toronto Health Unit",
    "St. Joseph's General Hospital":"City of Toronto Health Unit",
    "St. Mary's General Hospital":"Waterloo Health Unit",
    "St. Thomas-Elgin General Hospital":"Southwestern Public Health Unit",
    "Stevenson Memorial Hospital":"Simcoe Muskoka District Health Unit",
    "Stratford General Hospital":"Huron Perth Public Health Unit",
    "Strathroy Middlesex General Hospital":"Middlesex-London Health Unit",
    "Temiskaming Hospital":"Timiskaming Health Unit",
    "The Hospital for Sick Children":"City of Toronto Health Unit",
    "Thunder Bay Regional Health Sciences Centre":"Thunder Bay District Health Unit",
    "Tillsonburg District Memorial Hospital":"Southwestern Public Health Unit",
    "Timmins and District General Hospital":"Porcupine Health Unit",
    "University Health Network":"City of Toronto Health Unit",
    "University of Ottawa Heart Institute":"City of Ottawa Health Unit",
    "West Parry Sound Health Centre":"North Bay Parry Sound District Health Uni",
    "Humber River Regional Hospital":"City of Toronto Health Unit",
    "Woodstock General Hospital":"Southwestern Public Health Unit",
    }

    for df, save_file, date in transform(
        data_in = {'classification':'restricted', 'stage': 'processed','source_name':'ccso', 'table_name':'ccis',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_phu_icu_capacity',  'type': 'csv'}):
        try:
            df = df.loc[(df.icu_type != 'Neonatal') & (df.icu_type != 'Paediatric')]
            result = {"phu":[], "critical_care_beds":[], "confirmed_positive":[],"critical_care_patients":[],"critical_care_pct":[]}
            df = df.loc[(df.icu_type != 'Neonatal') & (df.icu_type != 'Paediatric')]
            df['province'] = 'Ontario'
            df['phu'] = df['hospital_name'].replace(replace)
            unique = df.phu.unique()
            for hr in unique:
                temp = df.loc[df.phu == hr]
                critical_care_beds = temp.loc[temp.unit_inclusion == 'Baseline'].critical_care_beds.sum()
                critical_care_patients = temp.critical_care_patients.sum()
                confirmed_positive = temp.confirmed_positive.sum()
                critical_care_pct = critical_care_patients / critical_care_beds
                result["phu"].append(hr)
                result["critical_care_beds"].append(critical_care_beds)
                result["confirmed_positive"].append(confirmed_positive)
                result["critical_care_patients"].append(critical_care_patients)
                result["critical_care_pct"].append(critical_care_pct)

            critical_care_beds = df.loc[df.unit_inclusion == 'Baseline'].critical_care_beds.sum()
            critical_care_patients = df.critical_care_patients.sum()
            confirmed_positive = df.confirmed_positive.sum()
            critical_care_pct = critical_care_patients / critical_care_beds
            result["phu"].append("Ontario")
            result["critical_care_beds"].append(critical_care_beds)
            result["critical_care_patients"].append(critical_care_patients)
            result["critical_care_pct"].append(critical_care_pct)
            result["confirmed_positive"].append(confirmed_positive)
            result = pd.DataFrame(result)
            result.to_csv(save_file, index=False)
        except Exception as e:
            print(e)
            print(f"Error in {save_file}")

@bp.cli.command('public_cases_ontario_phu_confirmed_positive_aggregated')
def transform_public_cases_ontario_phu_confirmed_positive_aggregated():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'canada_confirmed_positive_cases',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_phu_confirmed_positive_aggregated',  'type': 'csv'}):
        dfs = df.loc[df.province == "Ontario"]
        for column in ['date_report']:
            dfs[column] = pd.to_datetime(dfs[column])
        health_regions = dfs.health_region.unique()

        data_frame = {'date_report':[], 'health_regions':[], 'new_cases':[], 'cumulative_cases': []}
        min = dfs['date_report'].min()
        max = dfs['date_report'].max()
        idx = pd.date_range(min, max)

        for region in health_regions:
            df = dfs.loc[dfs.health_region == region]
            df = df.groupby("date_report").case_id.count()
            df = df.reindex(idx, fill_value=0).reset_index()
            date = datetime.strptime("2020-02-28","%Y-%m-%d")
            df = df.loc[df['index'] > date]
            df['date_str'] = df['index'].astype(str)
            df['cumulative_cases'] = df['case_id'].cumsum()

            data_frame['date_report'] += df['index'].tolist()
            data_frame['health_regions'] += [region]*len(df['index'].tolist())
            data_frame['new_cases'] += df['case_id'].tolist()
            data_frame['cumulative_cases'] += df['cumulative_cases'].tolist()

        df_final = pd.DataFrame(data_frame, columns=['date_report', 'health_regions', 'new_cases','cumulative_cases'])
        df_final.to_csv(save_file, index=False)

@bp.cli.command('public_cases_ontario_phu_weekly_new_cases')
def transform_public_cases_ontario_phu_weekly_new_cases():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_phu_weekly_new_cases',  'type': 'csv'}):

        ont_data = df.copy()
        for column in ['case_reported_date']:
            ont_data[column] = pd.to_datetime(ont_data[column])

        header = ["date","case_count"]
        rows = []

        delta = timedelta(days=1)
        start_date = datetime(2020,1,1)
        end_date = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')-timedelta(days=1)

        while start_date <end_date:
            rows.append([start_date,ont_data[ont_data["case_reported_date"]==start_date].shape[0]])
            start_date = start_date + delta

        # This is 100% correct
        daily_ont = pd.DataFrame(rows, columns=header)
        daily_ont = daily_ont.set_index('date').rolling(window=7).sum().reset_index()
        daily_ont['phu'] = 'Ontario'
        daily_ont = daily_ont.rename(columns={"date": "Date","case_count": "Cases", "phu": "PHU"})

        phus = set(ont_data["reporting_phu"].values)
        header = ["date", "phu", "case_count"]
        rows = {}

        delta = timedelta(days=1)

        end_date = datetime.strptime(datetime.today().strftime('%Y-%m-%d'), '%Y-%m-%d')-timedelta(days=1)

        for phu in phus:
            start_date = datetime(2020,1,1)
            phu_row = []
            while start_date <end_date:
                phu_row.append([start_date,phu,ont_data[(ont_data["case_reported_date"]==start_date)&(ont_data["reporting_phu"]==phu)].shape[0]])
                start_date = start_date + delta
            rows[phu] = phu_row

        phu_dfs = []
        for phu in rows:
            phu_rows = rows[phu]
            phu_rows = pd.DataFrame(phu_rows, columns=header)
            phu_rows = phu_rows.set_index('date').rolling(window=7).sum().reset_index()
            phu_rows["phu"] = phu
            phu_dfs.append(phu_rows)


        daily_phu = pd.concat(phu_dfs)
        daily_phu = daily_phu.rename(columns={"date": "Date","case_count": "Cases", "phu": "PHU"})

        df = pd.concat([daily_phu,daily_ont])
        df.to_csv(save_file, index=False)

@bp.cli.command('public_capacity_ontario_testing_24_hours')
def transform_public_capacity_ontario_testing_24_hours():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_testing_24_hours',  'type': 'csv'}):
        for column in ['case_reported_date','specimen_reported_date', 'test_reported_date']:
            df[column] = pd.to_datetime(df[column])
        df['turn_around'] = (df['test_reported_date'] - df['specimen_reported_date']).dt.days
        def less(thing):
            phu = thing.reporting_phu.unique()[0]
            date = thing.specimen_reported_date.unique()[0]
            less_1 = len(thing.loc[thing.turn_around <=1])
            total = len(thing)
            return less_1

        def total(thing):
            phu = thing.reporting_phu.unique()[0]
            date = thing.specimen_reported_date.unique()[0]
            less_1 = len(thing.loc[thing.turn_around <=1])
            total = len(thing)
            return total
        a = df.groupby(['reporting_phu','specimen_reported_date']).apply(less)
        b = df.groupby(['reporting_phu','specimen_reported_date']).apply(total)
        df = pd.merge(a.to_frame("less").reset_index(),b.to_frame("total").reset_index())
        dfs = []
        unique = df.reporting_phu.unique()
        for hr in unique:
            temp = df.loc[df.reporting_phu == hr]
            temp['PHU'] = hr
            temp['total_7_day'] = temp.total.rolling(7).sum()
            temp['less_7_day'] = temp.less.rolling(7).sum()
            temp['Percentage in 24 hours_7dayrolling'] = temp['less_7_day'] / temp['total_7_day']
            dfs.append(temp)

        temp = df.groupby(['specimen_reported_date']).sum().reset_index()
        temp['PHU'] = 'Ontario'
        temp['total_7_day'] = temp.total.rolling(7).sum()
        temp['less_7_day'] = temp.less.rolling(7).sum()
        temp['Percentage in 24 hours_7dayrolling'] = temp['less_7_day'] / temp['total_7_day']
        dfs.append(temp)
        result = pd.concat(dfs)
        result['Date'] = result['specimen_reported_date']
        result.to_csv(save_file, index=False)

@bp.cli.command('public_economic_ontario_job_postings')
def transform_public_economic_ontario_job_postings():
    for df, save_file, date in transform(
        data_in = {'classification':'confidential', 'stage': 'processed','source_name':'burning_glass', 'table_name':'industry_weekly',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'economic', 'table_name':'ontario_job_postings',  'type': 'csv'}):
        df = df.loc[df.geography == 'Ontario']
        df.to_csv(save_file, index=False)

@bp.cli.command('public_capacity_ontario_testing_analysis')
def transform_public_capacity_ontario_testing_analysis():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_testing_analysis',  'type': 'csv'}):
        latest=df.rename(columns={"accurate_episode_date": "Accurate_Episode_Date", "case_reported_date": "Case_Reported_Date", "test_reported_date":"Test_Reported_Date", "specimen_reported_date":"Specimen_Date"})
        for column in ['Case_Reported_Date','Specimen_Date', 'Test_Reported_Date', 'Accurate_Episode_Date']:
            latest[column] = pd.to_datetime(latest[column])
        percentiles = [50,80,90,95,99]
        metrics = ['Episode_to_Report', 'Episode_to_Specimen', 'Specimen_to_Result', 'Result_to_Report']
        combo_metrics = ['%s_%d' % (m, p) for m in metrics for p in percentiles]
        latest['Episode_to_Report'] = (latest['Case_Reported_Date'] - latest['Accurate_Episode_Date']).dt.days
        latest['Episode_to_Specimen'] = (latest['Specimen_Date'] - latest['Accurate_Episode_Date']).dt.days
        latest['Specimen_to_Result'] = (latest['Test_Reported_Date'] - latest['Specimen_Date']).dt.days
        latest['Result_to_Report'] = (latest['Case_Reported_Date'] - latest['Test_Reported_Date']).dt.days
        DATE_FIELDS = ['Accurate_Episode_Date', 'Case_Reported_Date', 'Test_Reported_Date', 'Specimen_Date']
        latest_date = latest[DATE_FIELDS].max().max()
        delay_df = pd.DataFrame(index=pd.date_range('2020-03-01', latest_date), columns=combo_metrics)
        for crd, grp in latest[latest['Accurate_Episode_Date']>=pd.to_datetime('2020-03-01')].groupby('Case_Reported_Date'):
            for m in metrics:
                for p in percentiles:
                    delay_df.loc[crd, '%s_%d' % (m, p)] = grp[m].quantile(p/100)
        delay_df.reset_index(inplace=True)
        delay_df.to_csv(save_file, index=False)

def impute_intervention_index(df_raw, prov, daterange):
    gov_res_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'E1', 'E2', 'H1', 'H2', 'H3']
    cont_hlth_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1', 'H2', 'H3']
    stringency_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1']
    economic_idx = ['E1', 'E2']
    intervention_categories_all = ['C1 School Closing',
                                   'C2 Workplace Closures',
                                   'C3 Cancel public events',
                                   'C4 Public Gathering Restrictions',
                                   'C5 Close public transport',
                                   'C6 Stay at home requirements',
                                   'C7 Restrictions on internal movements',
                                   'C8 International Travel Controls',
                                   'E1 Income Support',
                                   'E2 Debt / Contract Relief for Households',
                                   'E3 Fiscal measures',
                                   'E4 Support for Other Countries',
                                   'H1 Public Info Campaigns',
                                   'H2 Testing policy',
                                   'H3 Contact tracing',
                                   'H4 Emergency investment in health care',
                                   'H5 Investment in vaccines']


    prov_list = ['Federal', 'Alberta', 'British Columbia', 'Manitoba',
                 'New Brunswick', 'Newfoundland and Labrador', 'Northwest Territories', 'Nova Scotia',
                 'Ontario', 'Prince Edward Island', 'Quebec', 'Saskatchewan', 'Yukon']

    gov_res_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'E1', 'E2', 'H1', 'H2', 'H3']
    cont_hlth_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1', 'H2', 'H3']
    stringency_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1']
    economic_idx = ['E1', 'E2']

    closure_code = ['C1', 'C2', 'C3', 'C5', 'C6', 'C7']
    restriction_code = ['C4']
    travel_code = ['C8']
    income_code = ['E1']
    relief_code = ['E2']
    fiscal_code = ['E3', 'E4', 'H4', 'H5']
    info_code = ['H1']
    test_code = ['H2']
    trace_code = ['H3']

    geo_flag = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'H1']
    sect_flag = ['E1']

    # Setting columns for the empty dataframe
    other_cols_all = []
    ss = geo_flag + sect_flag
    for s in ss:
        other_cols_all.append(f'{s}_flag')

    code_dct = {'C4': 'oxford_restriction_code',
                'C8': 'oxford_travel_code',
                'E1': 'oxford_income_amount',
                'E2': 'oxford_debt_relief_code',
                'H1': 'oxford_public_info_code',
                'H2': 'oxford_testing_code',
                'H3': 'oxford_tracing_code'}
    for cc in closure_code:
        code_dct[cc] = 'oxford_closure_code'
    for fc in fiscal_code:
        code_dct[fc] = 'oxford_fiscal_measure_cad_mod'

    code_max = {'C1': 3,
                'C2': 3,
                'C3': 2,
                'C4': 4,
                'C5': 2,
                'C6': 3,
                'C7': 2,
                'C8': 4,
                'E1': 2,
                'E2': 2,
                'H1': 2,
                'H2': 3,
                'H3': 2}

    df_raw2 = df_raw[(df_raw['region'] == prov) & (df_raw['subregion'] == 'All')]

    # Make empty dataframe with all the neccesary columns and rows (dates)
    dff = pd.DataFrame( {
        "region" : prov,
        "date" : daterange,
    })

    df_return = pd.concat([dff,pd.DataFrame(columns=intervention_categories_all+other_cols_all)], sort=False)

    df_return["Government response index"] = 0
    df_return["Containment and health index"] = 0
    df_return["Stringency index"] = 0
    df_return["Economic support index"] = 0

    for idx, row in df_return.iterrows():
        obs_date = row['date']
#         subset = df_raw2[(df_raw2['start_date'] <= obs_date) & (df_raw2['start_date'] >= obs_date - np.timedelta64(28,'D'))]
        subset = df_raw2[(df_raw2['start_date'] <= obs_date)]
        gri = 0
        chi = 0
        si = 0
        esi = 0

        for iv in intervention_categories_all:
            iv_pref = iv.split(' ')[0]
            subset_iv = subset[subset['oxford_government_response_category'] == iv]

            if len(subset_iv[subset_iv['end_date'].isnull() == False]):
                help_subset = subset_iv[subset_iv['end_date'].isnull() == False]
                help_subset['end_date'] = pd.to_datetime(help_subset['end_date'], infer_datetime_format='%Y-%m-%d')
                end_date = help_subset.sort_values(by='end_date',ascending=False)['end_date'].iloc[0]
                if iv_pref not in fiscal_code:
                    if len(subset_iv[subset_iv['start_date'] >= end_date]):
                        subset_iv = subset_iv[subset_iv['start_date'] >= end_date]

            if len(subset_iv):
                if iv_pref in fiscal_code:
                    df_return.at[idx, iv] = np.nansum(subset_iv[code_dct[iv_pref]])
                else:
                    df_return.at[idx, iv] = np.nanmax(subset_iv[code_dct[iv_pref]])
                    if iv_pref in geo_flag:
                        df_return.at[idx, iv_pref + '_flag'] = np.nanmax(subset_iv['oxford_geographic_target_code'])
                    elif iv_pref in sect_flag:
                        df_return.at[idx, iv_pref + '_flag'] = np.nanmax(subset_iv['oxford_income_target'])
            else:
                if idx:
                    df_return.at[idx, iv] = df_return.iloc[idx-1][iv]
                    if iv_pref in geo_flag:
                        df_return.at[idx, iv_pref + '_flag'] = 0
                    elif iv_pref in sect_flag:
                        df_return.at[idx, iv_pref + '_flag'] = 0
                else:
                    df_return.at[idx, iv] = 0
                    if iv_pref in geo_flag:
                        df_return.at[idx, iv_pref + '_flag'] = 0
                    elif iv_pref in sect_flag:
                        df_return.at[idx, iv_pref + '_flag'] = 0

            if iv_pref in cont_hlth_idx:

                if iv_pref in geo_flag:
                    iv_val = 100 * (df_return.iloc[idx][iv]- 0.5*(1-df_return.iloc[idx][iv_pref + '_flag'])) / code_max[iv_pref]
                else:
                    iv_val = 100 * df_return.iloc[idx][iv] / code_max[iv_pref]

                iv_val = max(0, iv_val)
                gri += iv_val
                chi += iv_val
                if iv_pref in stringency_idx:
                    si += iv_val

            if iv_pref in economic_idx:
                if iv_pref in sect_flag:
                    iv_val = 100 * (df_return.iloc[idx][iv]- 0.5*(1-df_return.iloc[idx][iv_pref + '_flag'])) / code_max[iv_pref]
                else:
                    iv_val = 100 * df_return.iloc[idx][iv] / code_max[iv_pref]

                iv_val = max(0, iv_val)

                gri += iv_val
                esi += iv_val

        df_return.at[idx, "Government response index"] = gri / 13
        df_return.at[idx, "Containment and health index"] = chi / 11
        df_return.at[idx, "Stringency index"] = si / 9
        df_return.at[idx, "Economic support index"] = esi / 2

    return df_return

@bp.cli.command('public_interventions_canada_non_pharmaceutical_intervention_stringency')
def transform_public_interventions_canada_non_pharmaceutical_intervention_stringency():
    for df, save_file, date in transform(
        data_in = {'classification':'public', 'stage': 'transformed','source_name':'interventions', 'table_name':'canada_non_pharmaceutical_interventions',  'type': 'csv'},
        data_out = {'classification':'public', 'stage': 'transformed','source_name':'interventions', 'table_name':'canada_non_pharmaceutical_intervention_stringency',  'type': 'csv'}):
        start_tracking_date = np.datetime64('2020-01-07')
        latest_tracking_date = np.datetime64(datetime.today().strftime('%Y-%m-%d'))
        gov_res_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'E1', 'E2', 'H1', 'H2', 'H3']
        cont_hlth_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1', 'H2', 'H3']
        stringency_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1']
        economic_idx = ['E1', 'E2']
        intervention_categories_all = ['C1 School Closing',
                                       'C2 Workplace Closures',
                                       'C3 Cancel public events',
                                       'C4 Public Gathering Restrictions',
                                       'C5 Close public transport',
                                       'C6 Stay at home requirements',
                                       'C7 Restrictions on internal movements',
                                       'C8 International Travel Controls',
                                       'E1 Income Support',
                                       'E2 Debt / Contract Relief for Households',
                                       'E3 Fiscal measures',
                                       'E4 Support for Other Countries',
                                       'H1 Public Info Campaigns',
                                       'H2 Testing policy',
                                       'H3 Contact tracing',
                                       'H4 Emergency investment in health care',
                                       'H5 Investment in vaccines']
        prov_list = ['Federal', 'Alberta', 'British Columbia', 'Manitoba',
                     'New Brunswick', 'Newfoundland and Labrador', 'Northwest Territories', 'Nova Scotia',
                     'Ontario', 'Prince Edward Island', 'Quebec', 'Saskatchewan', 'Yukon']
        gov_res_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'E1', 'E2', 'H1', 'H2', 'H3']
        cont_hlth_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1', 'H2', 'H3']
        stringency_idx = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'C8', 'H1']
        economic_idx = ['E1', 'E2']
        closure_code = ['C1', 'C2', 'C3', 'C5', 'C6', 'C7']
        restriction_code = ['C4']
        travel_code = ['C8']
        income_code = ['E1']
        relief_code = ['E2']
        fiscal_code = ['E3', 'E4', 'H4', 'H5']
        info_code = ['H1']
        test_code = ['H2']
        trace_code = ['H3']
        geo_flag = ['C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'C7', 'H1']
        sect_flag = ['E1']
        # Setting columns for the empty dataframe
        other_cols_all = []
        ss = geo_flag + sect_flag
        for s in ss:
            other_cols_all.append(f'{s}_flag')
        # What are the unique places (country/region/subregion)?
        df["subregion"].fillna('All', inplace=True)
        df['start_date'] = pd.to_datetime(df['start_date'])
        df_places = df[['country', 'region', 'subregion']].drop_duplicates().sort_values(['region', 'subregion'])
        # Make list of unique places, condensing name with underscore separation
        unique_places = df_places[['country', 'region', 'subregion']].apply(lambda row: '_'.join(row.values.astype(str)), axis=1).values.tolist()
        daterange = pd.date_range(start=start_tracking_date,end=latest_tracking_date)
        code_dct = {'C4': 'oxford_restriction_code',
                    'C8': 'oxford_travel_code',
                    'E1': 'oxford_income_amount',
                    'E2': 'oxford_debt_relief_code',
                    'H1': 'oxford_public_info_code',
                    'H2': 'oxford_testing_code',
                    'H3': 'oxford_tracing_code'}
        for cc in closure_code:
            code_dct[cc] = 'oxford_closure_code'
        for fc in fiscal_code:
            code_dct[fc] = 'oxford_fiscal_measure_cad_mod'
        code_max = {'C1': 3,
                    'C2': 3,
                    'C3': 2,
                    'C4': 4,
                    'C5': 2,
                    'C6': 3,
                    'C7': 2,
                    'C8': 4,
                    'E1': 2,
                    'E2': 2,
                    'H1': 2,
                    'H2': 3,
                    'H3': 2}
        df['oxford_fiscal_measure_cad_mod'] = df['oxford_fiscal_measure_cad'].apply(parse_cad)
        # Generate temporal stringency index per province
        on = impute_intervention_index(df, 'Ontario', daterange)
        qb = impute_intervention_index(df, 'Quebec', daterange)
        bc = impute_intervention_index(df, 'British Columbia', daterange)
        sk = impute_intervention_index(df, 'Saskatchewan', daterange)
        nb = impute_intervention_index(df, 'New Brunswick', daterange)
        ns = impute_intervention_index(df, 'Nova Scotia', daterange)
        mb = impute_intervention_index(df, 'Manitoba', daterange)
        ab = impute_intervention_index(df, 'Alberta', daterange)
        nv = impute_intervention_index(df, 'Nunavut', daterange)
        pei = impute_intervention_index(df, 'Prince Edward Island', daterange)
        nwt = impute_intervention_index(df, 'Northwest Territories', daterange)
        nl = impute_intervention_index(df, 'Newfoundland and Labrador', daterange)
        yt = impute_intervention_index(df, 'Yukon', daterange)
        prov_dict = {'Ontario': on,
                    'Quebec': qb,
                    'British Columbia': bc,
                    'Saskatchewan': sk,
                    'New Brunswick': nb,
                    'Nova Scotia': ns,
                    'Manitoba' : mb,
                    'Alberta' : ab,
                    'Prince Edward Island': pei,
                    'Nunavut': nv,
                    'Northwest Territories' : nwt,
                    'Newfoundland and Labrador' : nl,
                    'Yukon': yt}
        canada = pd.DataFrame(columns=['date', 'region', 'Government response index',
               'Containment and health index', 'Stringency index',
               'Economic support index'])
        for k, v in prov_dict.items():
            canada = pd.concat([canada, v[['date', 'region', 'Government response index',
               'Containment and health index', 'Stringency index',
               'Economic support index']]])
        for index_type in ['Government response index',
               'Containment and health index', 'Stringency index',
               'Economic support index']:
            canada[index_type] = pd.to_numeric(canada[index_type])
        canada['region'] = canada['region'].apply(str)
        canada['date'] = pd.to_datetime(canada['date'])
        canada['date'] = canada['date'].dt.strftime('%m/%d')
        canada.to_csv(save_file, index=False)

@bp.cli.command('public_capacity_ontario_phu_icu_capacity_timeseries')
def transform_public_capacity_ontario_phu_icu_capacity_timeseries():
    data_in = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_phu_icu_capacity',  'type': 'csv'}
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_phu_icu_capacity_timeseries',  'type': 'csv'}
    load_dir, file_path = get_dir(data_in)
    files = glob.glob(load_dir + "/*." + data_in['type'])
    temps = []
    for file in files:
        temp = pd.read_csv(file)
        date = file.split('_')[-1]
        date = date.split('.csv')[0]
        temp['date'] = date
        temps.append(temp)

    result = pd.concat(temps)
    save_file, save_dir = get_file_path(data_out)
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    result.to_csv(save_file, index=False)

@bp.cli.command('public_cases_ontario_cases_seven_day_rolling_average')
def transform_public_cases_ontario_cases_seven_day_rolling_average():
    for df, save_file, date in transform(
        data_in={'classification':'public', 'stage': 'processed','source_name':'ontario_gov', 'table_name':'daily_change_in_cases_by_phu',  'type': 'csv'},
        data_out={'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_cases_seven_day_rolling_average',  'type': 'csv'}):

        dfs = []
        df['case_reported_date'] = pd.to_datetime(df['Date'])
        pop = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/hr_map.csv")
        pop = pop.loc[pop.province == "Ontario"]
        cases_df = pd.merge(df,pop, left_on=['HR_UID'], right_on=['HR_UID'], how='left')
        cases_df['phu'] = cases_df['health_region_esri']
        cases_df.loc[cases_df.HR_UID == 6, 'phu'] = 'Ontario'
        cases_df.loc[cases_df.phu == "Ontario", 'pop'] = 14745040
        unique = cases_df.HR_UID.unique()
        for hr in unique:
            temp = cases_df.loc[cases_df.HR_UID == hr]
            temp['rolling'] = temp.value.rolling(7).mean()
            temp['rolling_pop'] = temp['rolling'] / temp['pop'] * 100000
            dfs.append(temp)
        result = pd.concat(dfs)
        result = result[['HR_UID','phu','case_reported_date', 'pop','rolling', 'rolling_pop']]
        result.to_csv(save_file, index=False)

@bp.cli.command('public_summary_ontario')
def transform_public_summary_ontario():
    cases = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_cases_seven_day_rolling_average',  'type': 'csv'}
    tests = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_testing_24_hours',  'type': 'csv'}
    icu = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_phu_icu_capacity_timeseries',  'type': 'csv'}
    rt = {'classification':'public', 'stage': 'transformed','source_name':'rt', 'table_name':'canada_bettencourt_and_ribeiro_approach',  'type': 'csv'}
    final = {'classification':'public', 'stage': 'transformed','source_name':'summary', 'table_name':'ontario',  'type': 'csv'}

    cases_path = get_last_file(cases)
    cases_df = pd.read_csv(cases_path)
    cases_df['phu'] = cases_df['phu'].replace(Replace)
    cases_df = cases_df[['phu','HR_UID','case_reported_date', 'rolling','rolling_pop']]
    cases_df.rename(columns={"case_reported_date": "date"},inplace=True)
    cases_df['risk'] = cases_df.apply(process_cases, axis=1)
    cases_df['count'] = None
    cases_df['prev'] = None
    unique = cases_df.HR_UID.unique()
    hrs = []
    for hr in unique:
        temp = cases_df.loc[cases_df.HR_UID == hr]
        prev = None
        count = 1
        for index, row in temp.iterrows():
            temp.at[index,'prev'] = prev
            risk = row['risk']
            if risk != prev:
                prev = risk
                count = 1
            else:
                count += 1
            temp.at[index,'count'] = count
        hrs.append(temp)
    cases_df = pd.concat(hrs)



    tests_path = get_last_file(tests)
    tests_df = pd.read_csv(tests_path)
    tests_df = tests_df[["PHU", "specimen_reported_date","Percentage in 24 hours_7dayrolling"]]
    tests_df['PHU'] = tests_df['PHU'].replace(Replace)
    tests_df.rename(columns={"Percentage in 24 hours_7dayrolling": "rolling_test_twenty_four"},inplace=True)

    merged = pd.merge(cases_df,tests_df, left_on=['phu', 'date'], right_on=['PHU', 'specimen_reported_date'], how='left')

    icu_path = get_last_file(icu)
    icu_df = pd.read_csv(icu_path)
    icu_df['phu'] = icu_df['phu'].replace(Replace)
    icu_df = icu_df[["phu", "date","critical_care_beds","critical_care_patients","confirmed_positive","critical_care_pct"]]

    merged = pd.merge(merged,icu_df, left_on=['phu', 'date'], right_on=['phu', 'date'], how='left')

    rt_path = get_last_file(rt)
    rt_df = pd.read_csv(rt_path)
    rt_df = rt_df[["PHU", "date_report","ML","Low","High"]]
    rt_df['PHU'] = rt_df['PHU'].replace(Replace)
    rt_df.rename(columns={"ML": "rt_ml", "Low": "rt_low", "High": "rt_high"},inplace=True)

    merged = pd.merge(merged,rt_df, left_on=['phu', 'date'], right_on=['PHU', 'date_report'], how='left')
    merged = merged[["phu", 'HR_UID','date', 'rolling','rolling_pop', 'rolling_test_twenty_four', "critical_care_beds","critical_care_patients","confirmed_positive","critical_care_pct","rt_ml","rt_low","rt_high", 'prev','risk', 'count']]
    save_file, save_dir = get_file_path(final)
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    merged.to_csv(save_file, index=False)
