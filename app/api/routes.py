from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
import plotly.graph_objects as go
from datetime import datetime, timedelta
import requests
from app import db, cache
from app.models import *
from app.api import bp
from app.api import vis
import pandas as pd
import numpy as np
import io
import requests
import glob, os
import json
from app.export import restrictedsheetsHelper
from app.export import sheetsHelper
import sendgrid
import os
from sendgrid.helpers.mail import *
import click
import jwt
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from firebase_admin import credentials, initialize_app, storage
import tweepy
import platform

PHU = {'The District of Algoma Health Unit':'Algoma Public Health Unit',
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

def get_results():
    items = request.get_json()
    c = Covid.query.filter_by(province="Ontario")
    df = pd.read_sql(c.statement, db.engine)
    case_count = df.groupby("date").case_id.count().cumsum().reset_index()
    case_count = case_count.loc[case_count.case_id > 100]
    df = df.groupby("date").case_id.count().reset_index()
    df['case_id'] = df['case_id']*0.05
    df['case_id'] = df['case_id'].rolling(min_periods=1, window=8).sum()
    df = df.loc[df.date.isin(case_count.date.values)].reset_index()
    provines_dict = {}
    province_dict = df['case_id'].to_dict()
    provines_dict["Ontario"] = province_dict
    provinces = ["Italy", "South Korea", "Singapore"]
    for province in provinces:
        c = Comparison.query.filter_by(province=province)
        df = pd.read_sql(c.statement, db.engine)
        case_count = df['count'].cumsum()
        df['case_count'] = case_count
        df = df.loc[df['case_count'] > 100].reset_index()
        df['count'] = df['count']*0.05
        df['count'] = df['count'].rolling(min_periods=1, window=8).sum()
        df = df.reset_index()
        province_dict = df['count'].to_dict()
        provines_dict[province] = province_dict
    return jsonify(provines_dict)


def get_date():
    c = Covid.query.filter_by(province="Ontario")
    df = pd.read_sql(c.statement, db.engine)
    case_count = df.groupby("date").case_id.count().cumsum().reset_index()
    case_count = case_count.loc[case_count.case_id > 100]
    df = df.groupby("date").case_id.count().reset_index()
    df['case_id'] = df['case_id']*0.05
    df['case_id'] = df['case_id'].rolling(min_periods=1, window=8).sum()
    df = df.loc[df.date.isin(case_count.date.values)].reset_index()
    provines_dict = {}
    province_dict = df['date'].to_dict()
    provines_dict["Ontario"] = province_dict
    return jsonify(provines_dict)


@as_json
def get_phus():
    c = Covid.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
    "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron Perth Public Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
      "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
      "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
      "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
      "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
      "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit"}
    dfs.region = dfs.region.replace(replace)
    regions = dfs.region.unique()
    provines_dict = {}
    for region in regions:
        df = dfs.loc[dfs.region == region]
        df = df.groupby("date").case_id.count().cumsum().reset_index()
        date = datetime.strptime("2020-02-28","%Y-%m-%d")
        df = df.loc[df.date > date]
        df['date_str'] = df['date'].astype(str)
        province_dict = df.set_index('date_str')['case_id'].to_dict()
        provines_dict[region] = province_dict

    df = pd.read_sql_table('covidtests', db.engine)
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df['date_str'] = df['date'].astype(str)
    province_dict = df.set_index('date_str')['positive'].to_dict()
    provines_dict["Ontario"] = province_dict
    return provines_dict


@as_json
def get_phunew():
    c = Covid.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    regions = dfs.region.unique()
    provines_dict = {}
    for region in regions:
        df = dfs.loc[dfs.region == region]
        df = df.groupby("date").case_id.count().reset_index()
        date = datetime.strptime("2020-02-28","%Y-%m-%d")
        df = df.loc[df.date > date]
        df['date_str'] = df['date'].astype(str)
        province_dict = df.set_index('date_str')['case_id'].to_dict()
        provines_dict[region] = province_dict

    df = dfs.groupby("date").case_id.count().reset_index()
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df['date_str'] = df['date'].astype(str)
    province_dict = df.set_index('date_str')['case_id'].to_dict()
    provines_dict["Ontario"] = province_dict
    return provines_dict


@as_json
def get_growth():
    dfs = pd.read_sql_table('covid', db.engine)
    regions = dfs.province.unique()
    provines_dict = {}
    for region in regions:
        df = dfs.loc[dfs.province == region]
        df = df.groupby("date").case_id.count().cumsum().reset_index()
        df = df.loc[df.case_id > 100].reset_index()
        province_dict = df['case_id'].to_dict()
        provines_dict[region] = province_dict

    df = dfs.groupby("date").case_id.count().cumsum().reset_index()
    df = df.loc[df.case_id > 100].reset_index()
    province_dict = df['case_id'].to_dict()
    provines_dict["Canada"] = province_dict

    dfs = pd.read_sql_table('internationaldata', db.engine)
    regions = dfs.country.unique()
    for region in regions:
        df = dfs.loc[dfs.country == region]
        df = df['cases'].cumsum().reset_index()
        df = df.loc[df['cases'] > 100].reset_index()
        province_dict = df['cases'].to_dict()
        provines_dict[region] = province_dict



    return provines_dict

@bp.route('/api/viz', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_viz():
    df = pd.read_sql_table('viz', db.engine)
    df = df.loc[df.page == 'Analysis']
    df = df.loc[df.visible == True]
    df = df.sort_values(by=['category', 'header'])
    data = []
    for index, row in df.iterrows():
        data.append({"header": row["header"], "category": row["category"],
        "content": row["content"], "text_top": row["text_top"], "text_bottom": row["text_bottom"],
        "viz": row["viz"], "thumbnail": row["thumbnail"],
        "mobileHeight": row["mobileHeight"],"desktopHeight": row["desktopHeight"],
        "viz_type": row["viz_type"], "date": row["date"]})
    return data

@bp.route('/api/plots', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_plots():
    df = pd.read_sql_table('viz', db.engine)
    df = df.loc[df.page != 'Analysis']
    df = df.loc[df.html.notna()]
    df = df.loc[df.order > 0]
    df = df.loc[df.visible == True]
    df = df.sort_values(by=['order'])
    data = []
    for index, row in df.iterrows():
        data.append({"header": row["header"], "order": row["order"],
        "tab": row["content"],"tab_order": row["tab_order"],
        "row": 'span '+ str(row["row"]), "column": 'span '+ str(row["column"]),
        "html": row["html"],"category": row["page"], "group": row["category"],
        "phu": row["phu"], "viz_title": row["viz_title"],"viz": row["viz"],
        "text_top": row["text_top"], "text_bottom": row["text_bottom"]})
    return data

@bp.route('/api/source', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_source():
    df = pd.read_sql_table('source', db.engine)
    df = df.sort_values(by=['name'])
    data = []
    for index, row in df.iterrows():
        data.append({"region": row["region"],"type": row["type"],"name": row["name"], "source": row["source"],
        "description": row["description"], "data_feed_type": row["data_feed_type"],
        "link": row["link"], "refresh": row["refresh"],
        "contributor": row["contributor"],"contact": row["contact"],
        "download": row["download"], "html": row["html"]})
    return data

@bp.route('/api/team', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_api_team():
    df = pd.read_sql_table('members', db.engine)
    df = df.sort_values(by=['team_status','last_name'])
    data = []
    for index, row in df.iterrows():
        data.append({"team": row["team"],"title": row["title"],
        "first_name": row["first_name"], "last_name": row["last_name"],
        "education": row["education"], "affiliation": row["affiliation"],
        "role": row["role"], "team_status": row["team_status"],
        "linkedin": row["linkedin"]})
    return data


@bp.route('/api/reopening', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_reopening_metrics():
    stages_df = pd.read_csv("https://docs.google.com/spreadsheets/u/0/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=0")
    weekly_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1053507889")
    testing_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1206518301")
    rt_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=428679599")
    icu_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1132863788")
    positivity_df = pd.read_csv("https://docs.google.com/spreadsheets/u/0/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=1281856108")
    positivity_df = positivity_df.merge(stages_df, left_on="HR_UID", right_on="HR_UID")

    data = []
    for phu_select in PHU:
        temp_dict = {}
        temp_dict["phu"] = phu_select
        temp_dict["tracing"] = "nan"

        temp = positivity_df.loc[positivity_df.phu == phu_select]
        try:
            temp_dict["percent_positive"] = str((temp.tail(1)['% Positivity'].values[0]))
        except:
            temp_dict["percent_positive"] = "nan"

        temp = stages_df.loc[stages_df.phu == phu_select]
        try:
            temp_dict["stage"] = str((temp.tail(1)['stage'].values[0]))
        except:
            temp_dict["stage"] = "nan"


        temp = weekly_df.loc[weekly_df.PHU == PHU[phu_select]]
        temp = temp.sort_values('Date')
        try:
            temp_dict["weekly"] = str(int(temp.tail(1)['Cases'].values[0]))
        except:
            temp_dict["weekly"] = "nan"

        temp = testing_df.loc[testing_df.PHU == PHU[phu_select]]
        temp = temp.sort_values('Date')
        temp = temp.dropna(how='all')
        try:
            temp_dict["testing"] = str(int(temp.tail(1)['Percentage in 24 hours_7dayrolling'].values[0] * 100))
        except:
            temp_dict["testing"] = "nan"

        temp = rt_df.loc[rt_df.health_region == phu_select]
        temp = temp.sort_values('date_report')
        try:
            temp_dict["rt"] = str(temp.tail(1)['ML'].values[0])
        except:
            temp_dict["rt"] = "nan"

        temp = icu_df.loc[icu_df.phu == phu_select]
        try:
            temp_dict["icu"] = str(int(temp.tail(1)['critical_care_pct'].values[0] * 100))
        except:
            temp_dict["icu"] = "nan"




        data.append(temp_dict)

    return data

def get_last(thing):
    if len(thing.dropna()) > 0:
        return thing.dropna().iloc[-1]
    else:
        return np.nan

@cache.memoize(timeout=600)
def get_summary(HR_UID):
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615"
    positivity = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=2051486909"
    df = pd.read_csv(url)
    positive = pd.read_csv(positivity)
    df['date'] = pd.to_datetime(df['date'])
    positive['Date'] = pd.to_datetime(positive['Date'])
    df = df.merge(positive, left_on=['date', 'HR_UID'], right_on=['Date', 'HR_UID'], how='left')
    if int(HR_UID)==0:
        df = df.loc[df.phu == 'Ontario']
    elif int(HR_UID)>0:
        df = df.loc[df.HR_UID == int(HR_UID)]
    else:
        loop = {"phu":[], "HR_UID":[], "date":[], "rolling":[], "rolling_pop":[], "rolling_pop_trend":[],"rolling_test_twenty_four":[], "rolling_test_twenty_four_trend":[],"confirmed_positive":[], "critical_care_beds":[],"critical_care_patients":[],"critical_care_pct":[], "critical_care_pct_trend":[],"covid_pct":[],"rt_ml":[], "rt_ml_trend":[],"percent_positive": [],"percent_positive_trend": [], "prev": [], "risk": [], "count": [], "percent_vaccinated": []}
        unique = df.HR_UID.unique()
        for hr in unique:
            temp = df.loc[df.HR_UID == hr]
            temp['rolling_pop_trend'] = temp['rolling_pop'].diff(periods=7)
            temp['rt_ml_trend'] = temp['rt_ml'].diff(periods=7)
            temp['rolling_test_twenty_four_trend'] = temp['rolling_test_twenty_four'].diff(periods=7)
            temp['percent_positive_trend'] = temp['% Positivity'].diff(periods=7)
            temp['critical_care_pct_trend']  = temp['critical_care_pct'].diff(periods=7)
            temp['risk'] = temp['risk'].fillna('NaN')
            temp['prev'] = temp['prev'].fillna('NaN')
            if len(temp) > 0:
                loop['phu'].append(get_last(temp['phu']))
                loop['HR_UID'].append(hr)
                loop['date'].append(get_last(temp['date']))
                loop['rolling'].append(get_last(temp['rolling']))
                loop['rolling_pop'].append(get_last(temp['rolling_pop']))
                loop['rolling_pop_trend'].append(get_last(temp['rolling_pop_trend']))
                loop['rolling_test_twenty_four'].append(get_last(temp['rolling_test_twenty_four']))
                loop['rolling_test_twenty_four_trend'].append(get_last(temp['rolling_test_twenty_four_trend']))
                loop['confirmed_positive'].append(get_last(temp['confirmed_positive']))
                loop['critical_care_pct'].append(get_last(temp['critical_care_pct']))
                loop['covid_pct'].append(get_last(temp['covid_pct']))
                loop['critical_care_pct_trend'].append(get_last(temp['critical_care_pct_trend']))
                loop['rt_ml'].append(get_last(temp['rt_ml']))
                loop['rt_ml_trend'].append(get_last(temp['rt_ml_trend']))
                loop['percent_positive'].append(get_last(temp['% Positivity']))
                loop['percent_positive_trend'].append(get_last(temp['percent_positive_trend']))
                loop['critical_care_beds'].append(get_last(temp['critical_care_beds']))
                loop['critical_care_patients'].append(get_last(temp['critical_care_patients']))
                loop['prev'].append(get_last(temp['prev']))
                loop['risk'].append(get_last(temp['risk']))
                loop['count'].append(get_last(temp['count']))
                loop['percent_vaccinated'].append(get_last(temp['percent_vaccinated']))
        temp = df.loc[df.phu == 'Ontario']
        temp['rolling_pop_trend'] = temp['rolling_pop'].diff(periods=7)
        temp['rt_ml_trend'] = temp['rt_ml'].diff(periods=7)
        temp['rolling_test_twenty_four_trend'] = temp['rolling_test_twenty_four'].diff(periods=7)
        temp['percent_positive_trend'] = temp['% Positivity'].diff(periods=7)
        temp['critical_care_pct_trend']  = temp['critical_care_pct'].diff(periods=7)
        temp['risk'] = temp['risk'].fillna('NaN')
        temp['prev'] = temp['prev'].fillna('NaN')
        loop['phu'].append('Ontario')
        loop['HR_UID'].append(-1)
        loop['date'].append(get_last(temp['date']))
        loop['rolling'].append(get_last(temp['rolling']))
        loop['rolling_pop'].append(get_last(temp['rolling_pop']))
        loop['rolling_pop_trend'].append(get_last(temp['rolling_pop_trend']))
        loop['rolling_test_twenty_four'].append(get_last(temp['rolling_test_twenty_four']))
        loop['rolling_test_twenty_four_trend'].append(get_last(temp['rolling_test_twenty_four_trend']))
        loop['confirmed_positive'].append(get_last(temp['confirmed_positive']))
        loop['critical_care_pct'].append(get_last(temp['critical_care_pct']))
        loop['covid_pct'].append(get_last(temp['covid_pct']))
        loop['critical_care_pct_trend'].append(get_last(temp['critical_care_pct_trend']))
        loop['rt_ml'].append(get_last(temp['rt_ml']))
        loop['rt_ml_trend'].append(get_last(temp['rt_ml_trend']))
        loop['percent_positive'].append(get_last(temp['% Positivity']))
        loop['percent_positive_trend'].append(get_last(temp['percent_positive_trend']))
        loop['critical_care_beds'].append(get_last(temp['critical_care_beds']))
        loop['critical_care_patients'].append(get_last(temp['critical_care_patients']))
        loop['prev'].append(get_last(temp['prev']))
        loop['risk'].append(get_last(temp['risk']))
        loop['count'].append(get_last(temp['count']))
        loop['percent_vaccinated'].append(get_last(temp['percent_vaccinated']))
        df = pd.DataFrame(loop)
    return df


@bp.route('/api/howsmyflattening', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_my_flattening():
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615")
    HR_UID = 6
    toronto = df.loc[df.HR_UID == HR_UID]
    toronto['date'] = pd.to_datetime(toronto['date'])
    toronto['empty'] = 1 - toronto['critical_care_pct']
    toronto['non_covid'] = toronto['critical_care_pct'] - toronto['covid_pct']
    icu = toronto.dropna(how='any', subset=['critical_care_pct'])
    icu['week'] = icu['date'].dt.week
    icu['label'] = icu['date'].apply(lambda x: str(x.week) + '-' + str(x.month_name())[:3] + '-' + str(x.year)[-2:])
    icu['day-label'] = icu['date'].apply(lambda x: str(x.day) + '-' + str(x.month_name())[:3] + '-' + str(x.year)[-2:])
    icu = icu.drop_duplicates(subset=['label'],keep='last')
    icu_copy = icu.copy()
    toronto_copy = toronto.copy()
    icu = icu_copy.copy()
    icu = icu.tail(26)
    toronto = toronto_copy.copy()
    toronto = toronto.loc[toronto.date >= icu.date.min()]
    min_date = icu.date.min().month_name()
    date_max = df.date.max()
    response = {"df": toronto.to_dict(orient='records'), "icu": icu.to_dict(orient='records'), "min_date": min_date}
    return response


@bp.cli.command('get_images')
def get_images():
    df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615")
    credintionals = {
    "type": "service_account",
    "project_id": "covid-data-analytics-hub",
    "private_key_id": os.getenv('email_private_key_id'),
    "private_key": os.getenv('email_private_key').replace('\\n', '\n'),
    "client_email": "firebase-adminsdk-irx4s@covid-data-analytics-hub.iam.gserviceaccount.com",
    "client_id": "108720830737287977754",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/firebase-adminsdk-irx4s%40covid-data-analytics-hub.iam.gserviceaccount.com"
    }
    cred = credentials.Certificate(credintionals)
    initialize_app(cred, {'storageBucket': 'covid-data-analytics-hub.appspot.com'})
    bucket = storage.bucket()
    for HR_UID in df.HR_UID.unique():
        toronto = df.loc[df.HR_UID == HR_UID]
        toronto['date'] = pd.to_datetime(toronto['date'])
        min_date = toronto.date.max() - pd.Timedelta(7*26, unit="d")
        toronto['empty'] = 1 - toronto['critical_care_pct']
        toronto['non_covid'] = toronto['critical_care_pct'] - toronto['covid_pct']
        icu = toronto.dropna(how='any', subset=['critical_care_pct'])
        icu['week'] = icu['date'].dt.week
        icu['label'] = icu['date'].apply(lambda x: str(x.week) + '-' + str(x.month_name())[:3] + '-' + str(x.year)[-2:])
        icu['day-label'] = icu['date'].apply(lambda x: str(x.day) + '-' + str(x.month_name())[:3] + '-' + str(x.year)[-2:])
        icu = icu.drop_duplicates(subset=['label'],keep='last')
        icu_copy = icu.copy()
        toronto_copy = toronto.copy()

        icu = icu_copy.copy()

        icu = icu.tail(26)

        toronto = toronto_copy.copy()

        toronto = toronto.loc[toronto.date >= min_date]

        min_date = toronto.date.min().month_name()
        date_max = df.date.max()

        fig = make_subplots(rows=2, cols=1, shared_xaxes=False)

        fig.add_trace(go.Scatter(x=toronto['date'], y=toronto['rolling_pop'],
                            mode='lines',
                            name='Case Incidence', legendgroup="group1",
                                line=dict(color='#1AA8D0', width=2)), row=1, col=1)


        fig.add_trace(go.Bar(name='Covid Patients', x=icu['day-label'], y=icu['covid_pct'],marker_color='#D94718', marker_line_color='#D94718',legendgroup="group"),row=2, col=1)
        fig.add_trace(go.Bar(name='Other Patients', x=icu['day-label'], y=icu['non_covid'],marker_color='#F2BB2B', marker_line_color='#F2BB2B',legendgroup="group"),row=2, col=1)
        fig.add_trace(go.Bar(name='Available Room', x=icu['day-label'], y=icu['empty'],marker_color='#1AA8D0', marker_line_color='#1AA8D0',legendgroup="group"),row=2, col=1)

        # Change the bar mode
        fig.update_layout(
            barmode='stack',
            title=f" Daily Cases + ICU Capacity Update (Since {min_date})",
            xaxis_title="Date",
            xaxis2_title="Week Ending",
            yaxis_title="Daily Cases Per 100k",
            yaxis2_title="% of ICU beds",
            yaxis2 = dict(
            tickformat= ',.0%',
            range= [0,1]
            ),
            plot_bgcolor='rgba(0,0,0,0)',
            legend=dict(
            yanchor="top",
            y=0.6,
            xanchor="right",
            x=1.3
            )
        )
        # print(os.getcwd())
        file = f"{HR_UID}_{date_max}.jpeg"
        path = f"{os.getcwd()}/app/static/email/{file}"
        fig.write_image(path, width=1200,height=675)
        blob = bucket.blob(file)
        blob.upload_from_filename(path)
        blob.make_public()
        print("your file url", blob.public_url)

@bp.route('/api/summary', methods=['GET'])
@cache.cached(timeout=600, query_string=True)
def get_summary_metrics():
    HR_UID = request.args.get('HR_UID')
    if not HR_UID:
        HR_UID = -1
    df = get_summary(HR_UID)
    data = df.to_json(orient='records', date_format='iso')
    return data

@bp.route('/api/mail', methods=['POST'])
def subscribe():
    content = request.json
    email = content['email'].lower()
    frequency = content['frequency'].lower()
    regions = content['regions']
    date_subscribed = datetime.now()
    past = Subscribers.query.filter_by(email=email).all()
    try:
        sign_up(email,regions,frequency)
    except:
        "sign up failed"
    if past:
        for item in past:
            if item.date_subscribed:
                date_subscribed = item.date_subscribed
            db.session.delete(item)
    for region in regions:
        s = Subscribers(email=email,frequency=frequency,region=region, date_subscribed=date_subscribed)
        db.session.add(s)
    db.session.commit()

    return 'success', 200

def sign_up(email,regions,frequency):
    df = get_summary(-1)
    df = df.round(2)
    temp_df = df.loc[df.HR_UID.isin(regions)]
    regions = temp_df.to_dict(orient='records')
    key = os.environ.get('EMAIL_API')
    sg = sendgrid.SendGridAPIClient(api_key=key)
    from_email = "mycovidreport@howsmyflattening.ca"
    to_email = email
    subject = "Your Personalized COVID-19 Report"
    token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
    html = render_template("welcome_email.html",regions=regions,frequency=frequency, token=token)
    text = render_template("welcome_email.txt",regions=regions,frequency=frequency, token=token)
    message = Mail(
    from_email=from_email,
    to_emails=to_email,
    subject='Your personalized COVID-19 report',
    plain_text_content=text,
    html_content=html)
    try:
        response = sg.send(message)
    except Exception as e:
        print(e.message)

def create_text_response(text):
    response = {
    "fulfillmentText": text,
    "fulfillmentMessages": [
      {
        "text": {
          "text": [
            text
          ]
        }
      }
    ],
    "source": "Howsmyflattening.com"
  }
    return response

def lookup_postal(postal_code):
    postal_code = postal_code.upper().replace(" ", "")
    pccf = pd.read_csv('pccf_on_postal.csv')
    result = pccf.loc[pccf.postal_code == postal_code]
    HR_UID = result['HR_UID'].values[0]
    PHU = result['ENGNAME'].values[0]
    return HR_UID, PHU

def get_fsa(postal_code):
    postal_code = postal_code.upper().replace(" ", "")
    fsa = postal_code[:3]
    df = pd.read_sql_table('iphis', db.engine)
    temp = df.loc[df.fsa == fsa]
    cases = int(temp.tail(1)['cases_two_weeks'].values[0])
    deaths = int(temp.tail(1)['deaths_two_weeks'].values[0])
    date = temp.tail(1)['index'].values[0]
    return date, cases, deaths, fsa

@cache.memoize(timeout=600)
def get_ontario():
    ontario_cases = pd.read_csv("https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv")
    pop = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/hr_map.csv")
    pop = pop.loc[pop.province == "Ontario"]
    pop['health_region'] = pop['health_region'].replace(POP)
    ontario_cases = pd.merge(ontario_cases,pop, left_on=['Reporting_PHU'], right_on=['health_region'], how='left')
    ontario_cases["Case_Reported_Date"] = pd.to_datetime(ontario_cases["Case_Reported_Date"])
    return ontario_cases

@bp.route('/api/chatbot', methods=['POST'])
def chatbot_webhook():
    data = request.get_json()
    try:
        intent = data['queryResult']['intent']['displayName']
        session = data['session']
    except:
        'request not in proper format', 400
    if intent == "Default Welcome Intent":
        ontario = vis.get_testresults()
        ontario['Date'] = pd.to_datetime(ontario['Date'])
        ontario['Date'] = ontario['Date'].dt.strftime('%B %d')
        date = ontario.tail(1)['Date'].values[0]
        cases = int(ontario.tail(1)['New positives'].values[0])
        deaths = int(ontario.tail(1)['New deaths'].values[0])
        hospital = int(ontario.tail(1)['Hospitalized'].values[0])
        icu = int(ontario.tail(1)['ICU'].values[0])
        response = create_text_response(f"Hi! I'm Covee, the COVID-19 bot built by HowsMyFlattening.ca . Most recently on {date}, Ontario reported {cases} new cases of COVID-19 and {deaths} new deaths. There are currently {hospital} COVID-19 cases in hospital and {icu} in the ICU. If you'd like to know more about cases in your region, simply ask me how many cases are there in my area!")
    elif intent == 'How many cases are there in my postal code?':
        postal_code = data['queryResult']['parameters']['PostalCode']
        date, cases, deaths, fsa = get_fsa(postal_code)
        response = create_text_response(f"As of {date}, there are {cases} new cases and {deaths} new deaths in {fsa} in the last 2 week.")
    elif intent == 'How many cases are there in my area?':
        PHU = data['queryResult']['parameters']['PHU']
        print(PHU)
        HR_UID = pd.read_csv('phu.csv')
        HR_UID = HR_UID.loc[HR_UID.phu == PHU]['HR_UID'].values[0]
        df = get_summary(-1)
        df = df.loc[df.HR_UID == HR_UID]
        df['date'] = df['date'].dt.strftime('%B %d')
        date = df.tail(1)['date'].values[0]
        cases = round(df.tail(1)['rolling_pop'].values[0],2)
        response = create_text_response(f"As of {date}, there have been on average {cases} daily cases per 100,000 people in {PHU}. Would you like to know more?")
    elif intent == 'Are cases in my area increasing or decreasing?':
        PHU = data['queryResult']['parameters']['PHU']
        HR_UID = pd.read_csv('phu.csv')
        HR_UID = HR_UID.loc[HR_UID.phu == PHU]['HR_UID'].values[0]
        df = get_summary(-1)
        df = df.loc[df.HR_UID == HR_UID]
        rt = df.tail(1)['rt_ml'].values[0]
        response = create_text_response(f"The estimated Rt for {PHU} is {rt}. Rt is a measure of how contagious a disease is at a certain point in time. It tells us the average number of people one sick person can infect (e.g. an Rt of 5 means that on average, each case is expected to infect 5 other people). If Rt is greater than 1, it means the outbreak is growing.")
    elif intent == 'When should I expect to get my test results back?':
        PHU = data['queryResult']['parameters']['PHU']
        HR_UID = pd.read_csv('phu.csv')
        HR_UID = HR_UID.loc[HR_UID.phu == PHU]['HR_UID'].values[0]
        df = get_summary(-1)
        df = df.loc[df.HR_UID == HR_UID]
        test = df.tail(1)['rolling_test_twenty_four'].values[0]
        response = create_text_response(f"In {PHU}, {test}% of tests are returned in 24 hours or less")
    elif intent == 'How many cases are there in my area? - yes':
        PHU = data['queryResult']['outputContexts'][0]['parameters']['PHU']
        HR_UID = pd.read_csv('phu.csv')
        HR_UID = HR_UID.loc[HR_UID.phu == PHU]['HR_UID'].values[0]
        ontario_cases = get_ontario()
        phu = ontario_cases.loc[ontario_cases.HR_UID == HR_UID]
        month = ontario_cases["Case_Reported_Date"].max() - pd.Timedelta(30, unit='d')
        temp = phu.loc[phu["Case_Reported_Date"] > month]
        cases = len(temp)
        temp['Age_Group'] = temp['Age_Group'].replace({'20s': '20 Year Olds', '30s': '30 Year Olds', '40s': '40 Year Olds', '50s': '50 Year Olds', '60s': '60 Year Olds', '70s': '70 Year Olds', '80s': '80 Year Olds','90s': '90 Year Olds', '<20': 'Kids younger than 20 years old'})
        age = temp['Age_Group'].value_counts(True)
        age_index = temp['Age_Group'].value_counts(True).index
        age_group_1 = age_index[0]
        age_group_1_pct = round(age[0], 2)
        age_group_2 = age_index[1]
        age_group_2_pct = round(age[1], 2)
        temp['Case_AcquisitionInfo'] =  temp['Case_AcquisitionInfo'].replace({'CC': 'Close Contact', 'No Epi-link': 'Community Spread', 'OB': 'Outbreak'})
        acquisition = temp['Case_AcquisitionInfo'].value_counts(True)
        acquisition_index = temp['Case_AcquisitionInfo'].value_counts(True).index
        acquisition_source = acquisition_index[0]
        acquisition_source_pct = round(acquisition[0],2)
        response = create_text_response(f"In the last month, there have been {cases} cases in {PHU}. The majority of the cases in {PHU} were in {age_group_1} which accounted for {age_group_1_pct*100}% of cases and {age_group_2} which accounted for {age_group_2_pct*100}% of cases. Most of these cases are acquired via {acquisition_source} ({acquisition_source_pct*100}%). ")
    elif intent == 'What should I do given the current data?':
        PHU = data['queryResult']['parameters']['PHU']
        HR_UID = pd.read_csv('phu.csv')
        HR_UID = HR_UID.loc[HR_UID.phu == PHU]['HR_UID'].values[0]
        df = get_summary(-1)
        df = df.loc[df.HR_UID == HR_UID]
        risk = df.tail(1)['risk'].values[0]
        response = create_text_response(f"{PHU} is in {risk} risk. The following precautions are recommended by the Govt of Canada: [bullet list of recommendations from first page]")
    else:
        response = create_text_response("I'm confused")
    return jsonify(response)

@bp.route('/api/mail/<token>', methods=['GET'])
def unsubscribe(token):
    try:
        email = jwt.decode(token, os.getenv('SECRET_KEY'),algorithms=['HS256'])['email']
        past = Subscribers.query.filter_by(email=email).all()
        if past:
            for item in past:
                db.session.delete(item)
            db.session.commit()
            return 'You have been unsubscribed', 200
        else:
            return 'We did not find any subscricptiosn for this email address', 200
    except:
        return 'Invalid Link', 400

@cache.memoize(timeout=600)
def get_alerts():
    df = pd.read_sql_table('alerts', db.engine)
    df = df.loc[df.active == True]
    return df

@bp.cli.command("email")
@click.argument("frequency")
def email(frequency):
    df = get_summary(-1)
    alerts = get_alerts().to_dict(orient='records')
    if len(alerts) > 0:
        alerts = alerts[0]
    vaccine = get_vaccination().to_dict(orient='records')[0]
    df['rolling_test_twenty_four'] = df['rolling_test_twenty_four'] * 100
    df['critical_care_pct'] = df['critical_care_pct'] * 100
    df['covid_pct'] = df['covid_pct'] * 100
    df = df.round(2)
    changed = df.loc[df['count'] == 1]
    date = get_times()
    past = Subscribers.query.filter_by(frequency=frequency)
    subscribers = pd.read_sql_query(past.statement, db.engine)
    emails = subscribers.email.unique()
    ontario = vis.get_testresults()
    ontario['Date'] = pd.to_datetime(ontario['Date'])
    ontario['Date'] = ontario['Date'].dt.strftime('%B %d')
    ontario = ontario.tail(1).to_dict(orient='records')[0]
    if frequency == 'daily' or frequency == 'weekly':
        for email in emails:
            try:
                temp = subscribers.loc[subscribers.email == email]
                token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
                my_regions = temp.region.unique()[:]
                temp_df = df.loc[df.HR_UID.isin(my_regions)]
                max_date = df.date.max().strftime("%Y-%m-%d")
                temp_changed = changed.loc[changed.HR_UID.isin(my_regions)]
                regions = temp_df.to_dict(orient='records')
                regions_changed = temp_changed.to_dict(orient='records')
                key = os.environ.get('EMAIL_API')
                sg = sendgrid.SendGridAPIClient(api_key=key)
                from_email = "mycovidreport@howsmyflattening.ca"
                to_email = email
                subject = "Your Personalized COVID-19 Report"
                html = render_template("alert_email.html",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
                text = render_template("alert_email.txt",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
                message = Mail(
                from_email=from_email,
                to_emails=to_email,
                subject='Your personalized COVID-19 report',
                plain_text_content=text,
                html_content=html)
                response = sg.send(message)
            except Exception as e:
                print(e)
    elif frequency == 'change':
        for email in emails:
            try:
                temp = subscribers.loc[subscribers.email == email]
                token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
                my_regions = temp.region.unique()[:]
                temp_df = df.loc[(df.HR_UID.isin(my_regions)) & (df['count'] == 1)]
                max_date = df.date.max().strftime("%Y-%m-%d")
                if len(temp_df) > 1:
                    temp_changed = changed.loc[changed.HR_UID.isin(my_regions)]
                    regions = temp_df.to_dict(orient='records')
                    regions_changed = temp_changed.to_dict(orient='records')
                    key = os.environ.get('EMAIL_API')
                    sg = sendgrid.SendGridAPIClient(api_key=key)
                    from_email = "mycovidreport@howsmyflattening.ca"
                    to_email = email
                    subject = "Your Personalized COVID-19 Report"
                    html = render_template("alert_email.html",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
                    text = render_template("alert_email.txt",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token,alerts=alerts,vaccine=vaccine,max_date=max_date)
                    message = Mail(
                    from_email=from_email,
                    to_emails=to_email,
                    subject='Your personalized COVID-19 report',
                    plain_text_content=text,
                    html_content=html)
                    response = sg.send(message)
            except Exception as e:
                print(e)


@bp.cli.command("tweet")
def tweet():
    mapping = {
    3526:"AlgomaHealth",
    3527:"BrantHealthUnit",
    3540:"CKPublicHealth",
    3530:"DurhamHealth",
    3558:"EOHU_tweet",
    3533:"GBPublicHealth",
    3534:"HNHealthUnit",
    3535:"HKPRDHU",
    3536:"RegionofHalton",
    3537:"hamphslib",
    3538:"HPEPublicHealth",
    3539:"HPPublicHealth",
    3541:"KFLAPH",
    3542:"lambton_ph",
    3543:"LGLHealthUnit",
    3544:"MLHealthUnit",
    3546:"NRPublicHealth",
    3547:"NBPSDHealthUnit",
    3549:"TheNWHU",
    3551:"OttawaHealth",
    3553:"regionofpeel",
    3555:"Ptbohealth",
    3556:"PorcupineHU",
    3565:"ROWPublicHealth",
    3557:"RCDHealthUnit",
    3560:"SMDhealthunit",
    3575:"SW_PublicHealth",
    3561:"PublicHealthSD",
    3562:"TBDHealthUnit",
    3563:"TimiskamingHU",
    3595:"TOPublicHealth",
    3566:"WDGPublicHealth",
    3568:"TheWECHU",
    3570:"YorkRegionGovt",
    6: "Ontario"
    }
    df = get_summary(-1)
    df['date'] = pd.to_datetime(df['date'])
    df['rolling_pop_prev'] = df['rolling_pop'].shift(7)
    df['change'] = df['rolling_pop'] - df['rolling_pop_prev']
    max_date = df.date.max().strftime("%Y-%m-%d")
    df['critical_care_pct'] = df['critical_care_pct'] * 100
    df['rolling_test_twenty_four'] = df['rolling_test_twenty_four'] * 100
    ont = df.loc[(df.date == df.date.max()) & (df.phu == 'Ontario')].tail(1).to_dict(orient='records')[0]
    df = df.loc[(df.date == df.date.max()) & (df.phu != 'Ontario')]
    risks = df.groupby('risk').count()['phu']
    df = df.sort_values(by='change')
    vaccine = get_vaccination().to_dict(orient='records')[0]
    ontario = pd.read_sql_table('ontario_covid_summary',con=db.engine)
    ontario['percent_positive'] = ontario['percent_positive_tests_in_last_day']
    ontario['percent_positive_trend'] = ontario['percent_positive'].diff(7)
    ontario['reported_date'] = pd.to_datetime(ontario['reported_date'])
    ontario['reported_date'] = ontario['reported_date'].dt.strftime('%B %d')
    ontario = ontario.tail(1).to_dict(orient='records')[0]
    date = get_times()
    up = '\U00002B06'
    down = '\U00002B07'
    text = f'\U00002615{ontario["reported_date"]}:#COVID19 in #Ontario\n{int(ontario["total_cases_change"])} new cases, {int(ontario["deaths_change"])} new deaths.\n{int(ontario["number_of_patients_hospitalized_with_covid-19"])} in hospital, {int(ontario["number_of_patients_in_icu_due_to_covid-19"])} in the ICU.\n{vaccine["previous_day_doses_administered"]} doses vaccinated yesterday ({round(vaccine["percentage_completed"],2)}% of Ontario)'
    reply1_text= f'\U0001F3E5Long Term Care Update:\nResidents: {int(ontario["total_positive_ltc_resident_cases_change"])} new cases and {int(ontario["total_ltc_resident_deaths_change"])} new deaths.\nHealth Care Workers: {int(ontario["total_positive_ltc_hcw_cases_change"])} new cases and {int(ontario["total_ltc_hcw_deaths_change"])} new deaths.'
    reply2_text = f'\U0001F9A0Variant Update:\nB117 (identified in the UK): {int(ontario["total_lineage_b.1.1.7_change"])} new cases, {int(ontario["total_lineage_b.1.1.7"])} cases todate.\nB1351 (identified in South Africa): {int(ontario["total_lineage_b.1.351_change"])} new cases, {int(ontario["total_lineage_b.1.351"])} cases todate.\nP1 (identified in Brazil): {int(ontario["total_lineage_p.1_change"])} new cases, {int(ontario["total_lineage_p.1"])} cases todate.'
    reply3_text = f"\U0001F4C8Ontario Key Indicators + Trend:\nCase Incidence: {round(ont['rolling_pop'],1)} {up if ont['rolling_pop_trend'] > 0 else down} ({date['rolling_pop'][0]})\nRt: {round(ont['rt_ml'],2)} {up if ont['rt_ml_trend'] > 0 else down} ({date['rt_ml'][0]})\nTesting < 24h: {int(ont['rolling_test_twenty_four'])}% {up if ont['rolling_test_twenty_four_trend'] > 0 else down} ({date['rolling_test_twenty_four'][0]})\nPercent Positivity: {round(ontario['percent_positive'],1)}% {up if ontario['percent_positive_trend'] > 0 else down} ({ontario['reported_date']})\nICU Occupancy: {int(ont['critical_care_pct'])}% {up if ont['critical_care_pct_trend'] > 0 else down} ({date['critical_care_pct'][0]})"
    reply4_text = f"\U0001F6A6Ontario community risk levels:\n1. High (≥ 10  new cases per 100k): {risks['High']} regions\n2. Medium (5-10 new cases per 100k): {risks['Medium']} regions\n3. Low (1-5 new cases per 100k): {risks['Low']} regions\n4. Very Low (<1 new cases per 100k): {risks['Very Low']} regions"
    reply5_text = f'\U00002B50Top 3 Curve Flatteners in the last 7 days (using daily cases per 100k):\n1.{df.phu.values[0]} (from {round(df.rolling_pop_prev.values[0],1)} to {round(df.rolling_pop.values[0],1)})\n2.{df.phu.values[1]} (from {round(df.rolling_pop_prev.values[1],1)} to {round(df.rolling_pop.values[1],1)})\n3.{df.phu.values[2]} (from {round(df.rolling_pop_prev.values[2],1)} to {round(df.rolling_pop.values[2],1)})'
    reply6_text = "\U0001F4E7Learn how the pandemic is affecting regions across the province. Sign up for a customized daily report delivered to your inbox at: https://howsmyflattening.ca/#/home"
    api_key = os.getenv('twitter_api_key')
    api_secret_key = os.getenv('twitter_api_secret_key')
    key = os.getenv('twitter_key')
    secret = os.getenv('twitter_secret')
    auth = tweepy.OAuthHandler(api_key, api_secret_key)
    auth.set_access_token(key, secret)
    api = tweepy.API(auth)
    file = f"6_{max_date}.jpeg"
    path = f"{os.getcwd()}/app/static/email/{file}"
    url = f"https://storage.googleapis.com/covid-data-analytics-hub.appspot.com/{file}"
    with open(path, 'wb') as handle:
        response = requests.get(url)
        handle.write(response.content)
    original_tweet = api.update_with_media(filename=path,status=text)
    reply1_tweet = api.update_status(status=reply1_text,
                                 in_reply_to_status_id=original_tweet.id,
                                 auto_populate_reply_metadata=True)
    reply2_tweet = api.update_status(status=reply2_text,
                                     in_reply_to_status_id=reply1_tweet.id,
                                     auto_populate_reply_metadata=True)
    reply3_tweet = api.update_status(status=reply3_text,
                                     in_reply_to_status_id=reply2_tweet.id,
                                     auto_populate_reply_metadata=True)
    reply4_tweet = api.update_status(status=reply4_text,
                                     in_reply_to_status_id=reply3_tweet.id,
                                     auto_populate_reply_metadata=True)
    reply5_tweet = api.update_status(status=reply5_text,
                                     in_reply_to_status_id=reply4_tweet.id,
                                     auto_populate_reply_metadata=True)
    reply6_tweet = api.update_status(status=reply6_text,
                                     in_reply_to_status_id=reply5_tweet.id,
                                     auto_populate_reply_metadata=True)

@bp.cli.command("status")
def status():
    issues = []
    date = get_times(False)
    now = datetime.now()
    rolling_pop = (now - pd.to_datetime(date['rolling_pop'])).days.tolist()[0]
    rolling_test_twenty_four = (now - pd.to_datetime(date['rolling_test_twenty_four'])).days.tolist()[0]
    critical_care_pct = (now - pd.to_datetime(date['critical_care_pct'])).days.tolist()[0]
    rt_ml = (now - pd.to_datetime(date['rt_ml'])).days.tolist()[0]
    percent_positive = (now - pd.to_datetime(date['percent_positive'])).days.tolist()[0]
    percent_vaccinated = (now - pd.to_datetime(date['percent_vaccinated'])).days.tolist()[0]
    rolling_pop_value = 3
    if rolling_pop > rolling_pop_value:
        issues.append(f"Case Incidence (Per 100,000 People) is behind by {rolling_pop} days. Acceptable range is: {rolling_pop_value}")

    rolling_test_twenty_four_value = 3
    if rolling_test_twenty_four > rolling_test_twenty_four_value:
        issues.append(f"Testing < 24h is behind by {rolling_test_twenty_four} days. Acceptable range is: {rolling_test_twenty_four_value}")

    critical_care_pct_value = 3
    if critical_care_pct > critical_care_pct_value:
        issues.append(f"ICU Occupancy is behind by {critical_care_pct} days. Acceptable range is: {critical_care_pct_value}")

    rt_ml_value = 3
    if rt_ml > rt_ml_value:
        issues.append(f"Rt is behind by {rt_ml} days. Acceptable range is: {rt_ml_value}")

    percent_positive_value = 8
    if percent_positive > percent_positive_value:
        issues.append(f"Percent positivity is behind by {percent_positive} days. Acceptable range is: {percent_positive_value}")

    percent_vaccinated_value = 8
    if percent_vaccinated > percent_vaccinated_value:
        issues.append(f"Percent vaccinated is behind by {percent_vaccinated} days. Acceptable range is: {percent_vaccinated_value}")

    if len(issues) > 0:
        key = os.environ.get('EMAIL_API')
        sg = sendgrid.SendGridAPIClient(api_key=key)
        from_email = "data@howsmyflattening.ca"
        to_email = ["farbod.abolhassani@utoronto.ca", "laura.rosella@utoronto.ca", "Benjamin.Fine@thp.ca", "zenita.hirji@utoronto.ca"]
        subject = f"{len(issues)}/6 elements out of date"
        html = render_template("data_email.html", issues=issues)
        text = render_template("data_email.txt", issues=issues)
        message = Mail(
        from_email=from_email,
        to_emails=to_email,
        subject=subject,
        plain_text_content=text,
        html_content=html)
        response = sg.send(message)







@cache.memoize(timeout=600)
def get_times(convert=True):
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615"
    positivity = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=2051486909"
    df = pd.read_csv(url)
    positive = pd.read_csv(positivity)
    df['date'] = pd.to_datetime(df['date'])
    positive['Date'] = pd.to_datetime(positive['Date'])
    df = df.merge(positive, left_on=['date', 'HR_UID'], right_on=['Date', 'HR_UID'], how='left')
    df = df.rename(columns={"% Positivity":"percent_positive"})
    if convert:
        df['date'] = df['date'].dt.strftime('%B %d')
    metrics = ["rolling_pop", "rolling_test_twenty_four", "critical_care_pct", "rt_ml", "percent_positive", "percent_vaccinated"]
    data = {"rolling_pop":[], "rolling_test_twenty_four":[], "critical_care_pct":[], "rt_ml":[], "percent_positive":[], "percent_vaccinated": []}
    for metric in metrics:
        temp = df.loc[df[metric].notna()].tail(1)
        date_refreshed = temp['date'].values[:]
        data[metric] = date_refreshed
    return data

@cache.memoize(timeout=600)
def get_vaccination(last=True):
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1488620091"
    df = pd.read_csv(url)
    df['date'] = pd.to_datetime(df['date'])
    if last:
        df = df.loc[df.date == df.date.max()]
    return df

@bp.route('/api/auth/twitter/', methods=['GET'])
@cache.cached(timeout=600)
def get_twitter():
    print('GET')
    print(request.args)
    return 'success', 200


@bp.route('/api/vaccination', methods=['GET'])
@cache.cached(timeout=600)
def send_vaccination():
    data = get_vaccination()
    data = data.to_json(orient='records',date_format='iso')
    return data

@bp.route('/api/times', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_reopening_times():
    data = get_times()
    return data


@bp.route('/api/alerts', methods=['GET'])
@cache.cached(timeout=600)
def send_alerts():
    df = get_alerts()
    data = df.to_json(orient='records')
    return data

@bp.route('/api/epi', methods=['GET'])
@cache.cached(timeout=600, query_string=True)
def get_percentages():
    HR_UID = request.args.get('HR_UID')
    filter = request.args.get('filter')
    # cases = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'}
    # cases_path = get_last_file(cases)
    cases_path = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=975084275"
    df = pd.read_csv(cases_path)
    date = "case_reported_date"
    df[date] = pd.to_datetime(df[date])
    df.loc[df.outbreak_related.isna(), 'outbreak_related'] = 'No'

    if HR_UID and int(HR_UID)!=0:
        df = df.loc[df.HR_UID == int(HR_UID)]

    if filter and int(filter) != -1:
        last_month = df[date].max() - pd.Timedelta(int(filter), unit='d')
        temp = df.loc[df[date] > last_month]
    else:
        temp = df
    gender = temp['client_gender'].value_counts().to_dict()
    gender_pct = temp['client_gender'].value_counts(True).to_dict()

    age_group = temp['age_group'].value_counts().to_dict()
    age_group_pct = temp['age_group'].value_counts(True).to_dict()

    ouctome = temp['outcome_1'].value_counts().to_dict()
    ouctome['Total'] = int(temp['outcome_1'].count())
    ouctome['Age'] = int(temp['outcome_1'].count())
    ouctome['Date'] = df[date].max().strftime('%B %d, %Y')
    ouctome_pct = temp['outcome_1'].value_counts(True).to_dict()
    ouctome_pct['Total'] = 1

    case_acquisition = temp['case_acquisition_info'].value_counts().to_dict()
    case_acquisition_pct = temp['case_acquisition_info'].value_counts(True).to_dict()

    outbreak = temp['outbreak_related'].value_counts().to_dict()
    outbreak_pct = temp['outbreak_related'].value_counts(True).to_dict()

    data = {
    "gender": gender,
    "age_group": age_group,
    "outcome":ouctome,
    "case_acquisition":case_acquisition,
    "outbreak":outbreak,
    "gender_pct": gender_pct,
    "age_group_pct": age_group_pct,
    "outcome_pct":ouctome_pct,
    "case_acquisition_pct":case_acquisition_pct,
    "outbreak_pct":outbreak_pct,
    }
    return json.dumps(data)


@cache.memoize(timeout=600)
def get_risk_df():
    df = pd.read_csv("https://data.ontario.ca/dataset/cbb4d08c-4e56-4b07-9db6-48335241b88a/resource/ce9f043d-f0d4-40f0-9b96-4c8a83ded3f6/download/response_framework.csv")
    phu_mapper={3895:3595,
    5183:3539,
    2244:3544,
    4913:3575,
    2261:3561,
    2268:3568,
    2227:3527,
    2230:3530,
    2258:3558,
    2236:3536,
    2237:3537,
    2246:3546,
    2265:3565,
    2266:3566,
    2270:3570,
    2253:3553,
    2240:3540,
    2233:3533,
    2241:3541,
    2255:3555,
    2262:3562,
    2260:3560,
    2238:3538,
    2242:3542,
    2249:3549,
    2234:3534,
    2235:3535,
    2243:3543,
    2263:3563,
    2226:3526,
    2247:3547,
    2256:3556,
    2257:3557,
    2251:3551
    }
    df['HR_UID'] = df['Reporting_PHU_id'].replace(phu_mapper)
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    return df


@bp.route('/api/risk', methods=['GET'])
@cache.cached(timeout=600, query_string=True)
def get_risk():
    location = request.args.get('location')
    if not location:
        return 'Missing location parameter', 400
    response = {}
    fsa = location[:3].upper()
    df = pd.read_csv("pccf_on.csv")
    HR_UID = df.loc[df.fsa == fsa]['HR_UID'].values
    if len(HR_UID) == 0:
        return 'Public health unit not found', 400
    df = get_summary(-1)
    risk_df = get_risk_df()
    response = {"FSA": fsa}
    risk = []
    color = []
    for phu in HR_UID:
        critical_care_patients = df.loc[df.HR_UID == phu]['critical_care_patients'].values[0]
        risk_location = risk_df.loc[risk_df.HR_UID == phu]['Status_PHU'].replace({"Lockdown":0, "Control": 1, "Restrict": 2, "Protect": 3, "Prevent": 4, "Other": 5}).values[-1]
        color.append(risk_location)
        if critical_care_patients >= 10:
            risk.append(4)
        elif critical_care_patients >= 5:
            risk.append(2)
        else:
            risk.append(0)
    response['Risk'] = sum(risk) / len(risk)
    response['Province'] = int(min(color))
    return response


def get_bot():
    location = request.args.get('location')
    if not location:
        return 'Missing location parameter', 400
    response = {"fsa":{}, "phu":{},"ontario":{}}
    fsa = location[:3].upper()
    sheetsConfig = [
        {'name':'FSA Data'}
    ]
    ws = restrictedsheetsHelper.getVizSheet('FSA Data')
    df = pd.DataFrame(ws.get_all_records())
    temp = df.loc[df.fsa == fsa]
    temp['cases_two_weeks'] = temp.cases.rolling(14).sum()
    temp['deaths_two_weeks'] = temp.deaths.rolling(14).sum()
    temp = temp.tail(1)
    response["fsa"]["cumulative_cases"] = int(temp["cumulative_cases"].values[0])
    response["fsa"]["cumulative_deaths"] = int(temp["cumulative_deaths"].values[0])
    response["fsa"]["cases_two_weeks"] = int(temp["cases_two_weeks"].values[0])
    response["fsa"]["deaths_two_weeks"] = int(temp["deaths_two_weeks"].values[0])
    response["fsa"]["date"] = temp["index"].values[0]
    response["fsa"]["location"] = fsa
    postal_code = location.upper()
    df = pd.read_csv("pccf_on.csv")
    ontario_cases = pd.read_csv("https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv")
    pop = pd.read_csv("https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/other/hr_map.csv")
    pop = pop.loc[pop.province == "Ontario"]
    pop['health_region'] = pop['health_region'].replace(POP)
    ontario_cases = pd.merge(ontario_cases,pop, left_on=['Reporting_PHU'], right_on=['health_region'], how='left')
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615"
    positivity = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=2051486909"
    ontario_hmf = pd.read_csv(url)
    positive = pd.read_csv(positivity)
    ontario_hmf['date'] = pd.to_datetime(ontario_hmf['date'])
    positive['Date'] = pd.to_datetime(positive['Date'])
    ontario_hmf = ontario_hmf.merge(positive, left_on=['date', 'HR_UID'], right_on=['Date', 'HR_UID'], how='left')
    df = df.loc[df.postal_code == postal_code]
    if len(df) > 0:
        HR_UID = df["HR_UID"].values[0]
        phu = ontario_cases.loc[ontario_cases.HR_UID == HR_UID]
        response["phu"]["location"] = phu["Reporting_PHU"].values[0]
        phu['Case_Reported_Date'] = pd.to_datetime(phu['Case_Reported_Date'])
        temp = phu.groupby(['Case_Reported_Date']).Row_ID.count().reset_index()
        temp['cases_two_weeks'] = temp['Row_ID'].rolling(14).sum()
        temp['cumulative_cases'] = temp['Row_ID'].cumsum()
        temp = temp.tail(1)
        response["phu"]["cases_two_weeks"] = int(temp["cases_two_weeks"].values[0])
        response["phu"]["cumulative_cases"] = int(temp["cumulative_cases"].values[0])
        response["phu"]["date"] = str(temp["Case_Reported_Date"].values[0]).split('T')[0]
        ## Last Two Weeks
        two_weeks = phu["Case_Reported_Date"].max() - pd.Timedelta(14, unit='d')
        temp = phu.loc[phu["Case_Reported_Date"] > two_weeks]
        age = temp['Age_Group'].value_counts(True)
        age_index = temp['Age_Group'].value_counts(True).index
        response["phu"]["age_group_1"] = age_index[0]
        response["phu"]["age_group_1_pct"] = age[0]
        response["phu"]["age_group_2"] = age_index[1]
        response["phu"]["age_group_2_pct"] = age[1]
        temp['Case_AcquisitionInfo'] =  temp['Case_AcquisitionInfo'].replace({'CC': 'Close Contact', 'No Epi-link': 'Community Spread', 'OB': 'Outbreak'})
        acquisition = temp['Case_AcquisitionInfo'].value_counts(True)
        acquisition_index = temp['Case_AcquisitionInfo'].value_counts(True).index
        response["phu"]["acquisition_source"] = acquisition_index[0]
        response["phu"]["acquisition_source_pct"] = acquisition[0]
        temp = ontario_hmf.loc[ontario_hmf.HR_UID == HR_UID]
        response["phu"]["rolling_pop"] = get_last(temp["rolling_pop"])
        response["phu"]["rolling_test_twenty_four"] = get_last(temp["rolling_test_twenty_four"])
        response["phu"]["confirmed_positive_icu"] = get_last(temp["confirmed_positive"])
        response["phu"]["critical_care_pct"] = get_last(temp["critical_care_pct"])
        response["phu"]["rt"] = get_last(temp["rt_ml"])
    phu = ontario_cases
    response["ontario"]["location"] = "Ontario"
    phu['Case_Reported_Date'] = pd.to_datetime(phu['Case_Reported_Date'])
    temp = phu.groupby(['Case_Reported_Date']).Row_ID.count().reset_index()
    temp['cases_two_weeks'] = temp['Row_ID'].rolling(14).sum()
    temp['cumulative_cases'] = temp['Row_ID'].cumsum()
    temp = temp.tail(1)
    response["ontario"]["cumulative_cases"] = int(temp["cumulative_cases"].values[0])
    response["ontario"]["cases_two_weeks"] = int(temp["cases_two_weeks"].values[0])
    response["ontario"]["date"] = str(temp["Case_Reported_Date"].values[0]).split('T')[0]
    two_weeks = phu["Case_Reported_Date"].max() - pd.Timedelta(14, unit='d')
    temp = phu.loc[phu["Case_Reported_Date"] > two_weeks]
    age = temp['Age_Group'].value_counts(True)
    age_index = temp['Age_Group'].value_counts(True).index
    response["ontario"]["age_group_1"] = age_index[0]
    response["ontario"]["age_group_1_pct"] = age[0]
    response["ontario"]["age_group_2"] = age_index[1]
    response["ontario"]["age_group_2_pct"] = age[1]
    temp['Case_AcquisitionInfo'] =  temp['Case_AcquisitionInfo'].replace({'CC': 'Close Contact', 'No Epi-link': 'Community Spread', 'OB': 'Outbreak'})
    acquisition = temp['Case_AcquisitionInfo'].value_counts(True)
    acquisition_index = temp['Case_AcquisitionInfo'].value_counts(True).index
    response["ontario"]["acquisition_source"] = acquisition_index[0]
    response["ontario"]["acquisition_source_pct"] = acquisition[0]
    return response, 200



@as_json
def get_testresults():
    df = pd.read_sql_table('covidtests', db.engine)
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df = df.sort_values('date')
    tests ={}

    deaths = {}
    investigations = {}
    negatives = {}
    positives = {}
    resolveds = {}
    totals = {}
    news = {}
    investigations_pct = {}
    negatives_pct = {}
    positives_pct = {}

    df['new'] = df.total.diff()

    for index, row in df.iterrows():
        date = str(row['date'].date())
        negative = row['negative']
        investigation = row['investigation']
        positive = row['positive']
        resolved = row['resolved']
        death = row['deaths']
        total = row['total']
        new = row['new']


        deaths[date] = death
        investigations[date] = investigation
        negatives[date] = negative
        positives[date] = positive
        resolveds[date] = resolved
        totals[date] = total
        if row['new']==row['new']:
            news[date] = new
        positives_pct[date] = positive/total
        negatives_pct[date] = negative/total
        investigations_pct[date] = investigation/total

    tests['Deaths'] = deaths
    tests['Under Investigation'] = investigations
    tests['Positive'] = positives
    tests['Negatives'] = negatives
    tests['Total tested'] = totals
    tests['New tests'] = news
    tests['Resolved'] = resolveds
    tests['Positive pct'] = positives_pct
    tests['Negative pct'] = negatives_pct
    tests['Investigation pct'] = investigations_pct

    return  tests
