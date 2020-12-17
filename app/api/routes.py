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
    positivity = "https://docs.google.com/spreadsheets/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=1769215322"
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
        loop = {"phu":[], "HR_UID":[], "date":[], "rolling":[], "rolling_pop":[], "rolling_pop_trend":[],"rolling_test_twenty_four":[], "rolling_test_twenty_four_trend":[],"confirmed_positive":[], "critical_care_beds":[],"critical_care_patients":[],"critical_care_pct":[], "critical_care_pct_trend":[],"rt_ml":[], "rt_ml_trend":[],"percent_positive": [],"percent_positive_trend": [], "prev": [], "risk": [], "count": []}
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
        df = pd.DataFrame(loop)
    return df

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
    past = Subscribers.query.filter_by(email=email).all()
    sign_up(email,regions,frequency)
    if past:
        for item in past:
            db.session.delete(item)
    for region in regions:
        s = Subscribers(email=email,frequency=frequency,region=region)
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

@bp.cli.command("email")
@click.argument("frequency")
def email(frequency):
    df = get_summary(-1)
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
            temp = subscribers.loc[subscribers.email == email]
            token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
            my_regions = temp.region.unique()[:]
            temp_df = df.loc[df.HR_UID.isin(my_regions)]
            temp_changed = changed.loc[changed.HR_UID.isin(my_regions)]
            regions = temp_df.to_dict(orient='records')
            regions_changed = temp_changed.to_dict(orient='records')
            key = os.environ.get('EMAIL_API')
            sg = sendgrid.SendGridAPIClient(api_key=key)
            from_email = "mycovidreport@howsmyflattening.ca"
            to_email = email
            subject = "Your Personalized COVID-19 Report"
            html = render_template("alert_email.html",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token)
            text = render_template("alert_email.txt",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token)
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
    elif frequency == 'change':
        for email in emails:
            temp = subscribers.loc[subscribers.email == email]
            token = jwt.encode({'email': email}, os.getenv('SECRET_KEY'), algorithm='HS256').decode('utf-8')
            my_regions = temp.region.unique()[:]
            temp_df = df.loc[(df.HR_UID.isin(my_regions)) & (df['count'] == 1)]
            if len(temp_df) > 1:
                temp_changed = changed.loc[changed.HR_UID.isin(my_regions)]
                regions = temp_df.to_dict(orient='records')
                regions_changed = temp_changed.to_dict(orient='records')
                key = os.environ.get('EMAIL_API')
                sg = sendgrid.SendGridAPIClient(api_key=key)
                from_email = "mycovidreport@howsmyflattening.ca"
                to_email = email
                subject = "Your Personalized COVID-19 Report"
                html = render_template("alert_email.html",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token)
                text = render_template("alert_email.txt",regions=regions,regions_changed=regions_changed,ontario=ontario,date=date,token=token)
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


@cache.memoize(timeout=600)
def get_times():
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615"
    positivity = "https://docs.google.com/spreadsheets/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=1769215322"
    df = pd.read_csv(url)
    positive = pd.read_csv(positivity)
    df['date'] = pd.to_datetime(df['date'])
    positive['Date'] = pd.to_datetime(positive['Date'])
    df = df.merge(positive, left_on=['date', 'HR_UID'], right_on=['Date', 'HR_UID'], how='left')
    df = df.rename(columns={"% Positivity":"percent_positive"})
    df['date'] = df['date'].dt.strftime('%B %d')
    metrics = ["rolling_pop", "rolling_test_twenty_four", "critical_care_pct", "rt_ml", "percent_positive"]
    data = {"rolling_pop":[], "rolling_test_twenty_four":[], "critical_care_pct":[], "rt_ml":[], "percent_positive":[]}
    for metric in metrics:
        temp = df.loc[df[metric].notna()].tail(1)
        date_refreshed = temp['date'].values[:]
        data[metric] = date_refreshed
    return data


@bp.route('/api/times', methods=['GET'])
@as_json
@cache.cached(timeout=600)
def get_reopening_times():
    data = get_times()
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
    risk_df = pd.read_csv("https://docs.google.com/spreadsheets/d/1eJcsbs7vSt3rfV_lAH94PbWvAs9u6OxR9KXRpl45MWc/export?format=csv&id=1eJcsbs7vSt3rfV_lAH94PbWvAs9u6OxR9KXRpl45MWc&gid=1865799742")
    response = {"FSA": fsa}
    risk = []
    color = []
    for phu in HR_UID:
        critical_care_patients = df.loc[df.HR_UID == phu]['critical_care_patients'].values[0]
        risk_location = risk_df.loc[risk_df.HR_UID == phu]['stage'].values[0]
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


@bp.route('/api/bot', methods=['GET'])
@cache.cached(timeout=600, query_string=True)
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
    positivity = "https://docs.google.com/spreadsheets/d/1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk/export?format=csv&id=1npx8yddDIhPk3wuZuzcB6sj8WX760H1RUFNEYpYznCk&gid=1769215322"
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
