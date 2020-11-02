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
@cache.cached(timeout=50)
@as_json
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
@cache.cached(timeout=50)
@as_json
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
@cache.cached(timeout=50)
@as_json
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
@cache.cached(timeout=50)
@as_json
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
@cache.cached(timeout=50)
@as_json
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

@bp.route('/api/summary', methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
def get_summary_metrics():
    HR_UID = request.args.get('HR_UID')
    if not HR_UID:
        HR_UID = -1
    # final = {'classification':'public', 'stage': 'transformed','source_name':'summary', 'table_name':'ontario',  'type': 'csv'}
    # df_path = get_last_file(final)
    url = "https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1804151615"
    df = pd.read_csv(url)
    df['date'] = pd.to_datetime(df['date'])
    if int(HR_UID)==0:
        df = df.loc[df.phu == 'Ontario']
    elif int(HR_UID)>0:
        df = df.loc[df.HR_UID == int(HR_UID)]
    else:
        loop = {"phu":[], "HR_UID":[], "date":[], "rolling":[], "rolling_pop":[], "rolling_test_twenty_four":[], "confirmed_positive":[], "critical_care_pct":[], "rt_ml":[]}
        unique = df.HR_UID.unique()
        for hr in unique:
            temp = df.loc[df.HR_UID == hr]
            if len(temp) > 0:
                loop['phu'].append(get_last(temp['phu']))
                loop['HR_UID'].append(hr)
                loop['date'].append(get_last(temp['date']))
                loop['rolling'].append(get_last(temp['rolling']))
                loop['rolling_pop'].append(get_last(temp['rolling_pop']))
                loop['rolling_test_twenty_four'].append(get_last(temp['rolling_test_twenty_four']))
                loop['confirmed_positive'].append(get_last(temp['confirmed_positive']))
                loop['critical_care_pct'].append(get_last(temp['critical_care_pct']))
                loop['rt_ml'].append(get_last(temp['rt_ml']))
        temp = df.loc[df.phu == 'Ontario']
        loop['phu'].append('Ontario')
        loop['HR_UID'].append(-1)
        loop['date'].append(get_last(temp['date']))
        loop['rolling'].append(get_last(temp['rolling']))
        loop['rolling_pop'].append(get_last(temp['rolling_pop']))
        loop['rolling_test_twenty_four'].append(get_last(temp['rolling_test_twenty_four']))
        loop['confirmed_positive'].append(get_last(temp['confirmed_positive']))
        loop['critical_care_pct'].append(get_last(temp['critical_care_pct']))
        loop['rt_ml'].append(get_last(temp['rt_ml']))
        df = pd.DataFrame(loop)
    data = df.to_json(orient='records', date_format='iso')
    return data

@bp.route('/api/times', methods=['GET'])
@cache.cached(timeout=3600)
@as_json
def get_reopening_times():
    df = pd.read_sql_table('metric_update_date', db.engine)
    df = df.loc[df.recent == True]
    data = []
    for index, row in df.iterrows():
        source = row['source']
        date_refreshed = row['date_refreshed']
        data.append({'source':source, 'date_refreshed':date_refreshed})
    return data

@bp.route('/api/epi', methods=['GET'])
@cache.cached(timeout=3600, query_string=True)
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
