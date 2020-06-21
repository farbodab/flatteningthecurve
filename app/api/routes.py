from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
import plotly.graph_objects as go
from datetime import datetime
import requests
from app import db, cache
from app.models import *
from app.api import bp
from app.api import vis
import pandas as pd
import io
import requests

PHU = {'the_district_of_algoma':'The District of Algoma Health Unit',
 'brant_county':'Brant County Health Unit',
 'durham_regional':'Durham Regional Health Unit',
 'grey_bruce':'Grey Bruce Health Unit',
 'haldimand_norfolk':'Haldimand-Norfolk Health Unit',
 'haliburton_kawartha_pine_ridge_district':'Haliburton, Kawartha, Pine Ridge District Health Unit',
 'halton_regional':'Halton Regional Health Unit',
 'city_of_hamilton':'City of Hamilton Health Unit',
 'hastings_and_prince_edward_counties':'Hastings and Prince Edward Counties Health Unit',
 'huron_county':'Huron County Health Unit',
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
 'city_of_toronto':'City of Toronto Health Unit'}

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
     "Huron Perth":"Huron County Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
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
    df = df.loc[df.viz != 'NaN']
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
        "phu": row["phu"], "viz_title": row["viz_title"],
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
        "download": row["download"]})
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
    weekly_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1053507889")
    testing_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1206518301")
    rt_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=428679599")
    icu_df = pd.read_csv("https://docs.google.com/spreadsheets/d/19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA/export?format=csv&id=19LFZWy85MVueUm2jYmXXE6EC3dRpCPGZ05Bqfv5KyGA&gid=1132863788")

    data = []
    for phu_select in PHU.values():
        temp_dict = {}
        temp_dict["PHU"] = phu_select
        temp = weekly_df.loc[weekly_df.PHU == phu_select]
        temp = temp.sort_values('Date')
        try:
            temp_dict["weekly"] = str(temp.tail(1)['Cases'].values[0])
        except:
            temp_dict["weekly"] = "NaN"

        temp = testing_df.loc[testing_df.PHU == phu_select]
        temp = temp.sort_values('Date')
        try:
            temp_dict["testing"] = str(temp.tail(1)['Percentage in 24 hours_7dayrolling'].values[0])
        except:
            temp_dict["testing"] = "NaN"

        temp = rt_df.loc[rt_df.PHU == phu_select]
        temp = temp.sort_values('date')
        try:
            temp_dict["rt"] = str(temp.tail(1)['ML'].values[0])
        except:
            temp_dict["rt"] = "NaN"

        temp = icu_df.loc[icu_df.PHU == phu_select]
        temp = temp.sort_values('date')
        try:
            temp_dict["icu"] = str(temp.tail(1)['critical_care_pct'].values[0])
        except:
            temp_dict["icu"] = "NaN"

        data.append(temp_dict)

    return data


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
