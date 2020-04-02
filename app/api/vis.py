from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from datetime import datetime
import requests
from app import db
from app.models import *
from app.api import bp
import pandas as pd
import io
import requests

def get_results():
    c = Covid.query.filter_by(province="Ontario")
    df = pd.read_sql(c.statement, db.engine)
    case_count = df.groupby("date").case_id.count().cumsum().reset_index()
    case_count = case_count.loc[case_count.case_id > 100]
    df = df.loc[df.date.isin(case_count.date)]
    df = df.groupby("date").case_id.count().reset_index()
    df['case_id'] = df['case_id']*0.05
    df['case_id'] = df['case_id'].rolling(min_periods=1, window=8).sum()

    data = {'date':[], 'region':[], 'value':[]}
    data['date'] += list(range(len(df['date'].tolist())))
    data['region'] += ['Ontario']*len(df['date'].tolist())
    data['value'] += df['case_id'].tolist()

    provinces = ["Italy", "South Korea", "Singapore"]
    df = df.loc[df.date.isin(case_count.date.values)].reset_index()
    for province in provinces:
        c = Comparison.query.filter_by(province=province)
        df = pd.read_sql(c.statement, db.engine)
        case_count = df['count'].cumsum()
        df['case_count'] = case_count
        df = df.loc[df['case_count'] > 100].reset_index()
        df['count'] = df['count']*0.05
        df['count'] = df['count'].rolling(min_periods=1, window=8).sum()
        df = df.reset_index()

        data['date'] += list(range(len(df['date'].tolist())))
        data['region'] += df['province'].tolist()
        data['value'] += df['count'].tolist()

    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

'''@bp.route('/covid/results/date', methods=['GET'])
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
    return jsonify(provines_dict)'''

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

    data = {'date':[], 'region':[], 'value':[]}

    for region in regions:
        df = dfs.loc[dfs.region == region]
        df = df.groupby("date").case_id.count().cumsum().reset_index()
        date = datetime.strptime("2020-02-28","%Y-%m-%d")
        df = df.loc[df.date > date]
        df['date_str'] = df['date'].astype(str)

        data['date'] += df['date'].tolist()
        data['region'] += [region]*len(df['date'].tolist())
        data['value'] += df['case_id'].tolist()

    df = dfs.groupby("date").case_id.count().cumsum().reset_index()
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df['date_str'] = df['date'].astype(str)

    data['date'] += df['date'].tolist()
    data['region'] += ['Ontario']*len(df['date'].tolist())
    data['value'] += df['case_id'].tolist()
    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_phunew():
    c = Covid.query.filter_by(province="Ontario")
    dfs = pd.read_sql(c.statement, db.engine)
    regions = dfs.region.unique()
    data = {'date':[], 'region':[], 'value':[]}
    for region in regions:
        df = dfs.loc[dfs.region == region]
        df = df.groupby("date").case_id.count().reset_index()
        date = datetime.strptime("2020-02-28","%Y-%m-%d")
        df = df.loc[df.date > date]
        df['date_str'] = df['date'].astype(str)

        data['date'] += df['date'].tolist()
        data['region'] += [region]*len(df['date'].tolist())
        data['value'] += df['case_id'].tolist()

    df = dfs.groupby("date").case_id.count().reset_index()
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df['date_str'] = df['date'].astype(str)

    data['date'] += df['date'].tolist()
    data['region'] += ["Ontario"]*len(df['date'].tolist())
    data['value'] += df['case_id'].tolist()
    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_growth():
    dfs = pd.read_sql_table('covid', db.engine)
    regions = dfs.province.unique()
    data = {'date':[], 'region':[], 'value':[]}
    for region in regions:
        df = dfs.loc[dfs.province == region]
        df = df.groupby("date").case_id.count().cumsum().reset_index()
        df = df.loc[df.case_id > 100].reset_index()
        data['date'] += list(range(len(df['date'].tolist())))
        data['region'] += [region]*len(df['date'].tolist())
        data['value'] += df['case_id'].tolist()

    df = dfs.groupby("date").case_id.count().cumsum().reset_index()
    df = df.loc[df.case_id > 100].reset_index()

    data['date'] += list(range(len(df['date'].tolist())))
    data['region'] += ['Canada']*len(df['date'].tolist())
    data['value'] += df['case_id'].tolist()

    dfs = pd.read_sql_table('internationaldata', db.engine)
    regions = dfs.country.unique()
    for region in regions:
        df = dfs.loc[dfs.country == region]
        df = df['cases'].cumsum().reset_index()
        df = df.loc[df['cases'] > 100].reset_index()
        data['date'] += list(range(len(df['level_0'].tolist())))
        data['region'] += [region]*len(df['level_0'].tolist())
        data['value'] += df['cases'].tolist()

    df_final = pd.DataFrame(data, columns=['region', 'date', 'value'])

    return df_final

def get_growth_recent():
    dfs = pd.read_sql_table('covid', db.engine)
    regions = dfs.province.unique()
    data = {'region':[], 'value':[], 'cumulative':[]}
    for region in regions:
        df = dfs.loc[dfs.province == region]
        df_two = df.groupby("date").case_id.count().cumsum().reset_index()
        df = df.groupby("date").case_id.count().reset_index()
        df = df.tail(7)
        data['region'] += [region]
        data['value'] += [df['case_id'].sum()]
        data['cumulative'] += [*df_two['case_id'].tail(1).values]

    df_two = dfs.groupby("date").case_id.count().cumsum().reset_index()
    df = dfs.groupby("date").case_id.count().reset_index()
    df = df.tail(7)
    data['region'] += ['Canada']
    data['value'] += [df['case_id'].sum()]
    data['cumulative'] += [*df_two['case_id'].tail(1).values]

    dfs = pd.read_sql_table('internationaldata', db.engine)
    regions = dfs.country.unique()
    for region in regions:
        df = dfs.loc[dfs.country == region]
        df_two = df['cases'].cumsum().reset_index()
        df = df['cases'].reset_index()
        df = df.tail(7)
        data['region'] += [region]
        data['value'] += [df['cases'].sum()]
        data['cumulative'] += [*df_two['cases'].tail(1).values]

    df_final = pd.DataFrame(data, columns=['region', 'value', 'cumulative'])

    return df_final

def get_testresults():
    df = pd.read_sql_table('covidtests', db.engine)
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df = df.sort_values('date')
    tests ={}

    dates = []
    deaths = []
    investigations = []
    negatives = []
    positives = []
    resolveds = []
    totals = []
    new_deaths = []
    new_tests = []
    new_positives = []
    investigations_pct = []
    negatives_pct = []
    positives_pct = []

    df['new_tests'] = df.total.diff()
    df['new_deaths'] = df.deaths.diff()
    df['new_positives'] = df.positive.diff()

    for index, row in df.iterrows():
        date = str(row['date'].date())
        negative = row['negative']
        investigation = row['investigation']
        positive = row['positive']
        resolved = row['resolved']
        death = row['deaths']
        total = row['total']

        new_t = row['new_tests']
        new_d = row['new_deaths']
        new_p = row['new_positives']



        dates += [date]
        deaths += [death]
        investigations += [investigation]
        negatives += [negative]
        positives += [positive]
        resolveds += [resolved]
        totals += [total]
        if row['new_tests']==row['new_tests']:
            new_tests += [new_t]
        else:
            new_tests += [0]

        if row['new_deaths']==row['new_deaths']:
            new_deaths += [new_d]
        else:
            new_deaths += [0]

        if row['new_positives']==row['new_positives']:
            new_positives += [new_p]
        else:
            new_positives += [0]


        positives_pct += [positive/total]
        negatives_pct += [negative/total]
        investigations_pct += [investigation/total]

    data = {
        'Date': dates,
        'Deaths': deaths,
        'New deaths': new_deaths,
        'Under Investigation': investigations,
        'Positives': positives,
        'New positives': new_positives,
        'Negatives': negatives,
        'Total tested': totals,
        'New tests': new_tests,
        'Resolved': resolveds,
        'Positive pct': positives_pct,
        'Negative pct': negatives_pct,
        'Investigation pct': investigations_pct,
    }
    df = pd.DataFrame(data, columns=['Date', 'Deaths', 'New deaths','Under Investigation', 'Positives', 'New positives','Negatives', 'Total tested', 'New tests', 'Resolved', 'Positive pct', 'Negative pct', 'Investigation pct'])

    return  df

def get_icu_capacity():
    df = pd.read_sql_table('icucapacity', db.engine)

    replace = {"1. ESC":"Erie St. Clair", "2. SW": "South West", "3. WW": "Waterloo Wellington", "4. HNHB": "Hamilton Niagara Haldimand Brant", "5. CW": "Central West", "6. MH": "Mississauga Halton", "7. TC": "Toronto Central", "8. Central": "Central", "9. CE": "Central East", "10. SE": "South East", "11. Champlain": "Champlain", "12. NSM": "North Simcoe Muskoka", "13. NE": "North East", "14. NW": "North West"}
    df.lhin = df.lhin.replace(replace)


    df['non_covid'] = df['critical_care_patients'] - df['confirmed_negative'] - df['confirmed_positive'] - df['suspected_covid']
    df['critical_care_pct'] = df['critical_care_patients'] / df['critical_care_beds']
    df['vented_pct'] = df['vented_patients'] / df['vented_beds']
    df['confirmed_positive_pct'] = df['confirmed_positive'] / df['critical_care_beds']
    df['confirmed_negative_pct'] = df['confirmed_negative'] / df['critical_care_beds']
    df['suspected_covid_pct'] = df['suspected_covid'] / df['critical_care_beds']
    df['non_covid_pct'] = df['non_covid'] / df['critical_care_beds']
    df['residual_beds'] = df['critical_care_beds'] - df['critical_care_patients']
    df['residual_ventilators'] = df['vented_beds'] - df['vented_patients']

    return df