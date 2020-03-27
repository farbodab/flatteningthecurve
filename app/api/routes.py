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

@bp.route('/covid/tests', methods=['GET', 'POST'])
@as_json
def tests():
    url = "https://docs.google.com/spreadsheets/d/152uZD6ApMM87PisTK1QYU4TdPMkzbJDjpjwl1gaaQKI/export?format=csv&id=152uZD6ApMM87PisTK1QYU4TdPMkzbJDjpjwl1gaaQKI&gid=0"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    for index, row in df.iterrows():
        date = row['Date']
        date = datetime.strptime(date,"%m-%d-%Y")
        negative = row['Negative']
        investigation = row['Currently Under Investigation']
        positive = row['Confirmed Positive']
        resolved = row['Resolved']
        deaths = row['Deaths']
        total = row['Total number of patients approved for COVID-19 testing to date']

        c = CovidTests.query.filter_by(date=date).first()
        if not c:
            c = CovidTests(date=date, negative=negative, investigation=investigation, positive=positive, resolved=resolved, deaths=deaths, total=total)
            db.session.add(c)
            db.session.commit()
        else:
            if (c.negative == negative and (c.positive == positive) and (c.investigation == investigation) and (c.resolved == resolved) and (c.deaths == deaths) and (c.total == total)):
                pass
            else:
                c.negative = negative
                c.positive = positive
                c.investigation = investigation
                c.resolved = resolved
                c.deaths = deaths
                c.total = total
                db.session.add(c)
                db.session.commit()
    return 'success',200

@bp.route('/covid/cases', methods=['GET', 'POST'])
@as_json
def cases():
    url = "https://docs.google.com/spreadsheets/d/1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo/export?format=csv&id=1D6okqtBS3S2NRC7GFVHzaZ67DuTw7LX49-fqSLwJyeo"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df = df.iloc[3:]
    df.columns = ['case_id','provincial_case_id','age','sex','health_region','province','country','date_report','report_week','travel_yn','travel_history_country','locally_acquired','case_source','additional_info','additional_source']
    for index, row in df.iterrows():
        case_id = row['case_id']
        age = row['age']
        sex = row['sex']
        region = row['health_region']
        province = row['province']
        country = row['country']
        date = row['date_report']
        date = datetime.strptime(date,"%d-%m-%Y")
        travel = row['travel_yn']
        if travel == 'Not Reported':
            travel = -1
        travelh = row['travel_history_country']
        c = Covid.query.filter_by(case_id=case_id).first()
        if not c:
            c = Covid(case_id=case_id, age=age, sex=sex, region=region, province=province, country=country, date=date, travel=travel, travelh=travelh)
            db.session.add(c)
            db.session.commit()
        else:
            if ((c.age == age) and (c.sex == sex) and (c.region == region) and (c.province == province) and (c.country == country) and (c.date == date) and (c.travel==travel) and (c.travelh==travelh)):
                pass
            else:
                c.age = age
                c.sex = sex
                c.region = region
                c.province = province
                c.country = country
                c.date = date
                c.travel = travel
                c.travelh = travelh
                db.session.add(c)
                db.session.commit()
    return 'success',200

@bp.route('/covid/update', methods=['GET'])
@as_json
def update():
    tests()
    cases()
    return 'success',200

@bp.route('/covid/capacity', methods=['GET', 'POST'])
@as_json
def capacity():
    url = "https://docs.google.com/spreadsheets/d/1l6dyKXB0k2c5X13Lsfvy6I6g10Uh8ias1P7mLTAqxT8/export?format=csv&id=1l6dyKXB0k2c5X13Lsfvy6I6g10Uh8ias1P7mLTAqxT8&gid=1666640270"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    for index, row in df.iterrows():
        name = row['PHU']
        icu = row['Intensive Care']
        acute = row['Other Acute']
        c = PHUCapacity(name=name, icu=icu, acute=acute)
        db.session.add(c)
        db.session.commit()
    return 'success',200


@bp.route('/covid/comparison', methods=['POST'])
@as_json
def new_covid():
    if request.is_json:
        items = request.get_json()
        for item in items:
            date = datetime.strptime(item['date'],"%Y-%m-%d")
            province = item['province']
            confirmed = item['new']
            c = Comparison(province=province, count=confirmed, date=date)
            db.session.add(c)
            db.session.commit()
        return 'success',200
    else:
        return 'must use json', 400


@bp.route('/covid/results', methods=['GET'])
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

@bp.route('/covid/phu', methods=['GET'])
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

    df = dfs.groupby("date").case_id.count().cumsum().reset_index()
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
    df['date_str'] = df['date'].astype(str)
    province_dict = df.set_index('date_str')['case_id'].to_dict()
    provines_dict["Ontario"] = province_dict
    return provines_dict

@bp.route('/covid/phunew', methods=['GET'])
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

@bp.route('/covid/growth', methods=['GET'])
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

    dfs = pd.read_sql_table('comparison', db.engine)
    regions = dfs.province.unique()
    for region in regions:
        df = dfs.loc[dfs.province == region]
        df = df.groupby("date")['count'].cumsum().reset_index()
        df = df.loc[df['count'] > 100].reset_index()
        province_dict = df['count'].to_dict()
        provines_dict[region] = province_dict



    return provines_dict

@bp.route('/covid/testresults', methods=['GET'])
@as_json
def get_testresults():
    df = pd.read_sql_table('covidtests', db.engine)
    date = datetime.strptime("2020-02-28","%Y-%m-%d")
    df = df.loc[df.date > date]
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
        positives_pct[date] = negative/total
        negatives_pct[date] = positive/total
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
