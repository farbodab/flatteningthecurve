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
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import date

@bp.route('/covid/tests', methods=['GET', 'POST'])
@as_json
def tests():
    # Data source David Madras
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

@bp.route('/covid/testsnew', methods=['GET', 'POST'])
@as_json
def testsnew():
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options)
    urlpage = "https://www.ontario.ca/page/2019-novel-coronavirus#section-0"
    driver.implicitly_wait(30)
    driver.get(urlpage)
    text = driver.find_element_by_tag_name("table").text
    items = text.split("\n")
    new = []
    new.append(items[2])
    new.append(items[4])
    new.append(items[5])
    new.append(items[15])
    new.append(items[16])
    labels = ["positive","resolved", "deaths", "total", "investigation"]
    today = date.today()
    tests_dict = {}

    for i, row in enumerate(new):
        thing = row.split()
        number = thing[-2]
        number = number.replace(",","")
        number = number.replace("*","")
        tests_dict[labels[i]] = int(number)

    tests_dict["negative"] = tests_dict["total"] - tests_dict["positive"] - tests_dict["investigation"]
    c = CovidTests.query.filter_by(date=today).first()
    if not c:
        c = CovidTests(date=today, positive=tests_dict['positive'], negative=tests_dict['negative'],resolved=tests_dict['resolved'], deaths=tests_dict['deaths'], investigation=tests_dict['investigation'], total=tests_dict['total'])
        db.session.add(c)
        db.session.commit()
    else:
        if ((c.positive == tests_dict['positive']) and (c.negative == tests_dict['negative']) and (c.resolved == tests_dict['resolved']) and (c.deaths == tests_dict['deaths']) and (c.total == tests_dict['total']) and (c.investigation == tests_dict['investigation'])):
            pass
        else:
            c.positive = tests_dict['positive']
            c.negative = tests_dict['negative']
            c.resolved = tests_dict['resolved']
            c.investigation = tests_dict['investigation']
            c.deaths = tests_dict['deaths']
            c.total = tests_dict['total']
            db.session.add(c)
            db.session.commit()
    driver.quit()
    return 'success',200

@bp.route('/covid/cases', methods=['GET', 'POST'])
@as_json
def cases():
    # Data source Open Data Collab
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv"
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


@bp.route('/covid/capacity', methods=['GET', 'POST'])
@as_json
def capacity():
    # data source Petr Smirnov
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

@bp.route('/covid/icu', methods=['GET'])
@as_json
def capacityicu():
    df = pd.read_csv('CCSO.csv')
    date = "30-03-2020"
    date = datetime.strptime(date,"%d-%m-%Y")
    for index, row in df.iterrows():
        region = row['Region']
        lhin = row['LHIN']
        critical_care_beds = row['# Critical Care Beds']
        critical_care_patients = row['# Critical Care Patients']
        vented_beds = row['# Vented Beds']
        vented_patients = row['# Vented Patients']
        suspected_covid = row['# Suspected COVID-19']
        suspected_covid_ventilator = row['# Suspected COVID-19 Patients with Invasive Ventilation']
        confirmed_positive = row['# Confirmed Positive COVID-19']
        confirmed_positive_ventilator = row['# Confirmed Positive COVID-19 Patients with Invasive Ventilation']
        confirmed_negative = row['# Confirmed Negative COVID-19']
        c = ICUCapacity(date=date, region=region, lhin=lhin, critical_care_beds=critical_care_beds, critical_care_patients=critical_care_patients, vented_beds=vented_beds, vented_patients=vented_patients, suspected_covid=suspected_covid, suspected_covid_ventilator=suspected_covid_ventilator, confirmed_positive=confirmed_positive, confirmed_positive_ventilator=confirmed_positive_ventilator, confirmed_negative=confirmed_negative)
        db.session.add(c)
        db.session.commit()
    return 'success',200

@bp.route('/covid/international', methods=['GET', 'POST'])
@as_json
def international():
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    countries = ["Italy", "Korea, South", "Spain", "United Kingdom", "France", "US"]
    df = df.loc[df["Country/Region"].isin(countries)]
    df = df.drop(['Lat', 'Long', 'Province/State'], axis=1).groupby("Country/Region").sum().T
    df = df.diff().reset_index()
    df['Date']= pd.to_datetime(df['index'])
    for index, row in df.iterrows():
        date = row['Date']
        for country in countries:
            cases = row[country]
            if cases != cases:
                cases = 0
            c = InternationalData.query.filter_by(country=country, date=date).first()
            if not c:
                c = InternationalData(country=country, date=date, cases=cases)
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


@bp.route('/covid/source', methods=['POST'])
@as_json
def new_source():
    if request.is_json:
        items = request.get_json()
        for item in items:
            name = item['name']
            source = item['source']
            compiled = item['compiled']
            description = item['description']
            c = Source(source=source, name=name, description=description, compiled=compiled)
            db.session.add(c)
            db.session.commit()
        return 'success',200
    else:
        return 'must use json', 400



@bp.route('/covid/update', methods=['GET'])
@as_json
def update():
    testsnew()
    cases()
    international()
    return 'success',200
