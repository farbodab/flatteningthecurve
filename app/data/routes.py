from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from datetime import datetime
import requests
from app import db
from app.models import *
from app.api import bp
import pandas as pd
import numpy as np
import io
import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from datetime import date


########################################
############ONTARIO DATA################
########################################

@bp.route('/covid/testsnew', methods=['GET', 'POST'])
@as_json
def testsnew():
    url = "https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/ed270bb8-340b-41f9-a7c6-e8ef587e6d11/download/covidtesting.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Reported Date'] = pd.to_datetime(df['Reported Date'])
    date_include = datetime.strptime("2020-02-04","%Y-%m-%d")
    df = df.loc[df['Reported Date'] > date_include]

    for index, row in df.iterrows():
        date = row['Reported Date']
        negative = row['Confirmed Negative']
        investigation = row['Under Investigation']
        positive = row['Confirmed Positive']
        resolved = row['Resolved']
        deaths = row['Deaths']
        hospitalized = row['Number of patients hospitalized with COVID-19']
        icu = row['Number of patients in ICU with COVID-19']
        ventilator = row['Number of patients in ICU on a ventilator with COVID-19']
        total = row['Total patients approved for testing as of Reporting Date']

        if resolved != resolved:
            resolved = 0
        if deaths != deaths:
            deaths = 0

        if negative != negative:
            negative = total - positive - investigation

        c = CovidTests.query.filter_by(date=date).first()
        if not c:
            c = CovidTests(date=date, negative=negative, investigation=investigation, positive=positive, resolved=resolved, deaths=deaths, total=total)
            if hospitalized==hospitalized:
                c.hospitalized = hospitalized
            if icu==icu:
                c.icu = icu
            if ventilator==ventilator:
                c.ventilator = ventilator
            db.session.add(c)
            db.session.commit()
        else:
            if ((c.negative == negative) and (c.positive == positive) and (c.investigation == investigation) and (c.resolved == resolved) and (c.deaths == deaths) and (c.total == total) and (c.hospitalized == hospitalized) and (c.icu == icu) and (c.ventilator == ventilator)):
                pass
            else:
                c.negative = negative
                c.positive = positive
                c.investigation = investigation
                c.resolved = resolved
                c.deaths = deaths
                c.total = total
                if hospitalized==hospitalized:
                    c.hospitalized = hospitalized
                if icu==icu:
                    c.icu = icu
                if ventilator==ventilator:
                    c.ventilator = ventilator
                db.session.add(c)
                db.session.commit()
    return 'success',200

@bp.route('/covid/npi', methods=['GET', 'POST'])
@as_json
def getnpis():
    url = "https://docs.google.com/spreadsheets/d/16jZ8tdPS9x8kRHAi5CRh8iKfDshg0AMbfFLJ9ysgV9U/export?format=csv&id=16jZ8tdPS9x8kRHAi5CRh8iKfDshg0AMbfFLJ9ysgV9U&gid=1137367827"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df = df.fillna("NULL")
    for index, row in df.iterrows():
        start_date = row['start_date']
        end_date = row['end_date']
        country = row['country']
        region = row['region']
        intervention_summary = row['intervention_summary']
        intervention_category = row['intervention_category']
        target_population_category = row['target_population_category']
        enforcement_category = row['enforcement_category']
        oxford_government_response_category = row['oxford_government_response_category']
        oxford_closure_code = row['oxford_closure_code']
        oxford_public_info_code = row['oxford_public_info_code']
        oxford_travel_code = row['oxford_travel_code']
        oxford_geographic_target_code = row['oxford_geographic_target_code']
        oxford_fiscal_measure_cad = row['oxford_fiscal_measure_cad']
        oxford_monetary_measure = row['oxford_monetary_measure']
        source_url = row['source_url']
        source_organization = row['source_organization']
        source_organization_two = row['source_organization_2']
        source_category = row['source_category']
        source_title = row['source_title']
        source_full_text = row['source_full_text']
        c = NPIInterventions.query.filter_by(start_date=start_date, region=region, intervention_summary=intervention_summary).first()
        if not c:
            c = NPIInterventions(start_date=start_date,
            country=country, region=region, intervention_summary=intervention_summary,
            intervention_category=intervention_category, target_population_category=target_population_category,
            enforcement_category=enforcement_category, oxford_government_response_category=oxford_government_response_category,
            oxford_closure_code=oxford_closure_code, oxford_public_info_code=oxford_public_info_code,
            oxford_travel_code=oxford_travel_code, oxford_geographic_target_code=oxford_geographic_target_code,
            oxford_fiscal_measure_cad=oxford_fiscal_measure_cad, oxford_monetary_measure=oxford_monetary_measure,
            source_url=source_url, source_organization=source_organization, source_organization_two=source_organization_two,
            source_category=source_category, source_title=source_title, source_full_text=source_full_text)
            if end_date != "NULL":
                c.end_date = end_date
        else:
            c.start_date = start_date
            if end_date != "NULL":
                c.end_date = end_date
            c.country = country
            c.region = region
            c.intervention_summary = intervention_summary
            c.intervention_category = intervention_category
            c.target_population_category = target_population_category
            c.enforcement_category = enforcement_category
            c.oxford_government_response_category = oxford_government_response_category
            c.oxford_closure_code = oxford_closure_code
            c.oxford_public_info_code = oxford_public_info_code
            c.oxford_travel_code = oxford_travel_code
            c.oxford_geographic_target_code = oxford_geographic_target_code
            c.oxford_fiscal_measure_cad = oxford_fiscal_measure_cad
            c.source_url = source_url
            c.source_organization = source_organization
            c.source_organization_two = source_organization_two
            c.source_category = source_category
            c.source_title = source_title
            c.source_full_text = source_full_text
        db.session.add(c)
        db.session.commit()
    return 'success',200


@bp.route('/covid/icu', methods=['GET'])
@as_json
def capacityicu():
    df = pd.read_csv('CCSO.csv')
    date = "01-04-2020"
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

########################################
############CANADA DATA################
########################################

@bp.route('/covid/cases', methods=['GET', 'POST'])
@as_json
def cases():
    # Data source Open Data Collab
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
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

@bp.route('/covid/canadamortality', methods=['GET', 'POST'])
@as_json
def getcanadamortality():
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['date'] = pd.to_datetime(df['date_death_report'])
    df = df.fillna("NULL")
    df = df.replace("NA", "NULL")
    for index, row in df.iterrows():
        death_id = row['death_id']
        province_death_id = row['province_death_id']
        age = row['age']
        sex = row['sex']
        region = row['health_region']
        province = row['province']
        country = row['country']
        date = row['date']
        death_source = row['death_source']

        c = CanadaMortality.query.filter_by(death_id=death_id).first()
        if not c:
            c = CanadaMortality(death_id=death_id, province_death_id=province_death_id,
            age= age, sex=sex, region=region, province=province, country=country, date=date,
            death_source=death_source)

        else:
            c.death_id = death_id
            c.province_death_id = province_death_id
            c.age = age
            c.sex = sex
            c.region = region
            c.province = province
            c.country = country
            c.date = date
            c.death_source = death_source

        db.session.add(c)
        db.session.commit()
    return 'success',200

@bp.route('/covid/canadarecovered', methods=['GET', 'POST'])
@as_json
def getcanadarecovered():
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/recovered_cumulative.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['date_recovered'] = pd.to_datetime(df['date_recovered'])
    df = df.fillna(-1)
    for index, row in df.iterrows():
        date = row['date_recovered']
        province = row['province']
        cumulative_recovered = row['cumulative_recovered']
        c = CanadaRecovered.query.filter_by(date=date, province=province).first()
        if not c:
            c = CanadaRecovered(date=date, province=province, cumulative_recovered=cumulative_recovered)

        db.session.add(c)
        db.session.commit()
    return 'success',200

########################################
###########INTERNATIONAL DATA###########
########################################

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

@bp.route('/covid/internationalmortality', methods=['GET', 'POST'])
@as_json
def getinternationalmortality():
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"
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
            c = InternationalMortality.query.filter_by(country=country, date=date).first()
            if not c:
                c = InternationalMortality(country=country, date=date, deaths=cases)
                db.session.add(c)
                db.session.commit()
    return 'success',200

@bp.route('/covid/internationalrecovered', methods=['GET', 'POST'])
@as_json
def getinternationalrecovered():
    url = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"
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
            c = InternationalRecovered.query.filter_by(country=country, date=date).first()
            if not c:
                c = InternationalRecovered(country=country, date=date, recovered=cases)
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

########################################
###########SOURCE DATA###########
########################################




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


########################################
###########UPDATE DATA##################
########################################


@bp.route('/covid/update', methods=['GET'])
@as_json
def update():
    testsnew()
    international()
    return 'success',200
