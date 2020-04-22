from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from datetime import datetime, timedelta
from datetime import date
import requests
import csv
from app import db
from app.models import *
from app.api import bp
import pandas as pd
import numpy as np
import io
import os
from bs4 import BeautifulSoup
import urllib.request
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from app.tools.covidpdftocsv import covidpdftocsv
from app.tools.pdfparse import extract
import math
from sqlalchemy import text
from sqlalchemy import sql
import csv

########################################
############ONTARIO DATA################
########################################


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
        positive = row['Total Cases']
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
    return

def getnpis():
    url = "https://raw.githubusercontent.com/jajsmith/COVID19NonPharmaceuticalInterventions/master/npi_canada.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df.dropna(subset=['start_date'],inplace=True)
    df = df.fillna("NULL")
    for index, row in df.iterrows():
        start_date = row['start_date']
        end_date = row['end_date']
        country = row['country']
        region = row['region']
        subregion = row['subregion']
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
        oxford_testing_code = row['oxford_testing_code']
        oxford_tracing_code = row['oxford_tracing_code']
        source_url = row['source_url']
        source_organization = row['source_organization']
        source_organization_two = row['source_organization_2']
        source_category = row['source_category']
        source_title = row['source_title']
        source_full_text = row['source_full_text']
        c = NPIInterventions.query.filter_by(start_date=start_date, region=region, intervention_summary=intervention_summary).first()
        if not c:
            c = NPIInterventions(start_date=start_date,
            country=country, region=region, subregion=subregion, intervention_summary=intervention_summary,
            intervention_category=intervention_category, target_population_category=target_population_category,
            enforcement_category=enforcement_category, oxford_government_response_category=oxford_government_response_category,
            oxford_closure_code=oxford_closure_code, oxford_public_info_code=oxford_public_info_code,
            oxford_travel_code=oxford_travel_code, oxford_geographic_target_code=oxford_geographic_target_code,
            oxford_fiscal_measure_cad=oxford_fiscal_measure_cad, oxford_monetary_measure=oxford_monetary_measure,
            source_url=source_url, source_organization=source_organization, source_organization_two=source_organization_two,
            source_category=source_category, source_title=source_title, source_full_text=source_full_text, oxford_testing_code=oxford_testing_code,
            oxford_tracing_code=oxford_tracing_code)
            if end_date != "NULL":
                c.end_date = end_date
        else:
            c.start_date = start_date
            if end_date != "NULL":
                c.end_date = end_date
            c.country = country
            c.region = region
            c.subregion = subregion
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
            c.oxford_testing_code = oxford_testing_code
            c.oxford_tracing_code = oxford_tracing_code
            c.source_url = source_url
            c.source_organization = source_organization
            c.source_organization_two = source_organization_two
            c.source_category = source_category
            c.source_title = source_title
            c.source_full_text = source_full_text
        db.session.add(c)
        db.session.commit()
    return

def capacityicu(date):
    # Extract csv from pdf
    extract.extractCCSO(['', './CCSO.pdf', 1, 188, 600, 519, 959])
    df = pd.read_csv('CCSO.csv')
    for index, row in df.iterrows():
        region = row['Region']
        lhin = row['LHIN']
        critical_care_beds = row['# Critical Care Beds']
        critical_care_patients = row['# Critical Care Patients']
        vented_beds = row['# Baseline Vented Beds'] + row['# Expanded Vented Beds']
        vented_patients = row['# Vented Patients']
        suspected_covid = row['# Suspected COVID-19']
        confirmed_positive = row['# Confirmed Positive COVID-19']
        confirmed_positive_ventilator = row['# Confirmed Positive COVID-19 Patients with Invasive Ventilation']
        c = ICUCapacity(date=date, region=region, lhin=lhin, critical_care_beds=critical_care_beds, critical_care_patients=critical_care_patients, vented_beds=vented_beds, vented_patients=vented_patients, suspected_covid=suspected_covid, confirmed_positive=confirmed_positive, confirmed_positive_ventilator=confirmed_positive_ventilator)
        db.session.add(c)
        db.session.commit()
    return

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
    return

########################################
############CANADA DATA################
########################################

def cases():
    # Data source Open Data Collab
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv"
    r = requests.get(url, stream=True)
    for row in csv.DictReader(r.iter_lines(decode_unicode=True)):
    # for index, row in df.iterrows():
        case_id = row['case_id']
        age = row['age']
        sex = row['sex']
        region = row['health_region']
        province = row['province']
        country = row['country']
        date = row['date_report']
        date = datetime.strptime(date,"%d-%m-%Y")
        travel = row['travel_yn']
        travelh = row['travel_history_country']
        c = Covid.query.filter_by(case_id=case_id).first()
        if not c:
            c = Covid(case_id=case_id, age=age, sex=sex, region=region, province=province, country=country, date=date, travel=travel, travelh=travelh)
            db.session.add(c)
        else:
            if not all((
                (c.age == age),
                (c.sex == sex),
                (c.region == region),
                (c.province == province),
                (c.country == country),
                (c.date == date),
                (c.travel==travel),
                (c.travelh==travelh)
            )):
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
    return

def getcanadamortality():
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['date'] = pd.to_datetime(df['date_death_report'],dayfirst=True)
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
    return

def getcanadarecovered():
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/recovered_cumulative.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['date_recovered'] = pd.to_datetime(df['date_recovered'],dayfirst=True)
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
    return

def getcanadatested():
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/testing_cumulative.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['date_testing'] = pd.to_datetime(df['date_testing'],dayfirst=True)
    df = df.fillna(-1)
    for index, row in df.iterrows():
        date = row['date_testing']
        province = row['province']
        cumulative_testing = row['cumulative_testing']
        c = CanadaTesting.query.filter_by(date=date, province=province).first()
        if not c:
            c = CanadaTesting(date=date, province=province, cumulative_testing=cumulative_testing)

        db.session.add(c)
        db.session.commit()
    return

def getcanadamobility_google():
    # From global data
    try:
        url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
        r = requests.get(url, stream=True)
        for row in csv.DictReader(r.iter_lines(decode_unicode=True)):
            region = row['country_region']
            subregion = row['sub_region_1']
            date = row['date']
            if region == 'Canada':
                if subregion is not '':
                    region = subregion

                def add_transport(date, region, transportation_type, value):
                    if value == '':
                        value = -999

                    m = MobilityTransportation.query.filter_by(date=date, region=region, transportation_type=transportation_type).limit(1).first()
                    if not m:
                        m = MobilityTransportation(date=date, region=region, transportation_type=transportation_type, value=value)
                        print("Add transport mobility data for region: {}, date: {}, type: {}, value: {}".format(region, date, transportation_type, value))
                        db.session.add(m)

                add_transport(date, region, 'Retail & recreation', row['retail_and_recreation_percent_change_from_baseline'])
                add_transport(date, region, 'Grocery & pharmacy', row['grocery_and_pharmacy_percent_change_from_baseline'])
                add_transport(date, region, 'Parks', row['parks_percent_change_from_baseline'])
                add_transport(date, region, 'Transit stations', row['transit_stations_percent_change_from_baseline'])
                add_transport(date, region, 'Workplace', row['workplaces_percent_change_from_baseline'])
                add_transport(date, region, 'Residential', row['residential_percent_change_from_baseline'])
        db.session.commit()
    except Exception as err:
        print("failed to get data", err)
    return

def getcanadamobility_apple():
    start_date = None
    end_date = datetime.today()

    max_date = MobilityTransportation.query.order_by(text('date desc')).limit(1).first()
    if not max_date:
        start_date = end_date + timedelta(-14)
    else:
        start_date = max_date.date# + timedelta(1)

    datesToTry = [start_date + timedelta(x) for x in range(int((end_date - start_date).days))]
    datesToTry.reverse()

    ## ATTENION! this url likes to change and will need to be updated whenever the data starts falling behind. Find it here: https://www.apple.com/covid19/mobility
    base_url = 'https://covid19-static.cdn-apple.com/covid19-mobility-data/2006HotfixDev11/v1/en-us/applemobilitytrends-'
    regions = ['Toronto', 'Vancouver', 'Canada', 'Calgary', 'Edmonton', 'Halifax', 'Montreal', 'Ottawa']

    #EXAMPLE https://covid19-static.cdn-apple.com/covid19-mobility-data/2005HotfixDev13/v1/en-us/applemobilitytrends-2020-04-13.csv
    for dt in datesToTry:
        try:
            datetag = dt.strftime('%Y-%m-%d')
            url = '{}{}.csv'.format(base_url, datetag)
            df = None
            try:
                s = requests.get(url).content
                df = pd.read_csv(io.StringIO(s.decode('utf-8')))
            except:
                continue

            df = df[df['region'].isin(regions)]

            # Get all date columns (i.e. not kind, name, category) and insert record for each
            date_columns = [x for x in list(df.columns) if x not in ['geo_type', 'region', 'transportation_type']]

            for index, row in df.iterrows():
                region = row['region']
                transport = row['transportation_type']
                for col in date_columns:
                    value = row[col]
                    if math.isnan(value):
                        continue

                    m = MobilityTransportation.query.filter_by(date=col, region=region, transportation_type=transport).limit(1).first()
                    if not m:
                        m = MobilityTransportation(date=col, region=region, transportation_type=transport, value=value)
                        print("Add transport mobility data for region: {}, transport: {}, date: {}, value: {}".format(region, transport, col, value))
                        db.session.add(m)
                        db.session.commit()

            break
        except Exception as err:
            print("failed to get data for {}".format(dt), err)
    return

def getgovernmentresponse():
    url = "https://ocgptweb.azurewebsites.net/CSVDownload"
    r = requests.get(url, stream=True)

    def parse_val(val):
        if val == -1:
            return sql.null()
        else:
            return val

    for row in csv.DictReader(r.iter_lines(decode_unicode=True)):
    # for index, row in df.iterrows():
        date = row['Date']
        country = row['CountryName']
        country_code = row['CountryCode']
        s1_school_closing = parse_val(row['S1_School closing'])
        s1_is_general = parse_val(row['S1_IsGeneral'])
        s1_notes = parse_val(row['S1_Notes'])
        s2_workplace_closing = parse_val(row['S2_Workplace closing'])
        s2_is_general = parse_val(row['S2_IsGeneral'])
        s2_notes = parse_val(row['S2_Notes'])
        s3_cancel_public_events = parse_val(row['S3_Cancel public events'])
        s3_is_general = parse_val(row['S3_IsGeneral'])
        s3_notes = parse_val(row['S3_Notes'])
        s4_close_public_transport = parse_val(row['S4_Close public transport'])
        s4_is_general = parse_val(row['S4_IsGeneral'])
        s4_notes = parse_val(row['S4_Notes'])
        s5_public_information_campaigns = parse_val(row['S5_Public information campaigns'])
        s5_is_general = parse_val(row['S5_IsGeneral'])
        s5_notes = parse_val(row['S5_Notes'])
        s6_restrictions_on_internal_movement = parse_val(row['S6_Restrictions on internal movement'])
        s6_is_general = parse_val(row['S6_IsGeneral'])
        s6_notes = parse_val(row['S6_Notes'])
        s7_international_travel_controls = parse_val(row['S7_International travel controls'])
        s7_notes = parse_val(row['S7_Notes'])
        s8_fiscal_measures = parse_val(row['S8_Fiscal measures'])
        s8_notes = parse_val(row['S8_Notes'])
        s9_monetary_measures = parse_val(row['S9_Monetary measures'])
        s9_notes = parse_val(row['S9_Notes'])
        s10_emergency_investment_in_healthcare = parse_val(row['S10_Emergency investment in health care'])
        s10_notes = parse_val(row['S10_Notes'])
        s11_investement_in_vaccines = parse_val(row['S11_Investment in Vaccines'])
        s11_notes = parse_val(row['S11_Notes'])
        s12_testing_framework = parse_val(row['S12_Testing framework'])
        s12_notes = parse_val(row['S12_Notes'])
        s13_contact_tracing = parse_val(row['S13_Contact tracing'])
        s13_notes = parse_val(row['S13_Notes'])
        confirmed_cases = parse_val(row['ConfirmedCases'])
        confirmed_deaths = parse_val(row['ConfirmedDeaths'])
        stringency_index = parse_val(row['StringencyIndex'])
        stringency_index_for_display = parse_val(row['StringencyIndexForDisplay'])

        g = GovernmentResponse.query.filter_by(date=date, country=country).first()
        if not g:
            g = GovernmentResponse(
                date=date,
                country=country,
                country_code=country_code,
                s1_school_closing=s1_school_closing,
                s2_workplace_closing=s2_workplace_closing,
                s3_cancel_public_events=s3_cancel_public_events,
                s4_close_public_transport=s4_close_public_transport,
                s5_public_information_campaigns=s5_public_information_campaigns,
                s6_restrictions_on_internal_movement=s6_restrictions_on_internal_movement,
                s7_international_travel_controls=s7_international_travel_controls,
                s8_fiscal_measures=s8_fiscal_measures,
                s9_monetary_measures=s9_monetary_measures,
                s10_emergency_investment_in_healthcare=s10_emergency_investment_in_healthcare,
                s11_investement_in_vaccines=s11_investement_in_vaccines,
                s12_testing_framework=s12_testing_framework,
                s13_contact_tracing=s13_contact_tracing,
                s1_is_general=s1_is_general,
                s1_notes=s1_notes,
                s2_is_general=s2_is_general,
                s2_notes=s2_notes,
                s3_is_general=s3_is_general,
                s3_notes=s3_notes,
                s4_is_general=s4_is_general,
                s4_notes=s4_notes,
                s5_is_general=s5_is_general,
                s5_notes=s5_notes,
                s6_is_general=s6_is_general,
                s6_notes=s6_notes,
                s7_notes=s7_notes,
                s8_notes=s8_notes,
                s9_notes=s9_notes,
                s10_notes=s10_notes,
                s11_notes=s11_notes,
                s12_notes=s12_notes,
                s13_notes=s13_notes,
                confirmed_cases=confirmed_cases,
                confirmed_deaths=confirmed_deaths,
                stringency_index=stringency_index,
                stringency_index_for_display=stringency_index_for_display)

            db.session.add(g)

    db.session.commit()
    return


########################################
###########INTERNATIONAL DATA###########
########################################

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
    return

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
    return

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
    return

def getinternationaltested():
    url = "https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-all-observations.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Date'] = pd.to_datetime(df['Date'])
    df = df.fillna(-1)
    for index, row in df.iterrows():
        date = row['Date']
        region = row['Entity'].split('-')[0]
        cumulative_testing = row['Cumulative total']
        c = InternationalTesting.query.filter_by(date=date, region=region).first()
        if not c:
            c = InternationalTesting(date=date, region=region, cumulative_testing=cumulative_testing)

        db.session.add(c)
        db.session.commit()
    return

#TODO: remove, not used?
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

def getnpiusa():
    url = "https://raw.githubusercontent.com/Keystone-Strategy/covid19-intervention-data/master/complete_npis_inherited_policies.csv"
    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))

    def datefunc(x):
        try:
            return datetime.strptime(x, '%m/%d/%Y %H:%M')
        except:
            return sql.null()

    df = df.fillna(-1)
    for index, row in df.iterrows():
        start_date = row['start_date']
        end_date = row['end_date']
        county = row['county']
        state = row['state']
        npi = row['npi']
        citation = row['citation']
        note = row['note']

        start_date = datefunc(start_date)
        end_date = datefunc(end_date)

        if county == -1:
            county = sql.null()
        if citation == -1:
            citation = sql.null()
        if note == -1:
            note = sql.null()

        n = NPIInterventionsUSA.query.filter_by(state=state, county=county, npi=npi).first()
        # Update in case data changed
        if n:
            n.start_date = start_date
            n.end_date = end_date
            n.state = state
            n.county = county
            n.npi = npi
            n.citation = citation
            n.note = note
        else:
            n = NPIInterventionsUSA(start_date=start_date, end_date=end_date, state=state, county=county, npi=npi, citation=citation, note=note)
            db.session.add(n)

    db.session.commit()
    return

########################################
###########SOURCE DATA###########
########################################

@bp.route('/covid/viz', methods=['GET'])
@as_json
def new_viz():
    if os.environ['FLASK_CONFIG'] == 'development':
        url = "https://docs.google.com/spreadsheets/u/0/d/1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE/export?format=csv&id=1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE&gid=0"
    elif os.environ['FLASK_CONFIG'] == 'production':
        url = "https://docs.google.com/spreadsheets/u/0/d/1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE/export?format=csv&id=1ttbFlC3_EKCpMkF3U2FpK0y9Ta4q4kG1tMOYmn3QlHE&gid=803348886"
    s=requests.get(url).content
    data = io.StringIO(s.decode('utf-8'))
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        header = row['header']
        category = row['category']
        content = row['content']
        viz = row['viz']
        thumbnail = row['thumbnail']
        text = row['text']
        mobileHeight = row['mobileHeight']
        desktopHeight = row['desktopHeight']
        page = row['page']
        order = row['order']
        row_z = row['row']
        column = row['column']

        c = Viz.query.filter_by(header=header).first()
        if not c:
            c = Viz(header=header, category=category, content=content,
            viz=viz, thumbnail=thumbnail, text=text, mobileHeight=mobileHeight,
            desktopHeight=desktopHeight, page=page, order=order, row=row_z,
            column=column)
            db.session.add(c)
            db.session.commit()
        else:
            c.category = category
            c.content = content
            c.viz = viz
            c.mobileHeight = mobileHeight
            c.desktopHeight = desktopHeight
            c.thumbnail = thumbnail
            c.text=text
            c.page=page
            c.order = order
            c.row = row_z
            c.column = column
            db.session.add(c)
            db.session.commit()
    return 'success',200



@bp.route('/covid/source', methods=['GET'])
@as_json
def new_source():
    if os.environ['FLASK_CONFIG'] == 'development':
        url = "https://docs.google.com/spreadsheets/d/1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc/export?format=csv&id=1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc&gid=0"
    elif os.environ['FLASK_CONFIG'] == 'production':
        url = "https://docs.google.com/spreadsheets/d/1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc/export?format=csv&id=1UHDUYuqXCVkdPZTH-Z8TM_9CIDChYV77aGa_2-_2EFc&gid=1130875452"
    s=requests.get(url).content
    data = io.StringIO(s.decode('utf-8'))
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        name = row['Name']
        source = row['Source']
        description = row['Description']
        data_feed_type = row['Data feed type']
        link = row['Link of source']
        refresh = row['Refresh']
        contributor = row['Contributor']
        contact = row['Contributor contact']
        download = row['Download Link']

        c = Source.query.filter_by(name=name).first()
        if not c:
            c = Source(name=name, source=source, description=description,
            data_feed_type=data_feed_type, link=link, refresh=refresh,
            contributor=contributor, contact=contact, download=download)
            db.session.add(c)
            db.session.commit()
        else:
            c.source = source
            c.description = description
            c.data_feed_type = data_feed_type
            c.link = link
            c.refresh = refresh
            c.contributor = contributor
            c.contact = contact
            c.download = download
            db.session.add(c)
            db.session.commit()
    return 'success',200


########################################
###########UPDATE DATA##################
########################################


@bp.route('/covid/update', methods=['GET'])
@as_json
def update():
    testsnew()
    international()
    return 'success',200
