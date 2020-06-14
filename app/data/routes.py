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
import urllib.request
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from app.tools.covidpdftocsv import covidpdftocsv
from app.tools.covid_19_mc_interactive_model import scrape as interactiveScraper
from app.tools.ontario_health_unit_IDEA_model import scrape as ideaScraper
import math
from sqlalchemy import text
from sqlalchemy import sql
import csv
from app.export import sheetsHelper
import re
import asyncio
import sys

########################################
############ONTARIO DATA################
########################################

def confirmed_ontario():
    field_map = {
        "Row_ID":"row_id",
        "Accurate_Episode_Date": "accurate_episode_date",
        "Case_Reported_Date": "case_reported_date",
        "Specimen_Date": "specimen_reported_date",
        "Test_Reported_Date": "test_reported_date",
        "Age_Group":"age_group",
        "Client_Gender":"client_gender",
        "Case_AcquisitionInfo": "case_acquisitionInfo",
        "Outcome1": "outcome1",
        "Outbreak_Related": "outbreak_related",
        "Reporting_PHU": "reporting_phu",
        "Reporting_PHU_Address": "reporting_phu_address",
        "Reporting_PHU_City": "reporting_phu_city",
        "Reporting_PHU_Postal_Code": "reporting_phu_postal_code",
        "Reporting_PHU_Website": "reporting_phu_website",
        "Reporting_PHU_Latitude":"reporting_phu_latitude",
        "Reporting_PHU_Longitude": "reporting_phu_longitude",
    }
    url = "https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv"
    cases = {case.row_id:case for case in ConfirmedOntario.query.all()}
    cases_max = [int(case.row_id) for case in ConfirmedOntario.query.all()]
    req = requests.get(url)

    print('ontario case data being refreshed')
    df = pd.read_csv(url)
    df = df.fillna(sql.null())
    df = df.replace("12:00:00 AM", sql.null())
    # cases_max = max(cases_max)
    # df = df.loc[df.Row_ID > 28523]
    for index, row in df.iterrows():
        try:
            if int(row["Row_ID"]) in cases:
                daily_status = cases.get(int(row["Row_ID"]))
                for header in field_map.keys():
                    setattr(daily_status,field_map[header],row[header])
                db.session.add(daily_status)
                db.session.commit()
            else:
                c = ConfirmedOntario()
                for header in field_map.keys():
                    setattr(c,field_map[header],row[header])
                db.session.add(c)
                db.session.commit()
        except Exception as e:
            print(e)
            print(f'failed to update case {row["Row_ID"]}')
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')

    db.session.commit()

def testsnew():
    url = "https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/ed270bb8-340b-41f9-a7c6-e8ef587e6d11/download/covidtesting.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df = df.dropna(how='all')
    df['Reported Date'] = pd.to_datetime(df['Reported Date'])
    date_include = datetime.strptime("2020-02-04","%Y-%m-%d")
    df = df.loc[df['Reported Date'] > date_include]
    print('ontario testing data being refreshed')
    new = False
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
            new = True
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
    if new:
        return 'New'
    else:
        return 'Same'

def testsnew_faster():

    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Firefox(options=options)
    url= "https://www.ontario.ca/page/how-ontario-is-responding-covid-19"
    driver.implicitly_wait(100)
    driver.get(url)
    tables = driver.find_elements_by_tag_name("table")

    new = False
    try:
        for table in tables:
            caption = table.find_element_by_tag_name('caption')
            if not 'Summary of cases' in caption.text:
                continue

            # Get date from table header
            pattern = 'Summary of cases.*to (.*)'
            match = re.search(pattern, caption.text)
            date = match.group(1)
            date = datetime.strptime(date, '%B %d, %Y')
            date = datetime.strftime(date, '%Y-%m-%d')

            headers = [x.text for x in table.find_element_by_tag_name('thead').find_elements_by_tag_name('th')]
            rows = table.find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')
            values = {}

            for row in rows:
                row_values = [x.text for x in row.find_elements_by_tag_name('td')]

                key = row_values[0]
                value = row_values[1]
                # strip extras
                key = ''.join([i for i in key if not i.isdigit()])
                value = ''.join([i for i in value if i.isdigit()])
                if row_values[0]:
                    values[key] = value

            def getValue(key):
                try:
                    return int(values[key])
                except:
                    raise Exception("Couldnt find column {}, double check column names haven't changed at https://www.ontario.ca/page/how-ontario-is-responding-covid-19".format(key))

            # Get most recent data
            negative = None#getValue('Confirmed Negative')
            investigation = getValue('Currently under investigation')
            positive = getValue('Number of cases')
            resolved = getValue('Resolved')
            deaths = getValue('Total number of deaths')
            hospitalized = getValue('Number of patients currently hospitalized with COVID-') #Needed to strip the reference numbers thats why only COVID-
            icu = getValue('Number of patients currently in ICU with COVID-')
            ventilator = getValue('Number of patients currently in ICU on a ventilator with COVID-')
            total = getValue('Total tests completed')

            if negative == None:
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
                new = True
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
            break
    except Exception as e:
        driver.quit()
        raise

    driver.quit()

    if new:
        return 'New'
    else:
        return 'Same'

def getnpis():
    url = "https://raw.githubusercontent.com/jajsmith/COVID19NonPharmaceuticalInterventions/master/npi_canada.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df.dropna(subset=['start_date'],inplace=True)
    df = df.fillna("NULL")
    print('npi canada data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
    df = pd.read_csv('CCSO.csv')
    for index, row in df.iterrows():
        region = row['Region']
        lhin = row['LHIN']
        critical_care_beds = row['# Critical Care Beds']
        critical_care_patients = row['# Critical Care Patients']
        vented_beds = row['# Expanded Vented Beds']
        vented_patients = row['# Vented Patients']
        suspected_covid = row['# Suspected COVID-19']
        confirmed_positive = row['# Confirmed Positive COVID-19']
        confirmed_positive_ventilator = row['# Confirmed Positive COVID-19 Patients with Invasive Ventilation']
        c = ICUCapacity(date=date, region=region, lhin=lhin, critical_care_beds=critical_care_beds, critical_care_patients=critical_care_patients, vented_beds=vented_beds, vented_patients=vented_patients, suspected_covid=suspected_covid, confirmed_positive=confirmed_positive, confirmed_positive_ventilator=confirmed_positive_ventilator)
        db.session.add(c)
        db.session.commit()
    return

def capacityicu_auto():
    df = pd.read_sql_table('icucapacity', db.engine)
    maxdate = df.iloc[df['date'].idxmax()]['date']

    # Look from last date on
    start_date = maxdate + timedelta(days=1)
    end_date = datetime.today()

    def daterange(start_date, end_date):
        for n in range(int ((end_date - start_date).days)):
            yield start_date + timedelta(n)

    for single_date in daterange(start_date, end_date):
        csv = 'CCSO_{}.csv'.format(single_date.strftime('%Y%m%d'))
        if not os.path.exists(csv):
            continue

        date = single_date.strftime('%Y-%m-%d')

        df = pd.read_csv(csv)
        for index, row in df.iterrows():
            region = row['Region']
            lhin = row['LHIN']
            critical_care_beds = row['# Critical Care Beds']
            critical_care_patients = row['# Critical Care Patients']
            vented_beds = row['# Expanded Vented Beds']
            vented_patients = row['# Vented Patients']
            suspected_covid = row['# Suspected COVID-19']
            confirmed_positive = row['# Confirmed Positive COVID-19']
            confirmed_positive_ventilator = row['# Confirmed Positive COVID-19 Patients with Invasive Ventilation']
            c = ICUCapacity(date=date, region=region, lhin=lhin, critical_care_beds=critical_care_beds, critical_care_patients=critical_care_patients, vented_beds=vented_beds, vented_patients=vented_patients, suspected_covid=suspected_covid, confirmed_positive=confirmed_positive, confirmed_positive_ventilator=confirmed_positive_ventilator)
            db.session.add(c)
            db.session.commit()

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
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    print('canada case data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
            db.session.commit()
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
    print('canada mortality data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
    print('canada recovered data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
        date = row['date_recovered']
        province = row['province']
        cumulative_recovered = row['cumulative_recovered']
        c = CanadaRecovered.query.filter_by(date=date, province=province).first()
        if not c:
            c = CanadaRecovered(date=date, province=province, cumulative_recovered=cumulative_recovered)

        db.session.add(c)
        db.session.commit()
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
    return

def getcanadatested():
    url = "https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/testing_cumulative.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['date_testing'] = pd.to_datetime(df['date_testing'],dayfirst=True)
    df = df.fillna(-1)
    print('canada testing counts being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
        date = row['date_testing']
        province = row['province']
        cumulative_testing = row['cumulative_testing']
        c = CanadaTesting.query.filter_by(date=date, province=province).first()
        if not c:
            c = CanadaTesting(date=date, province=province, cumulative_testing=cumulative_testing)

        db.session.add(c)
        db.session.commit()
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
    return

def getcanadamobility_google():
    # From global data
    try:
        url = 'https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv'
        s=requests.get(url).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df = df.loc[df.country_region == 'Canada']
        print('google mobility data being refreshed')
        for index, row in df.iterrows():
            if (index % 100) == 0:
                print(f'{index} / {df.tail(1).index.values[0]} passed')
            region = row['country_region']
            subregion = row['sub_region_1']
            date = row['date']
            if region == 'Canada':
                if subregion != '':
                    region = subregion

                def add_transport(date, region, transportation_type, value):
                    if value == '':
                        value = -999
                    if region != region:
                        region = 'Canada'
                    m = MobilityTransportation.query.filter_by(date=date, region=region, transportation_type=transportation_type, source='Google').limit(1).first()
                    if not m:
                        m = MobilityTransportation(date=date, region=region, transportation_type=transportation_type, value=value, source='Google')
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
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Firefox(options=options)
    urlpage = "https://www.apple.com/covid19/mobility"
    driver.implicitly_wait(100)
    driver.get(urlpage)
    button = None
    url = None
    tries = 3
    while url == None and tries > 0:
        tries -= 1
        driver.implicitly_wait(1000)
        try:
            button = driver.find_elements_by_class_name("download-button-container")[0]
            url = button.find_element_by_tag_name('a').get_attribute('href')
        except:
            continue

    if url is None:
        print("Failed to find download button")
        driver.quit()
        return

    regions = ['Ontario', 'Canada', 'Toronto', 'Ottawa']

    s = requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df = df[df['region'].isin(regions)]

    # Get all date columns (i.e. not kind, name, category) and insert record for each
    date_columns = [x for x in list(df.columns) if x not in ['geo_type', 'region', 'transportation_type']]
    print('Apple mobility data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
        region = row['region']
        transport = row['transportation_type']
        for col in date_columns:
            try:
                value = row[col]
                if math.isnan(value):
                    continue
                if region==region:
                    m = MobilityTransportation.query.filter_by(date=col, region=region, transportation_type=transport, source='Apple').limit(1).first()
                    if not m:
                        m = MobilityTransportation(date=col, region=region, transportation_type=transport, value=value, source='Apple')
                        print("Add transport mobility data for region: {}, transport: {}, date: {}, value: {}".format(region, transport, col, value))
                        db.session.add(m)
                        db.session.commit()
            except Exception as err:
                print("failed to get apple data", err)

    driver.quit()
    return

def getgovernmentresponse():
    url = "https://ocgptweb.azurewebsites.net/CSVDownload"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Date'] = pd.to_datetime(df.Date,format="%Y%m%d")


    def parse_val(val):
        if val == -1:
            return sql.null()
        elif val != val:
            return sql.null()
        else:
            return val
    print('international npi data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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

def getlongtermcare():
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Firefox(options=options)
    urlpage = "https://www.ontario.ca/page/how-ontario-is-responding-covid-19"
    driver.implicitly_wait(30)
    driver.get(urlpage)
    tables = driver.find_elements_by_tag_name("table")

    def parseNum(num):
        return int(num.replace('<', ''))

    ltc_mapping = {}
    #https://docs.google.com/spreadsheets/d/1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A/edit?usp=sharing
    #https://www.phdapps.health.gov.on.ca/PHULocator/Results.aspx
    df = pd.read_csv('https://docs.google.com/spreadsheets/d/1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A/export?format=csv&id=1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A&gid=0')
    for index, row in df.iterrows():
        city = row[0]
        phu = row[1]
        ltc_mapping[city] = phu

    try:
        for table in tables:
            headers = [x.text for x in table.find_element_by_tag_name('thead').find_elements_by_tag_name('th')]

            # Isolate table we care about
            # Match first 3 headers we know
            if headers[0] != 'LTC Home' or headers[1] != 'City' or headers[2] != 'Beds':
                continue

            rows = table.find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')

            for row in rows:
                row_values = [x.text for x in row.find_elements_by_tag_name('td')]
                date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                home = row_values[0].replace('""','')
                city = row_values[1]
                beds = parseNum(row_values[2])
                confirmed_resident_cases = parseNum(row_values[3])
                resident_deaths = parseNum(row_values[4])
                confirmed_staff_cases = parseNum(row_values[4])
                phu = ''
                if city in ltc_mapping:
                    phu = ltc_mapping[city]
                l = LongTermCare.query.filter_by(date=date, home=home).first()
                if not l:
                    l = LongTermCare(
                        date=date,
                        home=home,
                        city=city,
                        beds=beds,
                        confirmed_resident_cases=confirmed_resident_cases,
                        resident_deaths=resident_deaths,
                        confirmed_staff_cases=confirmed_staff_cases,
                        phu=phu)
                    db.session.add(l)
            db.session.commit()
            break
    except:
        print('Failed to extract LTC data from ontario.ca')
        values = []

    driver.quit()

def getlongtermcare_summary():
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Firefox(options=options)
    urlpage = "https://www.ontario.ca/page/how-ontario-is-responding-covid-19"
    driver.implicitly_wait(30)
    driver.get(urlpage)
    tables = driver.find_elements_by_tag_name("table")

    def parseNum(num):
        return int(num.replace('<', ''))

    date = None
    # Find the date from the header
    for h3 in driver.find_elements_by_tag_name('h3'):
        if "Summary of long-term care cases" in h3.text:
            # Get date from table header
            pattern = 'Summary of long-term care cases.*to (.*, 20[0-9][0-9]).*'
            match = re.search(pattern, h3.text)
            if match:
                date = match.group(1)
                date = datetime.strptime(date, '%B %d, %Y')
                date = datetime.strftime(date, '%Y-%m-%d')

    if date is None:
        driver.quit()
        raise "Could not find date for LTC summary table"

    try:
        for table in tables:
            headers = [x.text for x in table.find_element_by_tag_name('thead').find_elements_by_tag_name('th')]

            # Isolate table we care about
            # Match first 3 headers we know
            if headers[0] != 'Report' or headers[1] != 'Number' or headers[2] != 'Previous Day Number':
                continue

            rows = table.find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')

            for row in rows:
                report = row.find_element_by_tag_name('th').text.replace('""','')
                row_values = [x.text for x in row.find_elements_by_tag_name('td')]
                number = parseNum(row_values[0])
                #print('Date', date, 'Report', report, 'Cases', number)
                l = LongTermCareSummary.query.filter_by(date=date, report=report).first()
                if not l:
                    l = LongTermCareSummary(
                        date=date,
                        report=report,
                        number=number)
                    db.session.add(l)
            db.session.commit()
            break
    except Exception as e:
        driver.quit()
        raise Exception('Failed to extract LTC summary data from ontario.ca: {}'.format(str(e)))

    driver.quit()


def getlongtermcare_nolongerinoutbreak():
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Firefox(options=options)
    urlpage = "https://www.ontario.ca/page/how-ontario-is-responding-covid-19"
    driver.implicitly_wait(30)
    driver.get(urlpage)
    tables = driver.find_elements_by_tag_name("table")

    def parseNum(num):
        return int(num.replace('<', ''))

    ltc_mapping = {}
    #https://docs.google.com/spreadsheets/d/1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A/edit?usp=sharing
    #https://www.phdapps.health.gov.on.ca/PHULocator/Results.aspx
    for row in sheetsHelper.readSheet('HowsMyFlattening - Mappings', 'CityToPHU'):
        city = row[0]
        phu = row[1]
        ltc_mapping[city] = phu

    try:
        for table in tables:
            headers = [x.text for x in table.find_element_by_tag_name('thead').find_elements_by_tag_name('th')]

            # Isolate table we care about
            # Match first 3 headers we know
            if headers[0] != 'LTC Home' or headers[1] != 'City' or headers[2] != 'Beds':
                continue

            rows = table.find_element_by_tag_name('tbody').find_elements_by_tag_name('tr')

            for row in rows:
                row_values = [x.text for x in row.find_elements_by_tag_name('td')]
                date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
                home = row.find_element_by_tag_name('th').text.replace('""','')
                city = row_values[0]
                beds = parseNum(row_values[1])
                resident_deaths = parseNum(row_values[2])
                phu = ''
                if city in ltc_mapping:
                    phu = ltc_mapping[city]
                #print('Date', date, 'Home', home, 'City', city, 'Beds', beds, 'Resident deaths', resident_deaths, 'PHU', phu)
                l = LongTermCareNoLongerInOutbreak.query.filter_by(date=date, home=home).first()
                if not l:
                    l = LongTermCareNoLongerInOutbreak(
                        date=date,
                        home=home,
                        city=city,
                        beds=beds,
                        resident_deaths=resident_deaths,
                        phu=phu)
                    db.session.add(l)
            db.session.commit()
            break
    except Exception as e:
        driver.quit()
        raise Exception('Failed to extract LTC summary data from ontario.ca: {}'.format(str(e)))

    driver.quit()

def getpredictivemodel():
    sources = interactiveScraper.get_permitted_sources()
    #['base_on', 'base_sk', 'base_on', 'base_italy', 'expanded_sk', 'expanded_on_expected', 'expanded_italy', 'base_on_n', 'base_on_e', 'base_on_w', 'base_on_c', 'base_toronto']

    def parseInt(val):
        try:
            val = int(val)
            if val == -1 or val != val:
                raise
        except:
            return sql.null()
        return val

    for source in sources:
        print("Getting predictive model data from source {}".format(source))
        try:
            df = asyncio.get_event_loop().run_until_complete(interactiveScraper.test(source))[1:]

            # One problem with this data is it assumes dates relative to 07/03/2020, not sure if this will change
            start_date = datetime(2020, 3, 7)
            for index, row in df.iterrows():
                if len(row) == 0 or row['date'] == None:
                    continue

                date_retrieved = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                region = source
                date = start_date + timedelta(days=parseInt(row['date']))
                cumulative_incidence = parseInt(row['cum_incidence'])
                required_hospW = parseInt(row['required_hospW'])
                required_hospNonVentICU = parseInt(row['required_hospNonVentICU'])
                required_hospVentICU = parseInt(row['required_hospVentICU'])
                available_hospW = parseInt(row['available_hospW'])
                available_hospNonVentICU = parseInt(row['available_hospNonVentICU'])
                available_hospVentICU = parseInt(row['available_hospVentICU'])
                waiting_hospW = parseInt(row['waiting_hospW'])
                waiting_hospNonVentICU = parseInt(row['waiting_hospNonVentICU'])
                waiting_hospVentICU = parseInt(row['waiting_hospVentICU'])

                p = PredictiveModel.query.filter_by(date=date, region=region).first()
                if not p:
                    p = PredictiveModel(
                            date_retrieved=date_retrieved,
                            region=region,
                            date=date,
                            cumulative_incidence=cumulative_incidence,
                            required_hospW=required_hospW,
                            required_hospNonVentICU=required_hospNonVentICU,
                            required_hospVentICU=required_hospVentICU,
                            available_hospW=available_hospW,
                            available_hospNonVentICU=available_hospNonVentICU,
                            available_hospVentICU=available_hospVentICU,
                            waiting_hospW=waiting_hospW,
                            waiting_hospNonVentICU=waiting_hospNonVentICU,
                            waiting_hospVentICU=waiting_hospVentICU)
                    db.session.add(p)
            db.session.commit()
        except:
            print('Failed to get predictive model for source {}'.format(source), sys.exc_info())

def getideamodel():
    sources = ideaScraper.get_permitted_sources()
    #['on', 'health_unit']

    def parseInt(val):
        try:
            val = int(val)
            if val == -1 or val != val:
                raise
        except:
            return sql.null()
        return val

    def parseFloat(val):
        try:
            val = float(val)
            if val == -1 or val != val:
                raise
        except:
            return sql.null()
        return val

    for source in sources:
        print("Getting IDEA model data from source {}".format(source))
        try:
            df = asyncio.get_event_loop().run_until_complete(ideaScraper.test(source))[1:]
            df = df.replace("NA", -1)
            # One problem with this data is it assumes dates relative to 07/03/2020, not sure if this will change
            for index, row in df.iterrows():
                if len(row) == 0 or row['Date'] is None:
                    continue

                date_retrieved = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                source = source
                date = datetime.strptime(row['Date'],"%Y-%m-%d")
                reported_cases = parseInt(row['Reported cases'])
                model_incident_cases = parseFloat(row['Model incident cases'])
                model_incident_cases_lower_PI = parseFloat(row['Model incident cases lower PI'])
                model_incident_cases_upper_PI = parseFloat(row['Model incident cases upper PI'])
                reported_cumulative_cases = parseInt(row['Reported cumulative cases'])
                model_cumulative_cases = parseFloat(row['Model cumulative cases'])
                model_cumulative_cases_lower_PI = parseFloat(row['Model cumulative cases lower PI'])
                model_cumulative_cases_upper_PI = parseFloat(row['Model cumulative cases upper PI'])

                p = IDEAModel.query.filter_by(date=date, source=source).first()
                if not p:
                    p = IDEAModel(
                            date_retrieved=date_retrieved,
                            source=source,
                            date=date,
                            reported_cases=reported_cases,
                            model_incident_cases=model_incident_cases,
                            model_incident_cases_lower_PI=model_incident_cases_lower_PI,
                            model_incident_cases_upper_PI=model_incident_cases_upper_PI,
                            reported_cumulative_cases=reported_cumulative_cases,
                            model_cumulative_cases=model_cumulative_cases,
                            model_cumulative_cases_lower_PI=model_incident_cases_lower_PI,
                            model_cumulative_cases_upper_PI=model_cumulative_cases_upper_PI)
                    db.session.add(p)
            db.session.commit()
        except:
            print('Failed to get IDEA model for source {}'.format(source), sys.exc_info())


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
    print('international case data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
    print('international mortality data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
    print('international recovered data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
    print('international testing data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
    print('npi us data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
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
        text_top = row['text_top']
        text_bottom = row['text_bottom']
        mobileHeight = row['mobileHeight']
        desktopHeight = row['desktopHeight']
        page = row['page']
        order = row['order']
        row_z = row['row']
        column = row['column']
        phu = row['phu']
        tab_order = row['tab_order']
        viz_type = row['viz_type']
        viz_title = row['title']
        date = row['date']
        visible = row['visible']

        c = Viz.query.filter_by(header=header, phu=phu).first()
        if not c:
            c = Viz(header=header, category=category, content=content,
            viz=viz, thumbnail=thumbnail, mobileHeight=mobileHeight,
            text_top=text_top, text_bottom=text_bottom,
            desktopHeight=desktopHeight, page=page, order=order, row=row_z,
            column=column, phu=phu, tab_order=tab_order,viz_type=viz_type,
            viz_title=viz_title, visible=visible)
            if page == 'Analysis':
                c.date = date
            db.session.add(c)
            db.session.commit()
        else:
            c.category = category
            c.content = content
            if viz_type == 'Tableau':
                c.viz = viz
            if page == 'Analysis':
                c.date = date
            c.mobileHeight = mobileHeight
            c.desktopHeight = desktopHeight
            c.thumbnail = thumbnail
            c.text_bottom=text_bottom
            c.text_top = text_top
            c.page=page
            c.order = order
            c.row = row_z
            c.column = column
            c.tab_order = tab_order
            c.viz_type = viz_type
            c.viz_title = viz_title
            c.visible = visible
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
        region = row['Region']
        type = row['Type']
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
            c = Source(region=region, type=type, name=name, source=source, description=description,
            data_feed_type=data_feed_type, link=link, refresh=refresh,
            contributor=contributor, contact=contact, download=download)
            db.session.add(c)
            db.session.commit()
        else:
            c.region = region
            c.type = type
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

@bp.route('/covid/team', methods=['GET'])
@as_json
def new_team():
    url = "https://docs.google.com/spreadsheets/d/1Sq_KtGI1v4ABLVnJUVRGPi-hOjupSpThL9dNeeVVis0/export?format=csv&id=1Sq_KtGI1v4ABLVnJUVRGPi-hOjupSpThL9dNeeVVis0&gid=0"
    s=requests.get(url).content
    data = io.StringIO(s.decode('utf-8'))
    df = pd.read_csv(data)
    for index, row in df.iterrows():
        team = row['Team']
        title = row['Title']
        first_name = row['First Name']
        last_name = row['Last Name']
        education = row['Highest Ed.']
        affiliation = row['Affiliation']
        role = row['Role (Maintainers Only)']
        team_status = row['Team Status']
        if first_name == first_name and first_name != '':
            c = Member.query.filter_by(first_name=first_name, last_name=last_name).first()
            if not c:
                c = Member(team=team, title=title, first_name=first_name,
                last_name=last_name, education=education, affiliation=affiliation,
                role=role, team_status=team_status)
                db.session.add(c)
                db.session.commit()
            else:
                c.team = team
                c.title = title
                c.education = education
                c.affiliation = affiliation
                c.role = role
                c.team_status = team_status
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
