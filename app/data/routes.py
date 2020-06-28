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
    driver = webdriver.PhantomJS(service_log_path=os.path.devnull)
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

    field_map = ["start_date","end_date","country","region","subregion",
    "intervention_summary","intervention_category","target_population_category",
    "enforcement_category","oxford_government_response_category",
    "oxford_closure_code","oxford_public_info_code","oxford_travel_code",
    "oxford_geographic_target_code","oxford_fiscal_measure_cad",
    "oxford_testing_code","oxford_tracing_code","oxford_restriction_code",
    "oxford_income_amount","oxford_income_target","oxford_debt_relief_code",
    "source_url","source_organization","source_organization_2","source_category",
    "source_title","source_full_text","note","end_source_url",
    "end_source_organization","end_source_organization_2","end_source_category",
    "end_source_title","end_source_full_text"]

    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['start_date'] = pd.to_datetime(df['start_date'])
    df['end_date'] = pd.to_datetime(df['end_date'])
    df = df.fillna(sql.null())
    df.dropna(subset=['start_date'],inplace=True)

    print('npi canada data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
        n = NPIInterventions.query.filter_by(start_date=row["start_date"], region=row["region"], intervention_summary=row["intervention_summary"]).first()
        if n:
            for header in field_map:
                setattr(n,header,row[header])
            db.session.add(n)
            db.session.commit()
        else:
            c = NPIInterventions()
            for header in field_map:
                setattr(c,header,row[header])
            db.session.add(c)
            db.session.commit()
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
    cases = {case.case_id:case for case in Covid.query.all()}
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    to_add = []
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
        if case_id not in cases:
            c = Covid(case_id=case_id, age=age, sex=sex, region=region, province=province, country=country, date=date, travel=travel, travelh=travelh)
            to_add.append(c)
        else:
            c = cases.get(case_id)
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
                to_add.append(c)

    print("to add {} records".format(len(to_add)))
    db.session.add_all(to_add)
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
    driver = webdriver.PhantomJS(service_log_path=os.path.devnull)
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
    field_map = {
        "CountryName": "country",
        "CountryCode": "country_code",
        "Date": "date",
        "C1_School closing":"c1_school_closing",
        "C1_Flag":"c1_flag",
        "C2_Workplace closing":"c2_workplace_closing",
        "C2_Flag":"c2_flag",
        "C3_Cancel public events":"c3_cancel_public_events",
        "C3_Flag":"c3_flag",
        "C4_Restrictions on gatherings":"c4_restrictions_on_gatherings",
        "C4_Flag":"c4_flag",
        "C5_Close public transport":"c5_close_public_transport",
        "C5_Flag":"c5_flag",
        "C6_Stay at home requirements":"c6_stay_at_home_requirements",
        "C6_Flag":"c6_flag",
        "C7_Restrictions on internal movement":"c7_restrictions_on_internal_movement",
        "C7_Flag":"c7_flag",
        "C8_International travel controls":"c8_international_travel_controls",
        "E1_Income support":"e1_income_support",
        "E1_Flag":"e1_flag",
        "E2_Debt/contract relief":"e2_debt_contract_relief",
        "E3_Fiscal measures":"e3_fiscal_measures",
        "E4_International support":"e4_international_support",
        "H1_Public information campaigns":"h1_public_information_campaigns",
        "H1_Flag":"h1_flag",
        "H2_Testing policy":"h2_testing_policy",
        "H3_Contact tracing":"h3_contact_tracing",
        "H4_Emergency investment in healthcare":"h4_emergency_investment_in_healthcare",
        "H5_Investment in vaccines":"h5_investment_in_vaccines",
        "M1_Wildcard":"m1_wildcard",
        "ConfirmedCases":"confirmed_cases",
        "ConfirmedDeaths":"confirmed_deaths",
        "StringencyIndex":"stringency_index",
        "StringencyIndexForDisplay":"stringency_index_for_display",
        "StringencyLegacyIndex":"stringency_legacy_index",
        "StringencyLegacyIndexForDisplay":"stringency_legacy_index_for_display",
        "GovernmentResponseIndex":"government_response_index",
        "GovernmentResponseIndexForDisplay":"government_response_index_for_display",
        "ContainmentHealthIndex":"containment_health_index",
        "ContainmentHealthIndexForDisplay":"containment_health_index_for_display",
        "EconomicSupportIndex":"economic_support_index",
        "EconomicSupportIndexForDisplay":"economic_support_index_for_display"
    }

    url = "https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"
    s=requests.get(url).content
    df = pd.read_csv(io.StringIO(s.decode('utf-8')))
    df['Date'] = pd.to_datetime(df.Date,format="%Y%m%d")
    df = df.fillna(sql.null())
    gov_responses = {(gr.date,gr.country):gr for gr in GovernmentResponse.query.all()}
    to_add = []

    print('international npi data being refreshed')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
        if (row["Date"],row["CountryName"]) not in gov_responses:
            g = GovernmentResponse()
            for header in field_map.keys():
                setattr(g,field_map[header],row[header])
            to_add.append(g)

    print("to add {} records".format(len(to_add)))
    db.session.add_all(to_add)
    db.session.commit()
    return

def getlongtermcare():
    driver = webdriver.PhantomJS(service_log_path=os.path.devnull)
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
    driver = webdriver.PhantomJS(service_log_path=os.path.devnull)
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
    driver = webdriver.PhantomJS(service_log_path=os.path.devnull)
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

def getjobsdata():
    print('jobs data being refreshed')
    df = pd.read_csv('industry_weekly_2020.csv')
    for index, row in df.iterrows():
        if (index % 100) == 0:
            print(f'{index} / {df.tail(1).index.values[0]} passed')
        w = WeeklyJobPosting.query.filter_by(country=row['country'], geography=row['geography'], group_name=row['group_name'], start_date=row['start_date']).first()
        if not w:
            w = WeeklyJobPosting()
            for header in df.columns:
                setattr(w,header,row[header])
            db.session.add(w)
            db.session.commit()

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
        linkedin =  row['LinkedIn URL']
        if first_name == first_name and first_name != '':
            c = Member.query.filter_by(first_name=first_name, last_name=last_name).first()
            if not c:
                c = Member(team=team, title=title, first_name=first_name,
                last_name=last_name, education=education, affiliation=affiliation,
                role=role, team_status=team_status,linkedin=linkedin)
                db.session.add(c)
                db.session.commit()
            else:
                c.team = team
                c.title = title
                c.education = education
                c.affiliation = affiliation
                c.role = role
                c.team_status = team_status
                c.linkedin = linkedin
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
