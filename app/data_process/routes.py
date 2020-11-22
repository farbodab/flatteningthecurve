from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app.data_process import bp
from datetime import datetime
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import glob
import os

def get_file_path(data, step='raw', today=datetime.today().strftime('%Y-%m-%d')):
    source_dir = 'data/' + data['classification'] + '/' + step + '/'
    if data['type'] != '':
        file_name = data['table_name'] + '_' + today + '.' + data['type']
    else:
        file_name = data['table_name'] + '_' + today
    save_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_path =  save_dir + '/' + file_name
    return file_path, save_dir

@bp.cli.command('public_ontario_gov_daily_change_in_cases_by_phu')
def process_public_ontario_gov_daily_change_in_cases_by_phu():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'daily_change_in_cases_by_phu',  'type': 'csv'}
    date_field = ['Date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {file}")
                print(e)
                return e

            df = df.melt(id_vars='Date')
            replace = {
            'Algoma_Public_Health_Unit':3526,
            'Brant_County_Health_Unit':3527,
            'Chatham-Kent_Health_Unit':3540,
            'Durham_Region_Health_Department':3530,
            'Eastern_Ontario_Health_Unit':3558,
            'Grey_Bruce_Health_Unit':3533,
            'Haldimand-Norfolk_Health_Unit':3534,
            'Haliburton,_Kawartha,_Pine_Ridge_District_Health_Unit':3535,
            'Halton_Region_Health_Department':3536,
            'Hamilton_Public_Health_Services':3537,
            'Hastings_and_Prince_Edward_Counties_Health_Unit':3538,
            'Huron_Perth_District_Health_Unit':3539,
            'Kingston,_Frontenac_and_Lennox_&_Addington_Public_Health':3541,
            'Lambton_Public_Health':3542,
            'Leeds,_Grenville_and_Lanark_District_Health_Unit':3543,
            'Middlesex-London_Health_Unit':3544,
            'Niagara_Region_Public_Health_Department':3546,
            'North_Bay_Parry_Sound_District_Health_Unit':3547,
            'Northwestern_Health_Unit':3549,
            'Ottawa_Public_Health':3551,
            'Peel_Public_Health':3553,
            'Peterborough_Public_Health':3555,
            'Porcupine_Health_Unit':3556,
            'Region_of_Waterloo,_Public_Health':3565,
            'Renfrew_County_and_District_Health_Unit':3557,
            'Simcoe_Muskoka_District_Health_Unit':3560,
            'Southwestern_Public_Health':3575,
            'Sudbury_&_District_Health_Unit':3561,
            'Thunder_Bay_District_Health_Unit':3562,
            'Timiskaming_Health_Unit':3563,
            'Toronto_Public_Health':3595,
            'Wellington-Dufferin-Guelph_Public_Health':3566,
            'Windsor-Essex_County_Health_Unit':3568,
            'York_Region_Public_Health_Services':3570,
            'Total':6
        }
            df['HR_UID'] = df['variable'].replace(replace)
            for column in date_field:
                df[column] = pd.to_datetime(df[column], errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)



@bp.cli.command('public_ontario_gov_conposcovidloc')
def process_public_ontario_gov_conposcovidloc():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'conposcovidloc',  'type': 'csv'}
    field_map = {
        "Row_ID":"row_id",
        "Accurate_Episode_Date": "accurate_episode_date",
        "Case_Reported_Date": "case_reported_date",
        "Specimen_Date": "specimen_reported_date",
        "Test_Reported_Date": "test_reported_date",
        "Age_Group":"age_group",
        "Client_Gender":"client_gender",
        "Case_AcquisitionInfo": "case_acquisition_info",
        "Outcome1": "outcome_1",
        "Outbreak_Related": "outbreak_related",
        "Reporting_PHU": "reporting_phu",
        "Reporting_PHU_Address": "reporting_phu_address",
        "Reporting_PHU_City": "reporting_phu_city",
        "Reporting_PHU_Postal_Code": "reporting_phu_postal_code",
        "Reporting_PHU_Website": "reporting_phu_website",
        "Reporting_PHU_Latitude":"reporting_phu_latitude",
        "Reporting_PHU_Longitude": "reporting_phu_longitude",
    }
    date_field = ['accurate_episode_date', 'case_reported_date', 'specimen_reported_date', 'test_reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {file}")
                print(e)
                return e

            df = df.replace("12:00:00 AM", None)
            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column], errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_ontario_gov_covidtesting')
def process_public_ontario_gov_covidtesting():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'covidtesting',  'type': 'csv'}
    field_map = {
        "Reported Date":"reported_date",
        "Confirmed Negative":"confirmed_negative",
        "Presumptive Negative":"presumptive_negative",
        "Presumptive Positive":"presumptive_positive",
        "Confirmed Positive":"confirmed_positive",
        "Resolved":"resolved",
        "Deaths":"deaths",
        "Total Cases":"total_cases",
        "Total patients approved for testing as of Reporting Date":"total_patients_approved_for_testing_as_of_reporting_date",
        "Total tests completed in the last day":"total_tests_completed_in_last_day",
        "Under Investigation":"under_investigation",
        "Number of patients hospitalized with COVID-19":"number_of_patients_hospitalized_with_covid19",
        "Number of patients in ICU with COVID-19":"number_of_patients_in_icu_with_covid19",
        "Number of patients in ICU on a ventilator with COVID-19":"number_of_patients_in_icu_on_a_ventilator_with_covid19",
        "Total Positive LTC Resident Cases":"total_positive_ltc_resident_cases",
        "Total Positive LTC HCW Cases":"total_positive_ltc_hcw_cases",
        "Total LTC Resident Deaths":"total_ltc_resident_deaths",
        "Total LTC HCW Deaths":"total_ltc_hcw_deaths"
    }
    date_field = ['reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {file}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column])
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_ontario_gov_longtermcare_in_outbreak')
def process_public_ontario_gov_longtermcare_in_outbreak():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'website',  'type': 'html'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        data_out = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'longtermcare_in_outbreak',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'processed', date)
        if not os.path.isfile(file):
            f = open(load_file, "r")
            contents = f.read()
            soup = BeautifulSoup(contents, 'html.parser')
            tables = soup.find_all("table")
            def parseNum(num):
                return int(num.replace('<', ''))
            ltc_mapping = {}
            df = pd.read_csv('https://docs.google.com/spreadsheets/d/1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A/export?format=csv&id=1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A&gid=0')
            for index, row in df.iterrows():
                city = row[0]
                phu = row[1]
                ltc_mapping[city] = phu

            data = []
            for table in tables:
                if not table.find_all('th'):
                    continue
                headers = [x.text for x in table.find('thead').find_all('th')]

                # Isolate table we care about
                # Match first 3 headers we know
                if headers[0] != 'LTC Home' or headers[1] != 'City' or headers[2] != 'Beds':
                    continue

                rows = table.find('tbody').find_all('tr')

                for row in rows:
                    home = row.find('th').text
                    row_values = [x.text for x in row.find_all('td')]
                    if len(row_values) != 5:
                        continue
                    city = row_values[0].replace('""','')
                    beds = row_values[1]
                    confirmed_resident_cases = parseNum(row_values[2])
                    resident_deaths = parseNum(row_values[3])
                    confirmed_staff_cases = parseNum(row_values[4])
                    phu = ''
                    if city in ltc_mapping:
                        phu = ltc_mapping[city]
                    data.append({"home":home, "city":city, "beds":beds, "confirmed_resident_cases":confirmed_resident_cases, "resident_deaths":resident_deaths, "confirmed_staff_cases": confirmed_staff_cases, "phu":phu})

            df = pd.DataFrame(data)
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_ontario_gov_longtermcare_summary')
def process_public_ontario_gov_longtermcare_summary():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'website',  'type': 'html'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        data_out = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'longtermcare_summary',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'processed', date)
        if not os.path.isfile(save_file):
            f = open(file, "r")
            contents = f.read()
            soup = BeautifulSoup(contents, 'html.parser')
            tables = soup.find_all("table")

            def parseNum(num):
                num = num.replace(',','')
                return int(num.replace('<', ''))

            data = []
            for table in tables:
                headers = [x.text for x in table.find('thead').find_all('th')]

                # Isolate table we care about
                # Match first 3 headers we know
                if headers[0] != 'Report' or headers[1] != 'Number' or headers[2] != 'Previous Day Number':
                    continue

                rows = table.find('tbody').find_all('tr')

                for row in rows:
                    report = row.find('th').text.replace('""','')
                    row_values = [x.text for x in row.find_all('td')]
                    number = parseNum(row_values[0])
                    data.append({"report":report, "number":number})

            df = pd.DataFrame(data)

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_ontario_gov_longtermcare_no_longer_in_outbreak')
def process_public_ontario_gov_longtermcare_no_longer_in_outbreak():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'website',  'type': 'html'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        data_out = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'longtermcare_no_longer_in_outbreak',  'type': 'csv'}
        save_file, save_dir = get_file_path(data_out, 'processed', date)
        if not os.path.isfile(save_file):
            f = open(file, "r")
            contents = f.read()
            soup = BeautifulSoup(contents, 'html.parser')
            tables = soup.find_all("table")

            def parseNum(num):
                return int(num.replace('<', ''))

            ltc_mapping = {}
            df = pd.read_csv('https://docs.google.com/spreadsheets/d/1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A/export?format=csv&id=1Pvj5_Y288_lmX_YsOm82gYkJw7oN-tPTz70FwdUUU5A&gid=0')
            for index, row in df.iterrows():
                city = row[0]
                phu = row[1]
                ltc_mapping[city] = phu

            data = []
            for table in tables:
                headers = [x.text for x in table.find('thead').find_all('th')]

                # Isolate table we care about
                # Match first 3 headers we know
                if headers[0] != 'LTC Home' or headers[1] != 'City' or headers[2] != 'Beds':
                    continue

                rows = table.find('tbody').find_all('tr')

                for row in rows:
                    row_values = [x.text for x in row.find_all('td')]
                    home = row.find('th').text.replace('""','')
                    city = row_values[0]
                    beds = parseNum(row_values[1])
                    resident_deaths = parseNum(row_values[2])
                    phu = ''
                    if city in ltc_mapping:
                        phu = ltc_mapping[city]
                    #print('Date', date, 'Home', home, 'City', city, 'Beds', beds, 'Resident deaths', resident_deaths, 'PHU', phu)
                    data.append({"home":home, "city":city, "beds":beds, "resident_deaths": resident_deaths, "phu":phu})

            df = pd.DataFrame(data)
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_howsmyflattening_npi_canada')
def process_public_howsmyflattening_npi_canada():
    data = {'classification':'public', 'source_name':'howsmyflattening', 'table_name':'npi_canada',  'type': 'csv'}
    field_map = {
        "Unnamed: 0": "index",
        "start_date":"start_date",
        "end_date":"end_date",
        "country":"country",
        "region":"region",
        "subregion":"subregion",
        "intervention_summary":"intervention_summary",
        "intervention_category":"intervention_category",
        "target_population_category":"target_population_category",
        "enforcement_category":"enforcement_category",
        "oxford_government_response_category":"oxford_government_response_category",
        "oxford_closure_code":"oxford_closure_code",
        "oxford_public_info_code":"oxford_public_info_code",
        "oxford_travel_code":"oxford_travel_code",
        "oxford_geographic_target_code":"oxford_geographic_target_code",
        "oxford_fiscal_measure_cad":"oxford_fiscal_measure_cad",
        "oxford_testing_code":"oxford_testing_code",
        "oxford_tracing_code":"oxford_tracing_code",
        "oxford_restriction_code":"oxford_restriction_code",
        "oxford_income_amount":"oxford_income_amount",
        "oxford_income_target":"oxford_income_target",
        "oxford_debt_relief_code":"oxford_debt_relief_code",
        "source_url":"source_url",
        "source_organization":"source_organization",
        "source_organization_2":"source_organization_2",
        "source_category":"source_category",
        "source_title":"source_title",
        "source_full_text":"source_full_text",
        "note":"note",
        "end_source_url":"end_source_url",
        "end_source_organization":"end_source_organization",
        "end_source_organization_2":"end_source_organization_2",
        "end_source_category":"end_source_category",
        "end_source_title":"end_source_title",
        "end_source_full_text":"end_source_full_text",
    }
    date_field = ['start_date','end_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file, engine='python',encoding='utf8')
            except Exception as e:
                print(f"{load_file}")
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df.dropna(how='any',subset=["start_date", "country", "intervention_summary","enforcement_category"])
            for column in date_field:
                df[column] = pd.to_datetime(df[column])

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_open_data_working_group_cases')
def process_public_open_data_working_group_cases():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'cases',  'type': 'csv'}
    field_map = {
    "case_id":"case_id",
    "provincial_case_id":"provincial_case_id",
    "age":"age",
    "sex":"sex",
    "health_region":"health_region",
    "province":"province",
    "country":"country",
    "date_report":"date_report",
    "report_week":"report_week",
    "travel_yn":"travel_yn",
    "travel_history_country":"travel_history_country",
    "locally_acquired":	"locally_acquired",
    "case_source":"case_source",
    "additional_info":"additional_info",
    "additional_source":"additional_source",
    "method_note":"method_note",
    }
    date_field = ['date_report','report_week']
    replace = {"Algoma":"The District of Algoma Health Unit", "Brant":"Brant County Health Unit", "Chatham-Kent":"Chatham-Kent Health Unit", "Durham":"Durham Regional Health Unit",
    "Eastern":"The Eastern Ontario Health Unit", "Grey Bruce":"Grey Bruce Health Unit", "Haliburton Kawartha Pineridge":"Haliburton, Kawartha, Pine Ridge District Health Unit",
     "Halton":"Halton Regional Health Unit", "Hamilton":"City of Hamilton Health Unit",  "Hastings Prince Edward":"Hastings and Prince Edward Counties Health Unit",
     "Huron Perth":"Huron Perth Public Health Unit", "Kingston Frontenac Lennox & Addington":"Kingston, Frontenac, and Lennox and Addington Health Unit",
      "Lambton":"Lambton Health Unit", "Middlesex-London":"Middlesex-London Health Unit", "Niagara":"Niagara Regional Area Health Unit",
      "North Bay Parry Sound":"North Bay Parry Sound District Health Unit", "Northwestern":"Northwestern Health Unit", "Ottawa":"City of Ottawa Health Unit",
      "Peel":"Peel Regional Health Unit", "Peterborough":"Peterborough County-City Health Unit", "Porcupine":"Porcupine Health Unit",  "Simcoe Muskoka":"Simcoe Muskoka District Health Unit",
      "Sudbury": "Sudbury and District Health Unit", "Timiskaming":"Timiskaming Health Unit", "Toronto":"City of Toronto Health Unit", "Waterloo":"Waterloo Health Unit",
      "Wellington Dufferin Guelph":"Wellington-Dufferin-Guelph Health Unit", "Windsor-Essex":"Windsor-Essex County Health Unit",  "York":"York Regional Health Unit",
      "Haldimand-Norfolk": "Haldimand-Norfolk Health Unit", "Leeds Grenville and Lanark": "Leeds, Grenville and Lanark District Health Unit", "Renfrew": "Renfrew County and District Health Unit",
      "Thunder Bay": "Thunder Bay District Health Unit", "Southwestern":"Southwestern Public Health Unit"}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce', dayfirst=True)
            df.health_region = df.health_region.replace(replace)
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_open_data_working_group_mortality')
def process_public_open_data_working_group_mortality():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'mortality',  'type': 'csv'}
    field_map = {
    "death_id":"death_id",
    "province_death_id":"provincial_death_id",
    "case_id":"case_id",
    "age":"age",
    "sex":"sex",
    "health_region":"health_region",
    "province":"province",
    "country":"country",
    "date_death_report":"date_death_report",
    "death_source":"death_source",
    "additional_info":"additional_info",
    "additional_source":"additional_source",
    }

    date_field = ['date_death_report']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df.replace({"NA":None})

            for column in date_field:
                df[column] = pd.to_datetime(df[column], format="%d-%m-%Y")

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_open_data_working_recovered_cumulative')
def process_public_open_data_working_recovered_cumulative():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'recovered_cumulative',  'type': 'csv'}
    field_map = {
    "date_recovered":"date_recovered",
    "province":"province",
    "cumulative_recovered":"cumulative_recovered"
    }

    date_field = ['date_recovered']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df.replace({"NA":None})

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce', format="%d-%m-%Y")

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_open_data_working_testing_cumulative')
def process_public_open_data_working_testing_cumulative():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'testing_cumulative',  'type': 'csv'}
    field_map = {
    "date_testing":"date_testing",
    "province":"province",
    "cumulative_testing":"cumulative_testing",
    "testing_info": "testing_info"
    }

    date_field = ['date_testing']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df.replace({"NA":None})

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce', format="%d-%m-%Y")

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_google_global_mobility_report')
def process_public_google_global_mobility_report():
    data = {'classification':'public', 'source_name':'google', 'table_name':'global_mobility_report',  'type': 'csv'}
    field_map = {
        "country_region_code":"country_region_code",
        "country_region":"country_region",
        "sub_region_1":"sub_region_1",
        "sub_region_2":"sub_region_2",
        "iso_3166_2_code":"iso_3166_2_code",
        "census_fips_code":"census_fips_code",
        "date":"date",
        "retail_and_recreation_percent_change_from_baseline":"retail_and_recreation_percent_change_from_baseline",
        "grocery_and_pharmacy_percent_change_from_baseline":"grocery_and_pharmacy_percent_change_from_baseline",
        "parks_percent_change_from_baseline":"parks_percent_change_from_baseline",
        "transit_stations_percent_change_from_baseline":"transit_stations_percent_change_from_baseline",
        "workplaces_percent_change_from_baseline":"workplaces_percent_change_from_baseline",
        "residential_percent_change_from_baseline":"residential_percent_change_from_baseline",
    }

    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_apple_applemobilitytrends')
def process_public_apple_applemobilitytrends():
    data = {'classification':'public', 'source_name':'apple', 'table_name':'applemobilitytrends',  'type': 'csv'}
    field_map = {
        "geo_type":"geo_type",
        "region":"region",
        "transportation_type": "transportation_type",
        "alternative_name": "alternative_name",
        "sub-region": "sub_region",
        "country":"country",
    }

    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = pd.melt(df, id_vars=['geo_type', 'region', 'transportation_type', 'alternative_name', 'sub_region', 'country'])

            df = df.rename(columns={"variable":"date"})

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')
            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_oxcgrt_oxcgrt_latest')
def process_public_oxcgrt_oxcgrt_latest():
    data = {'classification':'public', 'source_name':'oxcgrt', 'table_name':'oxcgrt_latest',  'type': 'csv'}
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

    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce',format="%Y%m%d")

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_jhu_time_series_covid19_confirmed_global')
def process_public_jhu_time_series_covid19_confirmed_global():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_confirmed_global',  'type': 'csv'}
    field_map = {
        "Province/State":"province/state",
        "Country/Region":"country/region",
        "Lat":"lat",
        "Long":"long",
    }

    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = pd.melt(df, id_vars=['province/state', 'country/region', 'lat', 'long'])
            df = df.rename(columns={"variable":"date"})

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_jhu_time_series_covid19_deaths_global')
def process_public_jhu_time_series_covid19_deaths_global():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_deaths_global',  'type': 'csv'}
    field_map = {
        "Province/State":"province/state",
        "Country/Region":"country/region",
        "Lat":"lat",
        "Long":"long",
    }

    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = pd.melt(df, id_vars=['province/state', 'country/region', 'lat', 'long'])
            df = df.rename(columns={"variable":"date"})

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_jhu_time_series_covid19_recovered_global')
def process_public_jhu_time_series_covid19_recovered_global():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_recovered_global',  'type': 'csv'}
    field_map = {
        "Province/State":"province/state",
        "Country/Region":"country/region",
        "Lat":"lat",
        "Long":"long",
    }

    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = pd.melt(df, id_vars=['province/state', 'country/region', 'lat', 'long'])
            df = df.rename(columns={"variable":"date"})

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_owid_covid_testing_all_observations')
def process_public_owid_covid_testing_all_observations():
    data = {'classification':'public', 'source_name':'owid', 'table_name':'covid_testing_all_observations',  'type': 'csv'}
    field_map = {
        "Entity":"entity",
        "ISO code":"iso_code",
        "Date"	:"date",
        "Source URL"	:"source_url",
        "Source label"	:"source_label",
        "Notes"	:"notes",
        "Cumulative total"	:"cumulative_total",
        "Daily change in cumulative total"	:"daily_change_in_cumulative_total",
        "Cumulative total per thousand"	:"cumulative_total_per_thousand",
        "Daily change in cumulative total per thousand"	:"daily_change_in_cumulative_total_per_thousand",
        "7-day smoothed daily change"	:"7_day_smoothed_daily_change",
        "7-day smoothed daily change per thousand":"7_day_smoothed_daily_change_per_thousand",
    }

    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_keystone_strategy_complete_npis_inherited_policies')
def process_public_keystone_strategy_complete_npis_inherited_policies():
    data = {'classification':'public', 'source_name':'keystone_strategy', 'table_name':'complete_npis_inherited_policies',  'type': 'csv'}
    field_map = {
        "fips":"fips",
        "county":"county",
        "state"	:"state",
        "npi"	:"npi",
        "start_date":"start_date",
        "end_date":"end_date",
        "citation":"citation",
        "note"	:"note",
        "re-opening_note":"re_opening_note",
        "re-opening_citation":"re_opening_citation",
    }

    date_field = ['start_date',"end_date"]
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_modcollab_base_on')
def process_public_modcollab_base_on():
    data = {'classification':'public', 'source_name':'modcollab', 'table_name':'base_on',  'type': 'csv'}
    field_map = {
        "Unnamed: 0": "index",
    	"cum_incidence"	:"cumulative_incidence",
        "required_hospW":"required_hospital_ward_beds",
        "required_hospNonVentICU":"required_hospital_non_vented_icu_beds",
        "required_hospVentICU":"required_hospital_vented_icu_beds",
        "available_hospW":"available_hospital_ward_beds",
        "available_hospNonVentICU":"available_hospital_non_vented_icu_beds",
        "available_hospVentICU":"available_hospital_vented_icu_beds",
        "waiting_hospW":"waiting_hospital_ward_beds",
        "waiting_hospNonVentICU":"waiting_hospital_non_vented_icu_beds",
        "waiting_hospVentICU":"waiting_hospital_vented_icu_beds",
    }
    date_field = []
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('public_fisman_ideamodel')
def process_public_fisman_ideamodel():
    data = {'classification':'public', 'source_name':'fisman', 'table_name':'ideamodel',  'type': 'csv'}
    field_map = {
        "Date":'date',
        "Reported cases":"reported_cases",
        'Model incident cases':'model_incident_cases',
        'Model incident cases lower PI':"model_incident_cases_lower_pi",
        'Model incident cases upper PI'	:"model_incident_cases_upper_pi",
        'Reported cumulative cases'	:"reported_cumulative_cases",
        'Model cumulative cases':"model_cumulative_cases",
        'Model cumulative cases lower PI':"model_cumulative_cases_lower_pi",
        'Model cumulative cases upper PI':"model_cumulative_cases_upper_pi",
    }
    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)
    return True

@bp.cli.command('confidential_211_call_reports')
def process_confidential_211_call_reports():
    data = {'classification':'confidential', 'source_name':'211', 'table_name':'call_reports',  'type': 'csv'}
    field_map = {
        "CallReportNum":"call_report_num",
        "CallDateAndTimeStart": "call_date_and_time_start",
        "Demographics of Inquirer - Age Category": "age_of_inquirer"
    }
    date_field = ['call_date_and_time_start']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df[field_map.values()]
            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)
    return True

@bp.cli.command('confidential_211_met_and_unmet_needs')
def process_confidential_211_met_and_unmet_needs():
    data = {'classification':'confidential', 'source_name':'211', 'table_name':'met_and_unmet_needs',  'type': 'csv'}
    field_map = {
        'DateOfCall':'date_of_call',
        'ReportNeedNum':'report_need_num',
        'AIRSNeedCategory':'airs_need_category'
    }
    date_field = ['date_of_call']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df[field_map.values()]
            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)
    return True

@bp.cli.command('confidential_211_referrals')
def process_confidential_211_referrals():
    data = {'classification':'confidential', 'source_name':'211', 'table_name':'referrals',  'type': 'csv'}
    field_map = {
        "CallReportNum":"call_report_num",
        "DateOfCall": "date_of_call",
        "ResourceNum":"resource_num",
        "MetOrUnmet":"met_or_unmet"
    }
    date_field = ['date_of_call']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df[field_map.values()]
            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)
    return True

@bp.cli.command('confidential_burning_glass_industry_weekly')
def process_confidential_burning_glass_industry_weekly():
    data = {'classification':'confidential', 'source_name':'burning_glass', 'table_name':'industry_weekly',  'type': 'csv'}
    field_map = {
        "country_code":"country_code",
        "country":"country",
        "geography_code":"geography_code",
        "geography"	:"geography",
        "geography_type":"geography_type",
        "group_code":"group_code",
        "group_name":"group_name",
        "group_type":"group_type",
        "start_date":"start_date",
        "end_date"	:"end_date",
        "job_postings_count":"job_postings_count",
        "population":"population",
    }
    date_field = ['start_date','end_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)

            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)

@bp.cli.command('restricted_ccso_ccis')
def process_restricted_ccso_ccis():
    data = {'classification':'restricted', 'source_name':'ccso', 'table_name':'ccis',  'type': 'csv'}
    field_map = {
        "RegionName":"region",
        "LHINName": "lhin",
        "CorporationName": "hospital_name",
        "SiteName": "site_name",
        "UnitInclusion": "unit_inclusion",
        "ICUType": "icu_type",
        "ICULevel": "icu_level",
        "Beds": "critical_care_beds",
        "VentedBeds": "vented_beds",
        "CCCensus": "critical_care_patients",
        "CensusVented": "vented_patients",
        "CCCOVID_P_Census": "confirmed_positive",
        "CensusCOVID_P_Vented": "confirmed_positive_ventilator",
        "REGIONNAME": "region",
        "LHINNAME": "lhin",
        "CORPORATIONNAME": "hospital_name",
        "SITENAME": "site_name",
        "UNITINCLUSION": "unit_inclusion",
        "ICUTYPE": "icu_type",
        "ICULEVEL": "icu_level",
        "BEDS": "critical_care_beds",
        "VENTEDBEDS": "vented_beds",
        "CCCENSUS": "critical_care_patients",
        "CENSUSVENTED": "vented_patients",
        "CCCOVIDPOSITIVECENSUS": "confirmed_positive",
        "CENSUSCOVIDPOSITIVEVENTED": "confirmed_positive_ventilator",
    }
    date_field = []
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e

            df = df.rename(columns=field_map)
            df = df[field_map.values()]
            for column in date_field:
                df[column] = pd.to_datetime(df[column],errors='coerce')

            Path(save_dir).mkdir(parents=True, exist_ok=True)
            df.to_csv(save_file, index=False)
    return True

@bp.cli.command('restricted_moh_iphis')
def process_restricted_moh_iphis():
    data = {'classification':'restricted', 'source_name':'moh', 'table_name':'iphis',  'type': 'csv'}
    field_map = {
        "pseudo_id": "pseudo_id",
        "FSA":"fsa",
        "CASE_REPORTED_DATE": "case_reported_date",
        "CLIENT_DEATH_DATE": "client_death_date",
        "HCW": "hcw"
    }
    date_field = ['case_reported_date','client_death_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    def convert_date(date):
        try:
            value = date.split(':')
            date_time_str = value[0]
            date_time_obj = datetime.strptime(date_time_str, "%d%b%Y")
            return date_time_obj
        except:
            return None
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,encoding = "ISO-8859-1")
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                continue

            df = df.rename(columns=field_map)
            try:
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = df[column].apply(convert_date)
                df['fsa'] = df['fsa'].str.upper()
                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
            except Exception as e:
                print(f"Couldn't process {file} due to {e}")
    return True

@bp.cli.command('public')
def process_all():
    process_public_ontario_gov_conposcovidloc()
    process_public_ontario_gov_covidtesting()
    process_public_ontario_gov_longtermcare_in_outbreak()
    process_public_ontario_gov_longtermcare_summary()
    process_public_ontario_gov_longtermcare_no_longer_in_outbreak()
    process_public_howsmyflattening_npi_canada()
    process_public_open_data_working_group_cases()
    process_public_open_data_working_group_mortality()
    process_public_open_data_working_recovered_cumulative()
    process_public_open_data_working_testing_cumulative()
    process_public_google_global_mobility_report()
    process_public_apple_applemobilitytrends()
    process_public_oxcgrt_oxcgrt_latest()
    process_public_jhu_time_series_covid19_confirmed_global()
    process_public_jhu_time_series_covid19_deaths_global()
    process_public_jhu_time_series_covid19_recovered_global()
    process_public_owid_covid_testing_all_observations()
    process_public_keystone_strategy_complete_npis_inherited_policies()
    process_public_modcollab_base_on()
    process_public_fisman_ideamodel()
    process_confidential_211_call_reports()
    process_confidential_211_met_and_unmet_needs()
    process_confidential_211_referrals()
    process_confidential_burning_glass_industry_weekly()
    process_restricted_ccso_ccis()
    process_restricted_moh_iphis()

@bp.cli.command('public_ontario_gov_ltc_covid_summary')
def process_public_ontario_gov_ltc_covid_summary():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'ltc_covid_summary',  'type': 'csv'}
    field_map = {
        "ï»¿Report_Data_Extracted":"date",
        "LTC_Homes_with_Active_Outbreak": "ltc_homes_with_active_outbreak",
        "LTC_Homes_with_Resolved_Outbreak":"ltc_homes_with_resolved_outbreak",
        "Confirmed_Active_LTC_Resident_Cases":"confirmed_active_ltc_resident_cases",
        "Confirmed_Active_LTC_HCW_Cases": "confirmed_active_ltc_hcw_cases",
        "Total_LTC_Resident_Deaths": "total_lct_resident_deaths",
        "Total_LTC_HCW_Deaths": "total_ltc_hcw_deaths",
    }
    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False)
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True

@bp.cli.command('public_ontario_gov_ltc_covid_active_outbreaks')
def process_public_ontario_gov_ltc_covid_active_outbreaks():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'ltc_covid_active_outbreaks',  'type': 'csv'}
    field_map = {
        "ï»¿Report_Data_Extracted":"date",
        "LTC_Home": "ltc_home",
        "LTC_City":"ltc_city",
        "Beds":"beds",
        "Total_LTC_Resident_Cases": "total_ltc_resident_cases",
        "Total_LTC_Resident_Deaths": "total_lct_resident_deaths",
        "Total_LTC_HCW_Cases": "total_ltc_hcw_cases",
    }
    date_field = ['date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False)
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True

@bp.cli.command('public_ontario_gov_school_covid_summary')
def process_public_ontario_gov_school_covid_summary():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'school_covid_summary',  'type': 'csv'}
    field_map = {
        "collected_date":"collected_date",
        "reported_date":"reported_date",
        "current_schools_w_cases":"current_schools_w_cases",
        "current_schools_closed":"current_schools_closed",
        "current_total_number_schools":"current_total_number_schools",
        "new_total_school_related_cases":"new_total_school_related_cases",
        "new_school_related_student_cases":"new_school_related_student_cases",
        "new_school_related_staff_cases":"new_school_related_staff_cases",
        "new_school_related_unspecified_cases":"new_school_related_unspecified_cases",
        "recent_total_school_related_cases":"recent_total_school_related_cases",
        "recent_school_related_student_cases":"recent_school_related_student_cases",
        "recent_school_related_staff_cases":"recent_school_related_staff_cases",
        "recent_school_related_unspecified_cases":"recent_school_related_unspecified_cases",
        "past_total_school_related_cases":"past_total_school_related_cases",
        "past_school_related_student_cases":"past_school_related_student_cases",
        "past_school_related_staff_cases":"past_school_related_staff_cases",
        "past_school_related_unspecified_cases":"past_school_related_unspecified_cases",
        "cumulative_school_related_cases":"cumulative_school_related_cases",
        "cumulative_school_related_student_cases":"cumulative_school_related_student_cases",
        "cumulative_school_related_staff_cases":"cumulative_school_related_staff_cases",
        "cumulative_school_related_unspecified_cases":"cumulative_school_related_unspecified_cases",

    }
    date_field = ['collected_date', 'reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False)
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True

@bp.cli.command('public_ontario_gov_school_covid_active')
def process_public_ontario_gov_school_covid_active():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'school_covid_active',  'type': 'csv'}
    field_map = {
        "collected_date":"collected_date",
        "reported_date":"reported_date",
        "school_board":"school_board",
        "school":"school",
        "municipality":"municipality",
        "confirmed_student_cases":"confirmed_student_cases",
        "confirmed_staff_cases":"confirmed_staff_cases",
        "confirmed_unspecified_cases":"confirmed_unspecified_cases",
        "total_confirmed_cases":"total_confirmed_cases",
    }
    date_field = ['collected_date', 'reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False, encoding='latin-1')
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False, encoding='latin-1')
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True

@bp.cli.command('public_ontario_gov_lc_covid_summary')
def process_public_ontario_gov_lc_covid_summary():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'lc_covid_summary',  'type': 'csv'}
    date_field = ['collected_date', 'reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False, encoding='latin-1')
                # df = df.rename(columns=field_map)
                # df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False, encoding='latin-1')
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True

@bp.cli.command('public_ontario_gov_lc_covid_active')
def process_public_ontario_gov_lc_covid_active():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'lc_covid_active',  'type': 'csv'}
    date_field = ['collected_date', 'reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False, encoding='latin-1')
                # df = df.rename(columns=field_map)
                # df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False, encoding='latin-1')
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True

@bp.cli.command('public_ontario_gov_corrections_covid_cases')
def process_public_ontario_gov_corrections_covid_cases():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'corrections_covid_cases',  'type': 'csv'}
    field_map = {
        "Reported_Date":"reported_date",
        "Region":"region",
        "Institution":"institution",
        "Total_Active_Inmate_Cases_As_Of_Reported_Date":"total_active_inmate_cases_as_of_reported_date",
        "Cumul_Nr_Resolved_Inmate_Cases_As_Of_Reported_Date":"cumul_nr_resolved_inmate_cases_as_of_reported_date",
        "Cumul_Nr_Positive_Released_Inmate_Cases_As_Of_Reported_Date":"cumul_nr_positive_released_inmate_cases_as_of_reported_date",
    }
    date_field = ['reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False, encoding='latin-1')
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False, encoding='latin-1')
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True

@bp.cli.command('public_ontario_gov_corrections_covid_testing')
def process_public_ontario_gov_corrections_covid_testing():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'corrections_covid_testing',  'type': 'csv'}
    field_map = {
        "Reported_Date":"reported_date",
        "Region":"region",
        "Cumulative_Number_of_Tests_as_of_Reported_Date":"cumulative_number_of_tests_as_of_reported_date",
        "Cumulative_Number_of_Positive_Tests_as_of_Reported_Date":"cumulative_number_of_positive_tests_as_of_reported_date",
        "Cumulative_Number_of_Negative_Tests_as_of_Reported_Date":"cumulative_number_of_negative_tests_as_of_reported_date",
        "Total_Number_of_Pending_Tests_on_Reported_Date":"total_number_of_pending_tests_on_reported_date",
        "Total_Number_of_Unknown_Tests_on_Reported_Date": "total_number_of_unknown_tests_on_reported_date",
        "Total_Inmates_that_Refused_Swab_as_of_Reported_Date":"total_inmates_that_refused_swab_as_of_reported_date",
        "Total_Inmates_on_Medical_Isolation_as_of_Reported_Date":"total_inmates_that_refused_swab_as_of_reported_date",
    }
    date_field = ['reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        filename = file.split('_')[-1]
        date = filename.split('.')[0]
        save_file, save_dir = get_file_path(data, 'processed', date)
        if not os.path.isfile(save_file):
            try:
                df = pd.read_csv(file,error_bad_lines=False, encoding='latin-1')
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column])

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False, encoding='latin-1')
            except Exception as e:
                print(f"Failed to get {data['source_name']}/{data['table_name']}")
                print(e)
                return e
    return True
