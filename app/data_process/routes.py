from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app.data_process import bp
from datetime import datetime
import pandas as pd
from pathlib import Path
from bs4 import BeautifulSoup
import glob
import os

positivity_replace = {
'ALG':3526,
'BRN':3527,
'CKH':3540,
'DUR':3530,
'EOH':3558,
'GBH':3533,
'HNH':3534,
'HKP':3535,
'HAL':3536,
'HAM':3537,
'HPE':3538,
'HPH':3539,
'KFL':3541,
'LAM':3542,
'LGL':3543,
'MSL':3544,
'NIA':3546,
'NPS':3547,
'NWR':3549,
'OTT':3551,
'PEL':3553,
'PET':3555,
'PQP':3556,
'WAT':3565,
'REN':3557,
'SMD':3560,
'SWH':3575,
'SUD':3561,
'THB':3562,
'TSK':3563,
'TOR':3595,
'WDG':3566,
'WEK':3568,
'YRK':3570,
'overall':6
}

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                    df = pd.read_csv(file)
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
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file)
                df = df.replace("12:00:00 AM", None)
                df = df.rename(columns=field_map)

                for column in date_field:
                    df[column] = pd.to_datetime(df[column], errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

@bp.cli.command('public_ontario_gov_vaccination')
def process_public_ontario_gov_vaccination():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'vaccination',  'type': 'csv'}
    date_field = ['date']
    field_map = {
    'report_date': 'date'
    }
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                    df = pd.read_csv(file)
                    df = df.rename(columns=field_map)
                    df.dropna(how='all', axis=1, inplace=True)
                    df.dropna(how='any', inplace=True)
                    for index, row in df.iterrows():
                        if type(row['previous_day_doses_administered'])==str:
                            df.at[index,'previous_day_doses_administered'] = row['previous_day_doses_administered'].replace(",","")
                        if type(row['total_doses_administered'])==str:
                            df.at[index,'total_doses_administered'] = row['total_doses_administered'].replace(",","")
                        if type(row['total_doses_in_fully_vaccinated_individuals'])==str:
                            df.at[index,'total_doses_in_fully_vaccinated_individuals'] = row['total_doses_in_fully_vaccinated_individuals'].replace(",","")
                        if type(row['total_individuals_fully_vaccinated'])==str:
                            df.at[index,'total_individuals_fully_vaccinated'] = row['total_individuals_fully_vaccinated'].replace(",","")

                    for column in date_field:
                        df[column] = pd.to_datetime(df[column])
                    Path(save_dir).mkdir(parents=True, exist_ok=True)
                    df.to_csv(save_file, index=False)

        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

@bp.cli.command('public_ontario_gov_covidtesting')
def process_public_ontario_gov_covidtesting():
    data = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'covidtesting',  'type': 'csv'}
    date_field = ['reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file)
                to_include = []
                for column in df.columns:
                    name = column.replace(' ','_').lower()
                    df[name] = df[column]
                    to_include.append(name)
                df = df[to_include]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column])
                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file)
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file)
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file,error_bad_lines=False)
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file)
                df = df.rename(columns=field_map)

                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file)
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = pd.to_datetime(df[column],errors='coerce')

                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

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
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            save_file, save_dir = get_file_path(data, 'processed', date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                df = pd.read_csv(file,encoding = "ISO-8859-1")
                df = df.rename(columns=field_map)
                df = df[field_map.values()]
                for column in date_field:
                    df[column] = df[column].apply(convert_date)
                df['fsa'] = df['fsa'].str.upper()
                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)
        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

@bp.cli.command('restricted_ices_positivity')
def process_restricted_ices_positivity():
    data = {'classification':'restricted', 'source_name':'ices', 'table_name':'positivity',  'type': 'xlsx'}
    date_field = ['reported_date']
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            data_out = {'classification':'public', 'source_name':'ices', 'table_name':'positivity',  'type': 'csv'}
            save_file, save_dir = get_file_path(data_out, 'processed', today=date)
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                temp_dfs = []
                for item in positivity_replace:
                    df = pd.read_excel(file,engine='openpyxl',sheet_name=item,header=2)
                    df["HR_UID"] = positivity_replace[item]
                    df["Overall - % positivity"] = df["Overall - % positivity"].str.replace("%","").astype(float)
                    temp_dfs.append(df.rename(columns={
                        "Overall - % positivity": "% Positivity",
                        "End date of week":"Date"
                    })[["Date","HR_UID","% Positivity"]])

                df = pd.concat(temp_dfs)
                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)

        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e

@bp.cli.command('restricted_ices_vaccination')
def process_restricted_ices_vaccination():
    data = {'classification':'restricted', 'source_name':'ices', 'table_name':'vaccination',  'type': 'xlsx'}
    load_file, load_dir = get_file_path(data)
    files = glob.glob(load_dir+"/*."+data['type'])
    for file in files:
        try:
            filename = file.split('_')[-1]
            date = filename.split('.')[0]
            data_out = {'classification':'public', 'source_name':'ices', 'table_name':'vaccination',  'type': 'csv'}
            save_file, save_dir = get_file_path(data_out, 'processed')
            if not os.path.isfile(save_file) or date ==  datetime.today().strftime('%Y-%m-%d'):
                date = pd.read_excel(file,engine='openpyxl',sheet_name=2, header=11)
                date_text = date.iloc[0,0]
                update_date_text = date_text.split()[-1]
                update_date = pd.to_datetime(update_date_text,format="%d%b%Y")
                df = pd.read_excel(file,engine='openpyxl',sheet_name=2,header=23)
                df = df[['FSA','% Vaccinated with at least 1 dose\n(All ages)']]
                df = df.rename(columns={'% Vaccinated with at least 1 dose\n(All ages)':'% Vaccinated'})
                df['date'] = update_date
                Path(save_dir).mkdir(parents=True, exist_ok=True)
                df.to_csv(save_file, index=False)

        except Exception as e:
            print(f"Failed to get {file}")
            print(e)
            return e
