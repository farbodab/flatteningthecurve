from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app.data_process import bp
from datetime import datetime
import pandas as pd
from pathlib import Path

def get_file_path(data, step='raw'):
    today = datetime.today().strftime('%Y-%m-%d')
    source_dir = 'data/' + data['classification'] + '/' + step + '/'
    file_name = data['table_name'] + '_' + today + '.' + data['type']
    save_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_path =  save_dir + '/' + file_name
    return file_path, save_dir

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.replace("12:00:00 AM", None)
    df = df.rename(columns=field_map)

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file, engine='python',encoding='utf8')
    except Exception as e:
        print(f"{load_file}")
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = df.dropna(how='any',subset=["start_date", "country", "intervention_summary","enforcement_category"])
    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)

    for column in date_field:
        df[column] = pd.to_datetime(df[column], format="%d-%m-%Y")

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = df.replace({"NA":None})

    for column in date_field:
        df[column] = pd.to_datetime(df[column], format="%d-%m-%Y")

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

def process_public_open_data_working_recovered_cumulative():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'recovered_cumulative',  'type': 'csv'}
    field_map = {
    "date_recovered":"date_recovered",
    "province":"province",
    "cumulative_recovered":"cumulative_recovered"
    }

    date_field = ['date_recovered']
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = df.replace({"NA":None})

    for column in date_field:
        df[column] = pd.to_datetime(df[column], format="%d-%m-%Y")

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

def process_public_open_data_working_testing_cumulative():
    data = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'testing_cumulative',  'type': 'csv'}
    field_map = {
    "date_testing":"date_testing",
    "province":"province",
    "cumulative_testing":"cumulative_testing",
    "testing_info": "testing_info"
    }

    date_field = ['date_testing']
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = df.replace({"NA":None})

    for column in date_field:
        df[column] = pd.to_datetime(df[column], format="%d-%m-%Y")

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = pd.melt(df, id_vars=['geo_type', 'region', 'transportation_type', 'alternative_name', 'sub_region', 'country'])

    df = df.rename(columns={"variable":"date"})

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)

    for column in date_field:
        df[column] = pd.to_datetime(df[column],format="%Y%m%d")

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

def process_public_jhu_time_series_covid19_confirmed_global():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_confirmed_global',  'type': 'csv'}
    field_map = {
        "Province/State":"province/state",
        "Country/Region":"country/region",
        "Lat":"lat",
        "Long":"long",
    }

    date_field = ['date']
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = pd.melt(df, id_vars=['province/state', 'country/region', 'lat', 'long'])
    df = df.rename(columns={"variable":"date"})

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

def process_public_jhu_time_series_covid19_deaths_global():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_deaths_global',  'type': 'csv'}
    field_map = {
        "Province/State":"province/state",
        "Country/Region":"country/region",
        "Lat":"lat",
        "Long":"long",
    }

    date_field = ['date']
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = pd.melt(df, id_vars=['province/state', 'country/region', 'lat', 'long'])
    df = df.rename(columns={"variable":"date"})

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

def process_public_jhu_time_series_covid19_recovered_global():
    data = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_recovered_global',  'type': 'csv'}
    field_map = {
        "Province/State":"province/state",
        "Country/Region":"country/region",
        "Lat":"lat",
        "Long":"long",
    }

    date_field = ['date']
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)
    df = pd.melt(df, id_vars=['province/state', 'country/region', 'lat', 'long'])
    df = df.rename(columns={"variable":"date"})

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)

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
    load_file, _ = get_file_path(data)
    try:
        df = pd.read_csv(load_file)
    except Exception as e:
        print(f"Failed to get {data['source_name']}/{data['table_name']}")
        print(e)
        return e

    df = df.rename(columns=field_map)

    for column in date_field:
        df[column] = pd.to_datetime(df[column])

    save_file, save_dir = get_file_path(data, 'processed')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    df.to_csv(save_file, index=False)
