from flask import Flask, request, jsonify, g, render_template
from app.data_export import bp
from app.export import sheetsHelper
from app.export import restrictedsheetsHelper
import glob
from datetime import datetime, date
from app.models import *
from app import db, cache
import pandas as pd


def get_dir(data, today=datetime.today().strftime('%Y-%m-%d')):
    source_dir = 'data/' + data['classification'] + '/' + data['stage'] + '/'
    load_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_name = data['table_name'] + '_' + today + '.' + data['type']
    file_path =  load_dir + '/' + file_name
    return load_dir, file_path

def get_file(data):
    load_dir, file_path = get_dir(data)
    files = glob.glob(load_dir + "/*." + data['type'])
    files = [file.split('_')[-1] for file in files]
    files = [file.split('.csv')[0] for file in files]
    dates = [datetime.strptime(file, '%Y-%m-%d') for file in files]
    max_date = max(dates).strftime('%Y-%m-%d')
    load_dir, file_path = get_dir(data, max_date)
    return file_path, max_date

def export(sheetsConfig, data_out):
    file_path, _ = get_file(data_out)
    sheetsConfig[0]['file'] = file_path
    sheetsHelper.exportToSheets(sheetsConfig)

def restricted_export(sheetsConfig, data_out):
    file_path, _ = get_file(data_out)
    sheetsConfig[0]['file'] = file_path
    if data_out['table_name'] == 'iphis':
        df = pd.read_csv(file_path)
        df['cases_two_weeks'] = df.groupby('fsa').cases.rolling(14).sum().droplevel(level=0)
        df['deaths_two_weeks'] = df.groupby('fsa').deaths.rolling(14).sum().droplevel(level=0)
        max_date = df['index'].max()
        temp = df.loc[df['index'] == max_date]
        temp.to_sql(data_out['table_name'],db.engine, if_exists='replace')
    restrictedsheetsHelper.exportToSheets(sheetsConfig)

@bp.cli.command('public_capacity_ontario_testing_24_hours')
def export_public_capacity_ontario_testing_24_hours():
    sheetsConfig = [
        {'name':'Reopening - Testing'}
    ]
    data_out ={'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_testing_24_hours',  'type': 'csv'}
    export(sheetsConfig, data_out)
    file_path, date = get_file(data_out)
    # m = MetricUpdateData.query.filter_by(source=data_out['table_name'], recent=True).first()
    # if m:
    #     m.recent = False
    #     db.session.add(m)
    # m_n = MetricUpdateData(source=data_out['table_name'], recent=True, date_refreshed=date)
    # db.session.add(m_n)
    # db.session.commit()

@bp.cli.command('public_cases_ontario_phu_weekly_new_cases')
def export_public_cases_ontario_phu_weekly_new_cases():
    sheetsConfig = [
        {'name':'Reopening - Weekly'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_phu_weekly_new_cases',  'type': 'csv'}
    export(sheetsConfig, data_out)
    file_path, date = get_file(data_out)
    # m = MetricUpdateData.query.filter_by(source=data_out['table_name'], recent=True).first()
    # if m:
    #     m.recent = False
    #     db.session.add(m)
    # m_n = MetricUpdateData(source=data_out['table_name'], recent=True, date_refreshed=date)
    # db.session.add(m_n)
    # db.session.commit()

@bp.cli.command('public_capacity_ontario_phu_icu_capacity')
def export_public_capacity_ontario_phu_icu_capacity():
    sheetsConfig = [
        {'name':'Reopening - ICU'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_phu_icu_capacity',  'type': 'csv'}
    export(sheetsConfig, data_out)
    file_path, date = get_file(data_out)
    # m = MetricUpdateData.query.filter_by(source=data_out['table_name'], recent=True).first()
    # if m:
    #     m.recent = False
    #     db.session.add(m)
    # m_n = MetricUpdateData(source=data_out['table_name'], recent=True, date_refreshed=date)
    # db.session.add(m_n)
    # db.session.commit()

@bp.cli.command('public_rt_canada_bettencourt_and_ribeiro_approach')
def export_public_rt_canada_bettencourt_and_ribeiro_approach():
    sheetsConfig = [
        {'name':'Reopening - Rt'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'rt', 'table_name':'canada_bettencourt_and_ribeiro_approach',  'type': 'csv'}
    export(sheetsConfig, data_out)
    file_path, date = get_file(data_out)


@bp.cli.command('public_cases_ontario_covid_summary')
def export_public_cases_ontario_covid_summary():
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_covid_summary',  'type': 'csv'}
    file_path, date = get_file(data_out)
    df = pd.read_csv(file_path)
    df.to_sql(data_out['table_name'],db.engine, if_exists='replace')

@bp.cli.command('public_socioeconomic_ontario_211_call_reports')
def export_public_socioeconomic_ontario_211_call_reports():
    sheetsConfig = [
        {'name':'Socioeconomic - 211 Call Report'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'socioeconomic', 'table_name':'ontario_211_call_reports',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_socioeconomic_ontario_211_call_reports_by_age')
def export_public_socioeconomic_ontario_211_call_reports_by_age():
    sheetsConfig = [
        {'name':'Socioeconomic - 211 Call Reports By Age'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'socioeconomic', 'table_name':'ontario_211_call_reports_by_age',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_socioeconomic_ontario_211_call_per_type_of_need')
def export_public_socioeconomic_ontario_211_call_per_type_of_need():
    sheetsConfig = [
        {'name':'Socioeconomic - 211 Call Report Per Type of Need'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'socioeconomic', 'table_name':'ontario_211_call_per_type_of_need',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_economic_ontario_job_postings')
def export_public_economic_ontario_job_postings():
    sheetsConfig = [
        {'name':'Economic - Ontario Job Postings'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'economic', 'table_name':'ontario_job_postings',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('confidential_moh_hcw')
def export_confidential_moh_iphis():
    sheetsConfig = [
        {'name':'Health Care Workers'}
    ]
    data_out = {'classification':'confidential', 'stage': 'transformed','source_name':'moh', 'table_name':'hcw',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_capacity_ontario_testing_analysis')
def export_public_capacity_ontario_testing_analysis():
    sheetsConfig = [
        {'name':'Duration Percentiles'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_testing_analysis',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('confidential_moh_iphis')
def export_confidential_moh_iphis():
    sheetsConfig = [
        {'name':'FSA Data'}
    ]
    data_out = {'classification':'confidential', 'stage': 'transformed','source_name':'moh', 'table_name':'iphis',  'type': 'csv'}
    restricted_export(sheetsConfig, data_out)

@bp.cli.command('public_summary_ontario')
def export_public_summary_ontario():
    sheetsConfig = [
        {'name':'Ontario Summary'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'summary', 'table_name':'ontario',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_cases_ontario_confirmed_positive_cases')
def export_public_cases_ontario_confirmed_positive_cases():
    sheetsConfig = [
        {'name':'Ontario Confirmed Positive'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_confirmed_positive_cases',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_vaccination_ontario')
def export_public_vaccination_ontario():
    sheetsConfig = [
        {'name':'Ontario Vaccination Data'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'vaccination', 'table_name':'ontario',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_subscribers_trend')
def export_subscribers_trend():
    sheetsConfig = [
        {'name':'Subscriber Trend','table':'subscribers_trend'}
    ]
    sheetsHelper.exportToSheets(sheetsConfig)

@bp.cli.command('public_ices_percent_positivity')
def export_public_ices_positivity():
    sheetsConfig = [
        {'name':'PHU Positivity'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'testing', 'table_name':'percent_positivity',  'type': 'csv'}
    export(sheetsConfig, data_out)
