from flask import Flask, request, jsonify, g, render_template
from app.data_export import bp
from app.export import sheetsHelper
import glob
from datetime import datetime


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
    return file_path

def export(sheetsConfig, data_out):
    file_path = get_file(data_out)
    sheetsConfig[0]['file'] = file_path
    sheetsHelper.exportToSheets(sheetsConfig)

@bp.cli.command('public_capacity_ontario_testing_24_hours')
def export_public_capacity_ontario_testing_24_hours():
    sheetsConfig = [
        {'name':'Reopening - Testing'}
    ]
    data_out ={'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_testing_24_hours',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_cases_ontario_phu_weekly_new_cases')
def export_public_cases_ontario_phu_weekly_new_cases():
    sheetsConfig = [
        {'name':'Reopening - Weekly'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'cases', 'table_name':'ontario_phu_weekly_new_cases',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_capacity_ontario_phu_icu_capacity')
def export_public_capacity_ontario_phu_icu_capacity():
    sheetsConfig = [
        {'name':'Reopening - ICU'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'capacity', 'table_name':'ontario_phu_icu_capacity',  'type': 'csv'}
    export(sheetsConfig, data_out)

@bp.cli.command('public_rt_canada_bettencourt_and_ribeiro_approach')
def export_public_rt_canada_bettencourt_and_ribeiro_approach():
    sheetsConfig = [
        {'name':'Reopening - Rt'}
    ]
    data_out = {'classification':'public', 'stage': 'transformed','source_name':'rt', 'table_name':'canada_bettencourt_and_ribeiro_approach',  'type': 'csv'}
    export(sheetsConfig, data_out)

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
