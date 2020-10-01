from flask import Flask, request, jsonify, g, render_template
from flask_json import FlaskJSON, JsonError, json_response, as_json
from app.data_in import bp
from datetime import datetime
import requests, zipfile
from io import StringIO
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import os
import time
from ftplib import FTP_TLS
import glob

def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

def get_public_csv(source_name, table_name, type, url, classification='public'):
    source_dir = 'data/' + classification + '/' + 'raw/'
    today = datetime.today().strftime('%Y-%m-%d')

    file_name = table_name + '_' + today + '.' + type
    save_dir = source_dir + source_name + '/' + table_name
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    save_path =  save_dir + '/' + file_name
    try:
        download_url(url, save_path)
        return f"{save_path} downloaded"
    except Exception as e:
        f"Failed to get {save_path}"
        return e

def get_file_path(data, today=datetime.today().strftime('%Y-%m-%d')):
    source_dir = 'data/' + data['classification'] + '/' + 'raw' + '/'
    file_name = data['table_name'] + '_' + today + '.' + data['type']
    save_dir = source_dir + data['source_name'] + '/' + data['table_name']
    file_path =  save_dir + '/' + file_name
    return file_path, save_dir

@bp.cli.command('public_ontario_gov_conposcovidloc')
def get_public_ontario_gov_conposcovidloc():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'conposcovidloc',  'type': 'csv','url':"https://data.ontario.ca/dataset/f4112442-bdc8-45d2-be3c-12efae72fb27/resource/455fd63b-603d-4608-8216-7d8647f43350/download/conposcovidloc.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_covidtesting')
def get_public_ontario_gov_covidtesting():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'covidtesting',  'type': 'csv','url':"https://data.ontario.ca/dataset/f4f86e54-872d-43f8-8a86-3892fd3cb5e6/resource/ed270bb8-340b-41f9-a7c6-e8ef587e6d11/download/covidtesting.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True


#########################################
@bp.cli.command('public_ontario_gov_ltc_covid_summary')
def get_public_ontario_gov_ltc_covid_summary():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'ltc_covid_summary',  'type': 'csv','url':"https://data.ontario.ca/dataset/42df36df-04a0-43a9-8ad4-fac5e0e22244/resource/0f8b343e-fc28-4ca5-9aab-c3a1d2c919f1/download/ltccovidsummary.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_ltc_covid_active_outbreaks')
def get_public_ontario_gov_ltc_covid_active_outbreaks():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'ltc_covid_active_outbreaks',  'type': 'csv','url':"https://data.ontario.ca/dataset/42df36df-04a0-43a9-8ad4-fac5e0e22244/resource/4b64488a-0523-4ebb-811a-fac2f07e6d59/download/activeltcoutbreak.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_ltc_covid_resolved_outbreaks')
def get_public_ontario_gov_ltc_covid_resolved_outbreaks():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'ltc_covid_resolved_outbreaks',  'type': 'csv','url':"https://data.ontario.ca/dataset/42df36df-04a0-43a9-8ad4-fac5e0e22244/resource/0cf2f01e-d4e1-48ed-8027-2133d059ec8b/download/resolvedltc.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_school_covid_summary')
def get_public_ontario_gov_school_covid_summary():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'school_covid_summary',  'type': 'csv','url':"https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/7fbdbb48-d074-45d9-93cb-f7de58950418/download/schoolcovidsummary.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_school_covid_active')
def get_public_ontario_gov_school_covid_active():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'school_covid_active',  'type': 'csv','url':"https://data.ontario.ca/dataset/b1fef838-8784-4338-8ef9-ae7cfd405b41/resource/8b6d22e2-7065-4b0f-966f-02640be366f2/download/schoolsactivecovid.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_lc_covid_summary')
def get_public_ontario_gov_lc_covid_summary():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'lc_covid_summary',  'type': 'csv','url':"https://data.ontario.ca/dataset/5bf54477-6147-413f-bab0-312f06fcb388/resource/74f9ac9f-7ca8-4860-b2c3-189a2c25e30c/download/lccovidsummary.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_lc_covid_active')
def get_public_ontario_gov_lc_covid_active():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'lc_covid_active',  'type': 'csv','url':"https://data.ontario.ca/dataset/5bf54477-6147-413f-bab0-312f06fcb388/resource/eee282d3-01e6-43ac-9159-4ba694757aea/download/lccactivecovid.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_corrections_covid_cases')
def get_public_ontario_gov_corrections_covid_cases():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'corrections_covid_cases',  'type': 'csv','url':"https://data.ontario.ca/dataset/ecb75ea0-8b72-4f46-a14a-9bd54841d6ab/resource/1f95eda9-53b5-448e-abe0-afc0b71581ed/download/correctionsinmatecases.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_ontario_gov_corrections_covid_testing')
def get_public_ontario_gov_corrections_covid_cases():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'corrections_covid_cases',  'type': 'csv','url':"https://data.ontario.ca/dataset/c4022f0f-6f3d-4e16-bd28-5312333a4bac/resource/d0d6ccc7-fc60-4a18-ac96-7f9493e9f10e/download/inmatetesting.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

######################################
@bp.cli.command('public_ontario_gov_website')
def get_public_ontario_gov_website():
    item = {'classification':'public', 'source_name':'ontario_gov', 'table_name':'website',  'type': 'html','url':"https://www.ontario.ca/page/how-ontario-is-responding-covid-19"}
    file_path, save_dir = get_file_path(item)
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options, service_log_path=os.path.devnull)
    driver.implicitly_wait(1000)
    driver.get(item['url'])
    tables = driver.find_elements_by_tag_name("table")
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    with open(file_path, 'w') as f:
        f.write(driver.page_source)
    return True

@bp.cli.command('public_howsmyflattening_npi_canada')
def get_public_howsmyflattening_npi_canada():
    item = {'classification':'public', 'source_name':'howsmyflattening', 'table_name':'npi_canada',  'type': 'csv','url':"https://raw.githubusercontent.com/jajsmith/COVID19NonPharmaceuticalInterventions/master/npi_canada.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_open_data_working_group_cases')
def get_public_open_data_working_group_cases():
    item = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'cases',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/cases.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_open_data_working_group_mortality')
def get_public_open_data_working_group_mortality():
    item = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'mortality',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/mortality.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_open_data_working_recovered_cumulative')
def get_public_open_data_working_recovered_cumulative():
    item = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'recovered_cumulative',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/recovered_cumulative.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_open_data_working_testing_cumulative')
def get_public_open_data_working_testing_cumulative():
    item = {'classification':'public', 'source_name':'open_data_working_group', 'table_name':'testing_cumulative',  'type': 'csv','url':"https://raw.githubusercontent.com/ishaberry/Covid19Canada/master/testing_cumulative.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_google_global_mobility_report')
def get_public_google_global_mobility_report():
    item = {'classification':'public', 'source_name':'google', 'table_name':'global_mobility_report',  'type': 'csv','url':"https://www.gstatic.com/covid19/mobility/Global_Mobility_Report.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_apple_applemobilitytrends')
def get_public_apple_applemobilitytrends():
    item = {'classification':'public', 'source_name':'apple', 'table_name':'applemobilitytrends',  'type': 'csv'}
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options, service_log_path=os.path.devnull)
    urlpage = "https://www.apple.com/covid19/mobility"
    driver.implicitly_wait(1000)
    driver.get(urlpage)
    button = None
    url = None
    tries = 10
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
        return False

    get_public_csv(item['source_name'],item['table_name'],item['type'],url)
    driver.quit()
    return True

@bp.cli.command('public_oxcgrt_oxcgrt_latest')
def get_public_oxcgrt_oxcgrt_latest():
    item = {'classification':'public', 'source_name':'oxcgrt', 'table_name':'oxcgrt_latest',  'type': 'csv','url':"https://raw.githubusercontent.com/OxCGRT/covid-policy-tracker/master/data/OxCGRT_latest.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_jhu_time_series_covid19_confirmed_global')
def get_public_jhu_time_series_covid19_confirmed_global():
    item = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_confirmed_global',  'type': 'csv','url':"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_confirmed_global.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_jhu_time_series_covid19_deaths_global')
def get_public_jhu_time_series_covid19_deaths_global():
    item = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_deaths_global',  'type': 'csv','url':"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_deaths_global.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_jhu_time_series_covid19_recovered_global')
def get_public_jhu_time_series_covid19_recovered_global():
    item = {'classification':'public', 'source_name':'jhu', 'table_name':'time_series_covid19_recovered_global',  'type': 'csv','url':"https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_time_series/time_series_covid19_recovered_global.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_owid_covid_testing_all_observations')
def get_public_owid_covid_testing_all_observations():
    item = {'classification':'public', 'source_name':'owid', 'table_name':'covid_testing_all_observations',  'type': 'csv','url':"https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/testing/covid-testing-all-observations.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_keystone_strategy_complete_npis_inherited_policies')
def get_public_keystone_strategy_complete_npis_inherited_policies():
    item = {'classification':'public', 'source_name':'keystone_strategy', 'table_name':'complete_npis_inherited_policies',  'type': 'csv','url':"https://raw.githubusercontent.com/Keystone-Strategy/covid19-intervention-data/master/complete_npis_inherited_policies.csv"}
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('public_modcollab_base_on')
def get_public_modcollab_base_on():
    item = {'classification':'public', 'source_name':'modcollab', 'table_name':'base_on',  'type': 'csv','url':"https://pechlilab.shinyapps.io/output/"}
    source_index = 2
    chunk_size=128
    file_path, save_dir = get_file_path(item)
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options, service_log_path=os.path.devnull)
    driver.implicitly_wait(10)
    driver.get(item['url'])
    inputButton = driver.find_element_by_tag_name('input[value="{}"]'.format(source_index))
    inputButton.click()
    modelButton = driver.find_element_by_id('DisplayModel1')
    modelButton.click()
    time.sleep(5)
    driver.implicitly_wait(10)
    button = driver.find_element_by_id('Download')
    data_endpoint = button.get_attribute('href')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    r = requests.get(data_endpoint, allow_redirects=True)
    with open(file_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
    driver.quit()
    return True

@bp.cli.command('public_fisman_ideamodel')
def get_public_fisman_ideamodel():
    item = {'classification':'public', 'source_name':'fisman', 'table_name':'ideamodel',  'type': 'csv','url':"https://art-bd.shinyapps.io/Ontario_Health_Unit_IDEA_model/_w_8d846955"}
    source_index = 2
    chunk_size=128
    file_path, save_dir = get_file_path(item)
    options = Options()
    options.headless = True
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    driver = webdriver.Chrome(options=options, service_log_path=os.path.devnull)
    driver.implicitly_wait(10)
    driver.get(item['url'])
    time.sleep(30)
    inputButton = driver.find_element_by_tag_name('a[data-value="Provinces/Regions"]')
    inputButton.click()
    time.sleep(30)
    button = driver.find_element_by_id('downloadRegionalData')
    url = button.get_attribute('href')
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    r = requests.get(url, allow_redirects=True)
    with open(file_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)
    driver.quit()
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'])
    return True

@bp.cli.command('confidential_211_call_reports')
def get_confidential_211_call_reports():
    item = {'classification':'confidential', 'source_name':'211', 'table_name':'call_reports',  'type': 'csv'}
    file_path, save_dir = get_file_path(item)
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    ftp = FTP_TLS('ontario.files.com',timeout=10)
    ftp.login(user=os.environ['USERNAME_211'], passwd=os.environ['PASSWORD_211'])
    ftp.cwd('/211projects/BensTeam')
    ftp.prot_p()
    files = ftp.nlst()
    for filename in files:
        names = filename.split('-')
        if not 'CallReports' in names:
            continue
        date = filename.split('-Created-')[-1]
        file_path, save_dir = get_file_path(item,date)
        if not os.path.isfile(file_path):
            print(f"Getting file {filename}")
            ftp.retrbinary("RETR " + filename ,open(file_path, 'wb').write)
    ftp.quit()
    return True

@bp.cli.command('confidential_211_met_and_unmet_needs')
def get_confidential_211_met_and_unmet_needs():
    item = {'classification':'confidential', 'source_name':'211', 'table_name':'met_and_unmet_needs',  'type': 'csv'}
    file_path, save_dir = get_file_path(item)
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    ftp = FTP_TLS('ontario.files.com',timeout=10)
    ftp.login(user=os.environ['USERNAME_211'], passwd=os.environ['PASSWORD_211'])
    ftp.cwd('/211projects/BensTeam')
    ftp.prot_p()
    files = ftp.nlst()
    for filename in files:
        names = filename.split('-')
        if not 'MetAndUnmetNeeds' in names:
            continue
        date = filename.split('-Created-')[-1]
        file_path, save_dir = get_file_path(item,date)
        if not os.path.isfile(file_path):
            print(f"Getting file {filename}")
            ftp.retrbinary("RETR " + filename ,open(file_path, 'wb').write)
    ftp.quit()
    return True

@bp.cli.command('confidential_211_referrals')
def get_confidential_211_referrals():
    item = {'classification':'confidential', 'source_name':'211', 'table_name':'referrals',  'type': 'csv'}
    file_path, save_dir = get_file_path(item)
    Path(save_dir).mkdir(parents=True, exist_ok=True)
    ftp = FTP_TLS('ontario.files.com',timeout=10)
    ftp.login(user=os.environ['USERNAME_211'], passwd=os.environ['PASSWORD_211'])
    ftp.cwd('/211projects/BensTeam')
    ftp.prot_p()
    files = ftp.nlst()
    for filename in files:
        names = filename.split('-')
        if not 'Referrals' in names:
            continue
        date = filename.split('-Created-')[-1]
        file_path, save_dir = get_file_path(item,date)
        if not os.path.isfile(file_path):
            print(f"Getting file {filename}")
            ftp.retrbinary("RETR " + filename ,open(file_path, 'wb').write)
    ftp.quit()
    return True

@bp.cli.command('confidential_burning_glass_jobs_data')
def get_confidential_burning_glass_jobs_data():
    item = {'classification':'confidential', 'source_name':'burning_glass', 'table_name':'zip',  'type': 'zip','url':"https://public.burning-glass.com/open_data.zip"}
    file_path, save_dir = get_file_path(item)
    get_public_csv(item['source_name'],item['table_name'],item['type'],item['url'],item['classification'])
    z = zipfile.ZipFile(file_path)
    z.extractall(save_dir)
    files = glob.glob(save_dir+"/*.csv")
    source_dir = 'data/' + item['classification'] + '/' + 'raw/'
    today = datetime.today().strftime('%Y-%m-%d')

    industry_weekly = source_dir + item['source_name'] + '/' + 'industry_weekly/'
    industry_monthly = source_dir + item['source_name'] + '/' + 'industry_monthly/'
    occupation_weekly = source_dir + item['source_name'] + '/' + 'occupation_weekly/'
    occupation_monthly = source_dir + item['source_name'] + '/' + 'occupation_monthly/'
    total_weekly = source_dir + item['source_name'] + '/' + 'total_weekly/'
    total_monthly = source_dir + item['source_name'] + '/' + 'total_monthly/'

    Path(industry_weekly).mkdir(parents=True, exist_ok=True)
    Path(industry_monthly).mkdir(parents=True, exist_ok=True)
    Path(occupation_weekly).mkdir(parents=True, exist_ok=True)
    Path(occupation_monthly).mkdir(parents=True, exist_ok=True)
    Path(total_weekly).mkdir(parents=True, exist_ok=True)
    Path(total_monthly).mkdir(parents=True, exist_ok=True)
    for file in files:
        names = file.split('zip')[-1]
        names = names.split('_')
        base = names[-3].replace("/","")
        base = base.replace("\\","")
        new_name = base + '_' + today + '.csv'
        if base == 'industry':
            if 'weekly' in names:
                os.rename(file, industry_weekly+'industry'+'_'+new_name)
            elif 'monthly' in names:
                os.rename(file, industry_monthly+'industry'+'_'+new_name)

        elif base == 'occupation':
            if 'weekly' in names:
                os.rename(file, occupation_weekly+'occupation'+'_'+new_name)
            elif 'monthly' in names:
                os.rename(file, occupation_monthly+'occupation'+'_'+new_name)
        elif base == 'total':
            if 'weekly' in names:
                os.rename(file, total_weekly+'total'+'_'+new_name)
            elif 'monthly' in names:
                os.rename(file, total_monthly+'total'+'_'+new_name)
    return True

@bp.cli.command('public')
def get_all():
    get_public_ontario_gov_conposcovidloc()
    get_public_ontario_gov_covidtesting()
    get_public_ontario_gov_website()
    get_public_howsmyflattening_npi_canada()
    get_public_open_data_working_group_cases()
    get_public_open_data_working_group_mortality()
    get_public_open_data_working_recovered_cumulative()
    get_public_open_data_working_testing_cumulative()
    get_public_google_global_mobility_report()
    get_public_apple_applemobilitytrends()
    get_public_oxcgrt_oxcgrt_latest()
    get_public_jhu_time_series_covid19_confirmed_global()
    get_public_jhu_time_series_covid19_deaths_global()
    get_public_jhu_time_series_covid19_recovered_global()
    get_public_owid_covid_testing_all_observations()
    get_public_keystone_strategy_complete_npis_inherited_policies()
    get_public_modcollab_base_on()
    get_public_fisman_ideamodel()
    get_confidential_211_call_reports()
    get_confidential_211_met_and_unmet_needs()
    get_confidential_211_referrals()
    get_confidential_burning_glass_jobs_data()
