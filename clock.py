from dotenv import load_dotenv
load_dotenv()
#Hello

import os
from app import create_app, db
from app.models import *
from flask_migrate import Migrate
from apscheduler.schedulers.blocking import BlockingScheduler
from app.data import routes
from app.api import vis
from app.export import sheetsHelper
from app.export import kaggleHelper
from app.plots import routes as plots
from datetime import datetime

sheetsConfig = [
    {'name': 'Infection Source PCT', 'function': vis.get_source_infection_pct, 'col':3},
    {'name': 'Age Trend Outbreak', 'function': vis.get_age_trend_outbreak, 'col':3},
    {'name': 'Age Trend', 'function': vis.get_age_trend, 'col':3},
    {'name': 'Infection Source', 'function': vis.get_source_infection, 'col':3},
    {'name': 'Heat Map', 'function': vis.get_npi_heatmap, 'col':16},
    {'name': 'Outbreak Tracker', 'function': vis.get_outbreaks, 'col':3},
    {'name': 'PHU Map', 'function': vis.get_phu_map, 'col':16},
    {'name':'Results','function':vis.get_results, 'col':3},
    {'name':'PHU','function':vis.get_phus, 'col':3, 'timeseries':'date'},
    {'name':'Growth','function':vis.get_growth, 'col':3},
    {'name':'Growth_Recent','function':vis.get_growth_recent, 'col':5, 'timeseries':'date'},
    {'name':'Test Results','function':vis.get_testresults, 'col':18, 'timeseries':'Date'},
    {'name':'ICU Capacity','function':vis.get_icu_capacity, 'col':20, 'timeseries':'date'},
    {'name':'ICU Capacity Province','function':vis.get_icu_capacity_province, 'col':19, 'timeseries':'date'},
    {'name':'ICU Case Status Province','function':vis.get_icu_case_status_province, 'col':3, 'timeseries':'date'},
    {'name':'Canada Mobility','function':vis.get_mobility, 'col':7, 'timeseries':'date'},
    {'name':'Canada Mobility Transportation','function':vis.get_mobility_transportation, 'col':6, 'timeseries':'date'},
    {'name':'Canada Testing','function':vis.get_tested, 'col':6, 'timeseries':'date'},
    {'name':'Canada Deaths','function':vis.get_deaths, 'col':6, 'timeseries':'date'},
    {'name':'Average Daily Cases (7-day rolling)','function':vis.get_cases_rolling_average, 'col':5, 'timeseries':'date'},
    {'name':'Average Daily Deaths (7-day rolling)','function':vis.get_deaths_rolling_average, 'col':5, 'timeseries':'date'},
    {'name':'Daily Deaths','function':vis.get_daily_deaths, 'col':3, 'timeseries':'date'},
    {'name':'Top Causes','function':vis.get_top_causes, 'col':3},
    {'name':'PHU Death','function':vis.get_phudeath, 'col':3, 'timeseries':'date'},
    {'name':'PHU ICU Capacity','function':vis.get_icu_capacity_phu, 'col':14, 'timeseries':'date'},
    {'name':'Estimation of Rt from Case Counts','function':vis.get_rt_est, 'col':5, 'timeseries':'date'},
    {'name':'Long-term Care Homes','table':'longtermcare', 'timeseries':'date'},
    {'name':'Predictive Model','table':'predictivemodel', 'timeseries':'date'},
    {'name':'IDEA Model','table':'ideamodel', 'timeseries':'date'},
]

kaggleConfig = [
    {'name':'canada_mortality.csv', 'table':'canadamortality', 'col':10, 'timeseries':'date'},
    {'name':'canada_recovered.csv', 'table':'canadarecovered', 'col':4, 'timeseries':'date'},
    {'name':'icu_capacity_on.csv','table':'phucapacity', 'col':4},
    {'name':'icucapacity.csv', 'table': 'icucapacity', 'col':14, 'timeseries':'date'},
    {'name':'international_mortality.csv','table':'internationalmortality', 'col':4, 'timeseries':'date'},
    {'name':'international_recovered.csv','table':'internationalrecovered', 'col':4, 'timeseries':'date'},
    {'name':'npi_canada.csv','table': 'npiinterventions', 'col':25},
    {'name':'npi_usa.csv','table': 'npiinterventions_usa', 'col':8},
    {'name':'test_data_canada.csv','table': 'covid', 'col':10, 'timeseries':'date'},
    {'name':'test_data_intl.csv','table': 'internationaldata', 'col':4, 'timeseries':'date'},
    {'name':'test_data_on.csv','table': 'covidtests', 'col':11, 'timeseries':'date'},
    {'name':'governmentresponse.csv','table': 'governmentresponse', 'col':40, 'timeseries':'date'},
    {'name':'longtermcare_on.csv','table': 'longtermcare', 'col':9, 'timeseries':'date'},
    {'name':'predictivemodel.csv','table':'predictivemodel', 'timeseries':'date'},
    {'name':'ideamodel.csv','table':'ideamodel', 'timeseries':'date'},
    {'name':'vis_canada_mobility.csv','function': vis.get_mobility, 'col':5, 'timeseries':'date'},
    {'name':'vis_canada_mobility_transportation.csv','function': vis.get_mobility_transportation, 'col':6, 'timeseries':'date'},
    {'name':'vis_growthrecent.csv','function': vis.get_growth_recent, 'col':5, 'timeseries':'date'},
    {'name':'vis_icucapacity.csv','function': vis.get_icu_capacity, 'col':20, 'timeseries':'date'},
    {'name':'vis_icucapacityprovince.csv','function': vis.get_icu_capacity_province, 'col':19, 'timeseries':'date'},
    {'name':'vis_icucasestatusprovince.csv','function': vis.get_icu_case_status_province, 'col':3, 'timeseries':'date'},
    {'name':'vis_phu.csv','function': vis.get_phus, 'col':3, 'timeseries':'date'},
    {'name':'vis_results.csv','function': vis.get_results, 'col':3},
    {'name':'vis_testresults.csv','function': vis.get_testresults, 'col':17, 'timeseries':'Date'},
]

PHU = ['the_district_of_algoma',
 'brant_county',
 'durham_regional',
 'grey_bruce',
 'haldimand_norfolk',
 'haliburton_kawartha_pine_ridge_district',
 'halton_regional',
 'city_of_hamilton',
 'hastings_and_prince_edward_counties',
 'huron_county',
 'chatham_kent',
 'kingston_frontenac_and_lennox_and_addington',
 'lambton',
 'leeds_grenville_and_lanark_district',
 'middlesex_london',
 'niagara_regional_area',
 'north_bay_parry_sound_district',
 'northwestern',
 'city_of_ottawa',
 'peel_regional',
 'perth_district',
 'peterborough_county_city',
 'porcupine',
 'renfrew_county_and_district',
 'the_eastern_ontario',
 'simcoe_muskoka_district',
 'sudbury_and_district',
 'thunder_bay_district',
 'timiskaming',
 'waterloo',
 'wellington_dufferin_guelph',
 'windsor_essex_county',
 'york_regional',
 'southwestern',
 'city_of_toronto']

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)


def getgoogle():
    with app.app_context():
        sheetsHelper.exportToSheets(sheetsConfig)
        print('Google sheets updated')


def getkaggle():
    with app.app_context():
        kaggleHelper.exportToKaggle(kaggleConfig, 'covid19-challenges', 'HowsMyFlattening COVID-19 Challenges')
        print('Kaggle data exported')


def getontario_faster():
    with app.app_context():
        timeout = 60*60
        interval = 60
        while timeout >= 0:
            timeout = timeout - interval
            resp = routes.testsnew_faster()
            if resp == 'Same':
                time.sleep(interval)
                continue

            if resp == 'New':
                plots.total_cases_plot()
                plots.new_tests_plot()
                plots.on_ventilator_plot()
                plots.in_icu_plot()
                plots.in_hospital_plot()
                plots.recovered_plot()
                plots.new_cases_plot()
                plots.total_tests_plot()
                plots.total_deaths_plot()
                plots.tested_positve_plot()
                plots.under_investigation_plot()
                plots.new_deaths_plot()
                plots.new_deaths_plot()
                plots.total_tests_plot()
                break


sched = BlockingScheduler()
sched.add_job(getgoogle, 'cron', next_run_time=datetime.now(), minute='*/120')
sched.add_job(getkaggle, 'cron', hour=18)
sched.add_job(getontario_faster, 'cron', hour=14)
sched.start()
