from flask import Blueprint
import click

bp = Blueprint('data', __name__,cli_group='data')

from app.data import routes
from app.api import vis
from app.export import sheetsHelper
from app.export import kaggleHelper
from app.plots import routes as plots
from app.tools.pdfparse import extract
from datetime import datetime

# TO ADD NEW DATA
# 1. Add function in routes.py
# 1.5 Add function in /app/api/vis.py if we need a visualization transform
# 2. Call function in appropriate cli command below
# 3. Add google sheets config with either table or vis function
# 4. Add kaggle config with either table or vis function


sheetsConfig = [
    {'name': 'PHU Map', 'function': vis.get_phu_map},
    {'name':'Results','function':vis.get_results},
    {'name':'PHU','function':vis.get_phus},
    {'name':'Growth','function':vis.get_growth},
    {'name':'Growth_Recent','function':vis.get_growth_recent},
    {'name':'Test Results','function':vis.get_testresults},
    {'name':'ICU Capacity','function':vis.get_icu_capacity},
    {'name':'ICU Capacity Province','function':vis.get_icu_capacity_province},
    {'name':'ICU Case Status Province','function':vis.get_icu_case_status_province},
    {'name':'Canada Mobility','function':vis.get_mobility},
    {'name':'Canada Mobility Transportation','function':vis.get_mobility_transportation},
    {'name':'Canada Testing','function':vis.get_tested},
    {'name':'Canada Deaths','function':vis.get_deaths},
    {'name':'Average Daily Cases (7-day rolling)','function':vis.get_cases_rolling_average},
    {'name':'Average Daily Deaths (7-day rolling)','function':vis.get_deaths_rolling_average},
    {'name':'Daily Deaths','function':vis.get_daily_deaths},
    {'name':'Top Causes','function':vis.get_top_causes},
    {'name':'PHU Death','function':vis.get_phudeath},
    {'name':'PHU ICU Capacity','function':vis.get_icu_capacity_phu},
    {'name':'Estimation of Rt from Case Counts','function':vis.get_rt_est},
    {'name':'Long-term Care Homes','table':'longtermcare'},
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
    {'name':'vis_canada_mobility.csv','function': vis.get_mobility, 'col':5, 'timeseries':'date'},
    {'name':'vis_canada_mobility_transportation.csv','function': vis.get_mobility_transportation, 'col':5, 'timeseries':'date'},
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

@bp.cli.command('ontario')
def getontario():
    routes.testsnew()
    routes.getlongtermcare()
    print('Ontario data refreshed')

@bp.cli.command('mobility')
def getcanada():
    # routes.getcanadamobility_google()
    routes.getcanadamobility_apple()
    print('Mobility data refreshed')

@bp.cli.command('npi')
def getcanada():
    routes.getnpis()
    routes.getgovernmentresponse()
    routes.getnpiusa()
    print('NPI data refreshed')

@bp.cli.command('icu')
@click.argument("arg")
def geticu(arg):
    if arg == 'extract':
        # Extract csv from pdf
        extract.extractCCSO(['', './CCSO.pdf', 1, 188, 600, 519, 959])
        print('CCSO data extracted')
        return

    date = None


    date = datetime.strptime(arg,"%d-%m-%Y")
    # except:
    #     print("Date format incorrect, should be DD-MM-YYYY")
    #     return

    routes.capacityicu(date)
    print('ICU data refreshed')

@bp.cli.command('canada')
def getcanada():
    routes.cases()
    routes.getcanadamortality()
    routes.getcanadarecovered()
    routes.getcanadatested()
    print('Canada data refreshed')

@bp.cli.command('international')
def getinternational():
    routes.international()
    routes.getinternationalmortality()
    routes.getinternationalrecovered()
    print('International data refreshed')

@bp.cli.command('google')
def export_sheets():
    sheetsHelper.exportToSheets(sheetsConfig)
    print('Google sheets updated')

@bp.cli.command('kaggle')
def export_kaggle():
    kaggleHelper.exportToKaggle(kaggleConfig, 'covid19-challenges', 'HowsMyFlattening COVID-19 Challenges')
    print('Kaggle data exported')

@bp.cli.command('plots')
def updateplots():

    # for region in PHU:
    #     ## Cases
    #     plots.total_cases_plot(region=region)
    #     plots.new_cases_plot(region=region)
    #     plots.new_deaths_plot(region=region)
    #     plots.total_deaths_plot(region=region)
    #     ## Hospitalization
    #     plots.on_ventilator_plot(region=region)
    #     plots.in_icu_plot(region=region)
    #     ## Capacity
    #     plots.icu_ontario_plot(region=region)
    #     plots.ventilator_ontario_plot(region=region)
    #     plots.rt_analysis_plot(region=region)
    #     ## ltc_cases_plot
    #     plots.ltc_deaths_plot(region=region)
    #     plots.ltc_cases_plot(region=region)
    #     plots.ltc_outbreaks_plot(region=region)
    plots.map()
    plots.apple_mobility_plot()
    plots.total_cases_plot()
    plots.new_tests_plot()
    plots.on_ventilator_plot()
    plots.in_icu_plot()
    plots.in_hospital_plot()
    plots.recovered_plot()
    plots.new_cases_plot()
    plots.total_tests_plot()
    plots.total_deaths_plot()
    plots.retail_mobility_plot()
    plots.icu_ontario_plot()
    plots.ventilator_ontario_plot()
    plots.icu_projections_plot()
    plots.tested_positve_plot()
    plots.under_investigation_plot()
    plots.new_deaths_plot()
    plots.ventilator_ontario_plot()
    plots.new_deaths_plot()
    plots.total_tests_plot()
    plots.ltc_deaths_plot()
    plots.ltc_cases_plot()
    plots.ltc_outbreaks_plot()
    plots.ltc_staff_plot()

    plots.hospital_staff_plot()
    plots.rt_analysis_plot()
    print("Plot htmls updated")
