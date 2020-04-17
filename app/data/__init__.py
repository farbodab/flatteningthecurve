from flask import Blueprint

bp = Blueprint('data', __name__,cli_group='data')

from app.data import routes
from app.api import vis
from app.export import sheetsHelper
from app.export import kaggleHelper

# TO ADD NEW DATA
# 1. Add function in routes.py
# 1.5 Add function in /app/api/vis.py if we need a visualization transform
# 2. Call function in appropriate cli command below
# 3. Add google sheets config with either table or vis function
# 4. Add kaggle config with either table or vis function


sheetsConfig = [
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
    {'name':'Government Response','table':'governmentresponse'},
    {'name':'NPI Interventions - USA','table':'npiinterventions_usa'}
]

kaggleConfig = [
    {'name':'canada_mortality.csv', 'table':'canadamortality'},
    {'name':'canada_recovered.csv', 'table':'canadarecovered'},
    {'name':'icu_capacity_on.csv','table':'phucapacity'},
    {'name':'icucapacity.csv', 'table': 'icucapacity'},
    {'name':'international_mortality.csv','table':'internationalmortality'},
    {'name':'international_recovered.csv','table':'internationalrecovered'},
    {'name':'npi_canada.csv','table': 'npiinterventions'},
    {'name':'npi_usa.csv','table': 'npiinterventions_usa'},
    {'name':'test_data_canada.csv','table': 'covid'},
    {'name':'test_data_intl.csv','table': 'internationaldata'},
    {'name':'test_data_on.csv','table': 'covidtests'},
    {'name':'governmentresponse.csv','table': 'governmentresponse'},
    {'name':'vis_canada_mobility.csv','function': vis.get_mobility,},
    {'name':'vis_canada_mobility_transportation.csv','function': vis.get_mobility_transportation},
    {'name':'vis_growthrecent.csv','function': vis.get_growth_recent},
    {'name':'vis_icucapacity.csv','function': vis.get_icu_capacity},
    {'name':'vis_icucapacityprovince.csv','function': vis.get_icu_capacity_province},
    {'name':'vis_icucasestatusprovince.csv','function': vis.get_icu_case_status_province},
    {'name':'vis_phu.csv','function': vis.get_phus},
    {'name':'vis_results.csv','function': vis.get_results},
    {'name':'vis_testresults.csv','function': vis.get_testresults},
]

@bp.cli.command('ontario')
def getontario():
    routes.testsnew()
    print('Ontario data refreshed')

@bp.cli.command('mobility')
def getcanada():
    routes.getcanadamobility_google()
    routes.getcanadamobility_apple()
    print('Mobility data refreshed')

@bp.cli.command('npi')
def getcanada():
    routes.getnpis()
    routes.getgovernmentresponse()
    routes.getnpiusa()
    print('NPI data refreshed')


@bp.cli.command('icu')
def geticu():
    routes.capacityicu()
    print('ICU data refreshed')

@bp.cli.command('canada')
def getcanada():
    routes.cases()
    routes.getcanadamortality()
    routes.getcanadarecovered()
    routes.getcanadamobility_google()
    routes.getcanadamobility_apple()
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

@bp.cli.command('test')
def test():
    print("Hello world")
