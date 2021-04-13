from app.data import bp
from app.data.helpers import *
from app.api import vis
from app.export import sheetsHelper
from app.plots import routes as plots
from datetime import datetime
import time
import sys
from ftplib import FTP_TLS
import os
import click

# TO ADD NEW DATA
# 1. Add function in routes.py
# 1.5 Add function in /app/api/vis.py if we need a visualization transform
# 2. Call function in appropriate cli command below
# 3. Add google sheets config with either table or vis function
# 4. Add kaggle config with either table or vis function

@bp.cli.command('rt')
def getrt():
    sheetsHelper.exportToSheets(rtsheetsConfig)
    print('Rt sheets updated')


@bp.cli.command('ontario')
def getontario():
    routes.testsnew()
    print('Ontario data refreshed')


@bp.cli.command('mobility')
def getmobility():
    routes.getcanadamobility_google()
    routes.getcanadamobility_apple()
    print('Mobility data refreshed')


@bp.cli.command('canada')
def getcanada():
    routes.cases()
    routes.getcanadamortality()
    routes.getcanadarecovered()
    routes.getcanadatested()
    print('Canada data refreshed')


@bp.cli.command('international')
def getinternational():
    routes.getinternationaltested()
    routes.international()
    routes.getinternationalmortality()
    routes.getinternationalrecovered()
    print('International data refreshed')


@bp.cli.command('google')
def export_sheets():
    sheetsHelper.exportToSheets(sheetsConfig)
    print('Google sheets updated')


@bp.cli.command('plots')
def updateplots():

    plots.new_cases_plot()
    plots.total_cases_plot()
    plots.active_cases_plot()
    plots.new_deaths_plot()
    plots.total_deaths_plot()

    plots.new_tests_plot()
    plots.total_tests_plot()
    plots.tested_positve_plot()
    plots.under_investigation_plot()

    plots.on_ventilator_plot()
    plots.in_icu_plot()
    plots.in_hospital_plot()

    plots.rt_analysis_plot()

    for region in PHU:
        plots.total_cases_plot(region=region)
        plots.new_cases_plot(region=region)
        plots.new_deaths_plot(region=region)
        plots.total_deaths_plot(region=region)
        plots.rt_analysis_plot(region=region)

    print("Plot htmls updated")


@bp.cli.command('ftp')
def get_jobs_data():
    ftp = FTP_TLS('ontario.files.com',timeout=10)
    ftp.login(user=os.environ['211_username'], passwd=os.environ['211_password'])
    ftp.cwd('/211projects/BensTeam')
    ftp.prot_p()
    files = ftp.nlst()
    for filename in files:
        if not os.path.isfile('211_data/'+filename):
            print(f"Getting file {filename}")
            ftp.retrbinary("RETR " + filename ,open('211_data/'+filename, 'wb').write)
    ftp.quit()
    return 'Done'


# Required for pytest don't change
@bp.cli.command('test')
def test():
    print('Hello World')
