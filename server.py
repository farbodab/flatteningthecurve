import os
import sys
import click
from app import create_app, db
from app.models import *
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from app.data.routes import *
from app.googlesheets import gsHelper
from app.api.vis import *
from datetime import datetime

def getontario():
    with app.app_context():
        testsnew()
        print('Ontario data refreshed')

def getcanada():
    with app.app_context():
        getnpis()
        cases()
        getcanadamortality()
        getcanadarecovered()
        getcanadamobility()
        getcanadatested()
        print('Canada data refreshed')

def getinternational():
    with app.app_context():
        international()
        getinternationalmortality()
        getinternationalrecovered()
        print('International data refreshed')


def sheets():
    with app.app_context():
        # get_deaths()
        # print('done')
    	gsHelper.dumpTablesToSheets()
    	print('Google sheets updated')

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)
sched = BackgroundScheduler(daemon=True)
sched.add_job(getontario,'interval',minutes=120)
sched.add_job(getcanada,'interval',minutes=240)
sched.add_job(getinternational,'interval',minutes=240)
sched.add_job(sheets, 'interval', next_run_time=datetime.now(),minutes=60)

sched.start()
