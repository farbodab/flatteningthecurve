import os
import sys
import click
from app import create_app, db
from app.models import *
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from app.data.routes import *
from app.export import sheetsHelper
from app.export import kaggleHelper
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
        print('Canada data refreshed')

def getinternational():
    with app.app_context():
        international()
        getinternationalmortality()
        getinternationalrecovered()
        print('International data refreshed')

def export_sheets():
    with app.app_context():
    	sheetsHelper.exportToSheets()
    	print('Google sheets updated')

def export_kaggle():
    with app.app_context():
        print('Begin Kaggle export')
        '''
        Example:
        kaggleHelper.exportToKaggle([
            {'table':'npiinterventions', 'filename':'npiinterventions.csv'},
            {'function':get_results , 'filename':'vis.csv'},
            ], 'test', 'test data')'''

        print('Kaggle data exported')

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)
sched = BackgroundScheduler(daemon=True)
sched.add_job(getontario,'interval',minutes=15)
sched.add_job(getcanada,'interval',minutes=120)
sched.add_job(getinternational,'interval',minutes=120)
sched.add_job(export_sheets, 'interval',minutes=15)
sched.add_job(export_kaggle, 'interval',next_run_time=datetime.now(), hours=12)
sched.start()
