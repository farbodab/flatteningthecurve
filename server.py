import os
import sys
import click
from app import create_app, db
from app.models import *
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from app.data.routes import update, cases
from app.googlesheets import gsHelper
from app.api.vis import *
from datetime import datetime

def sensor():
    with app.app_context():
        update()
        print('testing and international refreshed')

def covid():
    with app.app_context():
        cases()
        print('canada refreshed')

def sheets():
    with app.app_context():
    	print('Updating google sheets')
        # get_phus()
    	gsHelper.dumpTablesToSheets()
    	print('Google sheets updated')

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)
sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor,'interval',minutes=15)
sched.add_job(covid,'interval',minutes=60)
sched.add_job(sheets, 'interval',next_run_time=datetime.now(),minutes=15)
sched.start()
