import os
import sys
import click
from app import create_app, db
from app.models import *
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from app.data.routes import update
from app.googlesheets import gsHelper
from datetime import datetime

def sensor():
    with app.app_context():
        update()
        print('refreshed')

def sheets():
    with app.app_context():
    	gsHelper.dumpTablesToSheets()
    	print('google sheets updated')

app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)
sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor,'interval',minutes=180)
sched.add_job(sheets, 'interval',next_run_time=datetime.now(),minutes=180)
sched.start()
