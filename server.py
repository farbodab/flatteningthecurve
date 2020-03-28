import os
import sys
import click
from app import create_app, db
from app.models import *
from flask_migrate import Migrate
from apscheduler.schedulers.background import BackgroundScheduler
from app.data.routes import update

def sensor():
    with app.app_context():
        update()
        print('refreshed')


app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)
sched = BackgroundScheduler(daemon=True)
sched.add_job(sensor,'interval',minutes=180)
sched.start()
