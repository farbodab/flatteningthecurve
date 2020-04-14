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


app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)

# sched = BackgroundScheduler(daemon=True)
# sched.add_job(getontario,'interval',minutes=120)
# sched.add_job(getcanada,'interval',minutes=240)
# sched.add_job(getinternational,'interval',minutes=240)
# sched.add_job(export_sheets, 'interval',next_run_time=datetime.now(), minutes=15)
# if os.getenv('FLASK_CONFIG') == 'production':
#     sched.add_job(export_kaggle, 'interval',next_run_time=datetime.now(), hours=12)

# sched.start()
