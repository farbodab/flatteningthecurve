import os
import sys
import click
from app import create_app, db
from app.models import *
from flask_migrate import Migrate


app = create_app(os.getenv('FLASK_CONFIG') or 'default')
migrate = Migrate(app, db)
