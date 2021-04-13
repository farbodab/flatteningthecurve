from flask import Blueprint

bp = Blueprint('data', __name__,cli_group='data')

from app.data import routes, cli, helpers
