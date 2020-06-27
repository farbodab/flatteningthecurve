from flask import Blueprint
import click

bp = Blueprint('data_in', __name__,cli_group='data_in')

from app.data_in import routes
