from flask import Blueprint
import click

bp = Blueprint('data_process', __name__,cli_group='data_process')

from app.data_process import routes
