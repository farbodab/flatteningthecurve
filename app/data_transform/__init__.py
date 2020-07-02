from flask import Blueprint
import click

bp = Blueprint('data_transform', __name__,cli_group='data_transform')

from app.data_transform import routes
