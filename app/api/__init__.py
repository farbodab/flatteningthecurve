from flask import Blueprint

bp = Blueprint('api', __name__,cli_group='viz')

from app.api import routes
from app.api import vis
