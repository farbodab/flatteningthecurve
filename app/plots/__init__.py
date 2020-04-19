from flask import Blueprint

bp = Blueprint('plots', __name__)

from app.plots import routes
