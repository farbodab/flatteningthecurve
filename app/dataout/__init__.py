from flask import Blueprint

bp = Blueprint('dataout', __name__)

from app.dataout import routes
