from flask import Blueprint

bp = Blueprint('data', __name__,cli_group='data')

from app.data import routes
from app.export import sheetsHelper
from app.export import kaggleHelper

@bp.cli.command('ontario')
def getontario():
    routes.testsnew()
    print('Ontario data refreshed')

@bp.cli.command('canada')
def getcanada():
    routes.getnpis()
    routes.cases()
    routes.getcanadamortality()
    routes.getcanadarecovered()
    routes.getcanadamobility_google()
    routes.getcanadamobility_apple()
    routes.getcanadatested()
    print('Canada data refreshed')

@bp.cli.command('international')
def getinternational():
    routes.international()
    routes.getinternationalmortality()
    routes.getinternationalrecovered()
    print('International data refreshed')

@bp.cli.command('google')
def export_sheets():
    sheetsHelper.exportToSheets()
    print('Google sheets updated')

@bp.cli.command('kaggle')
def export_kaggle():
    kaggleHelper.exportToKaggle()
    print('Kaggle data exported')
