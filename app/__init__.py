from flask import Flask, g
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from flask_json import FlaskJSON
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flask_caching import Cache
from flasgger import Swagger
from config import config
import os
import time


cors = CORS()
db = SQLAlchemy()
migrate = Migrate()
flaskjson = FlaskJSON()
limiter = Limiter(key_func=get_remote_address)
cache = Cache(config={'CACHE_TYPE': 'simple'})

def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    cors.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)
    flaskjson.init_app(app)
    limiter.init_app(app)
    cache.init_app(app)
    swagger = Swagger(app)

    from app.data import bp as data
    app.register_blueprint(data)

    from app.dataout import bp as dataout
    app.register_blueprint(dataout)

    from app.api import bp as api
    app.register_blueprint(api)

    from app.plots import bp as plots
    app.register_blueprint(plots)

    from app.data_in import bp as data_in
    app.register_blueprint(data_in)

    from app.data_process import bp as data_process
    app.register_blueprint(data_process)

    from app.data_transform import bp as data_transform
    app.register_blueprint(data_transform)

    from app.data_export import bp as data_export
    app.register_blueprint(data_export)

    if not app.debug and not app.testing:
        pass

    return app

APP_ROOT = os.path.dirname(os.path.abspath(__file__))
