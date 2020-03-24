from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from flask_json import FlaskJSON
from config import config


cors = CORS()
db = SQLAlchemy()
migrate = Migrate()
flaskjson = FlaskJSON()

def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    cors.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)
    flaskjson.init_app(app)

    from app.core import bp as core
    app.register_blueprint(core)

    from app.api import bp as api
    app.register_blueprint(api)

    if not app.debug and not app.testing:
        pass

    return app
