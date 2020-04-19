from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from flask_json import FlaskJSON
from flask_limiter import Limiter
from flask_limiter.util import get_remote_address
from flasgger import Swagger
from config import config


cors = CORS()
db = SQLAlchemy()
migrate = Migrate()
flaskjson = FlaskJSON()
limiter = Limiter(key_func=get_remote_address)

def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    cors.init_app(app)
    db.init_app(app)
    migrate.init_app(app, db)
    flaskjson.init_app(app)
    limiter.init_app(app)
    swagger = Swagger(app)

    from app.data import bp as data
    app.register_blueprint(data)

    from app.dataout import bp as dataout
    app.register_blueprint(dataout)

    from app.api import bp as api
    app.register_blueprint(api)

    from app.plots import bp as plots
    app.register_blueprint(plots)


    if not app.debug and not app.testing:
        pass

    return app
