from flask import Flask
from flask_sqlalchemy import SQLAlchemy
from flask_migrate import Migrate
from flask_cors import CORS
from config import config


cors = CORS()
db = SQLAlchemy()
migrate = Migrate()


def create_app(config_name):
    app = Flask(__name__)
    app.config.from_object(config[config_name])

    db.init_app(app)
    migrate.init_app(app, db)

    from app.core import bp as api_core
    app.register_blueprint(api_core)

    if not app.debug and not app.testing:
        pass

    return app
