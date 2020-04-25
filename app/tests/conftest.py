import pytest 
from app import create_app, db, APP_ROOT
from app.data import kaggleConfig
from app.export import kaggleHelper
from kaggle.api.kaggle_api_extended import KaggleApi
import subprocess
import os
from zipfile import ZipFile

@pytest.fixture(scope='module')
def test_client():
    flask_app = create_app('testing')

    # Flask provides a way to test your application by exposing the Werkzeug test Client
    # and handling the context locals for you.
    testing_client = flask_app.test_client()
 
    # Establish an application context before running the tests.
    ctx = flask_app.app_context()
    ctx.push()
 
    yield testing_client  # this is where the testing happens!
 
    ctx.pop()

@pytest.fixture(scope='module')
def init_kaggle():
    api = KaggleApi()
    api.authenticate()
    dirname = os.path.join(APP_ROOT, 'tests/data')
    data = api.dataset_download_files('covid19-challenges', path=dirname, unzip=True)

