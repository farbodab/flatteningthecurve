import pytest
from app import create_app, db, APP_ROOT
import subprocess
import os
from zipfile import ZipFile

@pytest.fixture(scope='function')
def app():
    app = create_app('testing')

    ctx = app.app_context()
    ctx.push()

    def teardown():
        ctx.pop()

    return app


@pytest.fixture(scope='function')
def client(app):
    return app.test_client()

@pytest.fixture(scope='function')
def cli(app):
    return app.test_cli_runner()
