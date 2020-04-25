import pytest
import time
from datetime import date, datetime, timedelta
import pandas as pd
from app.data import kaggleConfig
from app import APP_ROOT

@pytest.mark.parametrize('config', kaggleConfig)
def test_kaggle_basic(test_client, init_kaggle, config):
    def isempty(f):
        for i, l in enumerate(f):
            if i == 1:
                return False
        return True

    def columncount(f):
        line = f.readline()
        ncol = len(line.split(','))
        return ncol

    csv = '{}/tests/data/{}'.format(APP_ROOT, config['name'])

    with open(csv, encoding='utf-8') as f:
        assert not isempty(f), 'Dataset {} is empty'.format(csv)
        f.seek(0)
        colcount = columncount(f)
        assert colcount == config['col'], '({} != {}) Dataset {} has incorrect number of columns '.format(colcount, config['col'], config['name'])
        f.seek(0)

@pytest.mark.parametrize('config', kaggleConfig)
def test_kaggle_uptodate(test_client, init_kaggle, config):
    csv = '{}/tests/data/{}'.format(APP_ROOT, config['name'])

    # Advanced checks
    if 'timeseries' in config:
        df = pd.read_csv(csv)

        uptodate = date.today() - timedelta(days=1)
        latest = datetime.strptime(max(df[config['timeseries']]), "%Y-%m-%d").date()

        assert latest >= uptodate, "({} < {}) Dataset {} is not  up to date".format(latest, uptodate, config['name'])

