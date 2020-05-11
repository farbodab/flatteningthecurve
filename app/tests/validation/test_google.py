import pytest
import time
from datetime import date, datetime, timedelta
import pandas as pd
from app.data import sheetsConfig
from app import APP_ROOT
from app.export import sheetsHelper


outofdate_threshold = 2

configs = [x for x in sheetsConfig]
@pytest.mark.parametrize('config', configs)
def test_google_basic(test_client, config):
    name = config['name']
    sheet = sheetsHelper.getVizSheet(name)

    assert sheet is not None, 'Dataset {} is null'.format(name)
    assert sheet.row_count > 0, 'Dataset {} is empty'.format(name)
    assert sheet.col_count == config['col'], 'Dataset has incorrect number of columns'.format(name)

configs = [x for x in sheetsConfig if 'timeseries' in x]
@pytest.mark.parametrize('config', configs)
def test_google_uptodate(test_client, config):
    name = config['name']
    sheet = sheetsHelper.getVizSheet(name)

    uptodate = date.today() - timedelta(days=outofdate_threshold)
    col_index = sheet.row_values(1).index(config['timeseries'])+1
    try:
        dates = [datetime.strptime(x, '%Y-%m-%d %H:%M:%S').date() for x in sheet.col_values(col_index)[1:]]
    except:
        dates = [datetime.strptime(x, '%Y-%m-%d').date() for x in sheet.col_values(col_index)[1:]]

    latest = max(dates)
    assert latest >= uptodate, "({} < {}) Dataset {} is not  up to date".format(latest, uptodate, name)
