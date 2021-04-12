import pytest
import time
from datetime import date, datetime, timedelta
import pandas as pd
from app.data import kaggleConfig
from app import APP_ROOT
from app import db

#
# outofdate_threshold = 2
#
# configs = [x for x in kaggleConfig if ('table' in x and 'timeseries' in x)]
#
# @pytest.mark.parametrize('config', configs)
# def test_db_uptodate(test_client, config):
#     df = pd.read_sql_table(config['table'], db.engine)
#
#     uptodate = date.today() - timedelta(days=outofdate_threshold)
#     latest = max(df[config['timeseries']]).date()
#     assert latest >= uptodate, "({} < {}) Dataset {} is not  up to date".format(latest, uptodate, config['name'])
