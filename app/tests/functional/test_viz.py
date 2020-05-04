import pytest
from app.data import vis
import pandas as pd
from app.models import *
from app.data import sheetsConfig

exclude = ['Estimation of Rt from Case Counts']
viz_defs = [x for x in sheetsConfig if ('function' in x and 'col' in x and x['name'] not in exclude)]

@pytest.mark.parametrize('viz', viz_defs)
def test_get_size(test_client, viz):
    results = viz['function']()
    print(results)
    assert len(list(results.columns)) == viz['col']
    assert len(results) > 0

def test_get_results(test_client):
    actual = vis.get_results()

    actual = actual.loc[actual.region == 'Ontario']
    c = Covid.query.filter_by(province="Ontario")
    df = pd.read_sql(c.statement, db.engine)
    case_count = df.groupby("date").case_id.count().cumsum().reset_index()
    case_count = case_count.loc[case_count.case_id > 100]
    df = df.loc[df.date.isin(case_count.date)]
    df = df.groupby("date").case_id.count().reset_index()
    df['case_id'] = df['case_id']*0.05
    df['case_id'] = df['case_id'].rolling(min_periods=1, window=8).sum()

    print(df.tail(1).case_id)
    print(actual.tail(1).value)
    assert float(df.tail(1).case_id) == float(actual.tail(1).value)