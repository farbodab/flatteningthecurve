import pytest
from app.data import vis
import pandas as pd
from app.models import *
from app.data import sheetsConfig

exclude = ['Estimation of Rt from Case Counts']
viz_defs = [x for x in sheetsConfig if ('function' in x and 'col' in x and x['name'] not in exclude)]

@pytest.mark.parametrize('viz', viz_defs)
def test_returns_results(test_client, viz):
    results = viz['function']()
    assert len(list(results.columns)) == viz['col']
    assert len(results) > 0