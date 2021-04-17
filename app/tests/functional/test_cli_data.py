import pytest
from app.data.cli import *

# def test_rt(cli):
#     result = cli.invoke(getrt)
#     assert 'Rt sheets updated' in result.output

def test_ontario(cli):
    result = cli.invoke(getontario)
    assert 'Ontario data refreshed' in result.output

def test_mobility(cli):
    result = cli.invoke(getmobility)
    assert 'Mobility data refreshed' in result.output

def test_canada(cli):
    result = cli.invoke(getcanada)
    assert 'Canada data refreshed' in result.output

def test_international(cli):
    result = cli.invoke(getinternational)
    assert 'International data refreshed' in result.output

# def test_export_sheets(cli):
#     result = cli.invoke(export_sheets)
#     assert 'Google sheets updated' in result.output

def test_updateplots(cli):
    result = cli.invoke(updateplots)
    assert 'Plot htmls updated' in result.output

def test_jobs_data(cli):
    result = cli.invoke(get_jobs_data)
    assert 'Done' in result.output
