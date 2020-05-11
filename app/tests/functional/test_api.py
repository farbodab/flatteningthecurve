import pytest
import time
from flask import json


data_endpoints = [
    '/data/covidtests',
    '/data/covid',
    '/data/npi',
    '/data/canadamortality',
    '/data/canadarecovered',
    '/data/internationaldata',
    '/data/internationalmortality',
    '/data/internationalrecovered',
    '/data/icucapacity',
    '/data/phucapacity',
    '/data/canadatesting',
    '/data/internationaltesting',
    '/data/mobility',
    '/data/mobilitytransportation',
    '/data/governmentresponse',
    '/data/npiinterventions_usa'
]

responseThresold = 3000

@pytest.mark.parametrize('endpoint', data_endpoints)
def test_dataout(test_client, endpoint):
    start = time.time()
    response = test_client.get(endpoint)
    elapsed = int((time.time() - start)*1000)
    assert response.status_code == 200
    assert response.content_type == 'text/csv'
    assert response.content_length > 0
    assert elapsed < responseThresold, "({} > {}) Request time exceeded threshold".format(elapsed, responseThresold)

api_endpoints = [
    '/api/viz',
    '/api/plots',
    '/api/source',
]

@pytest.mark.parametrize('endpoint', api_endpoints)
def test_api(test_client, endpoint):
    start = time.time()
    response = test_client.get(endpoint)
    elapsed = int((time.time() - start)*1000)
    assert response.status_code == 200
    assert response.content_type == 'application/json'
    assert response.content_length > 0
    assert elapsed < responseThresold, "({} > {}) Request time exceeded threshold".format(elapsed, responseThresold)


def test_viz(test_client):
    response = test_client.get('/api/viz')
    data = json.loads(response.get_data(as_text=True))

    attrs = ['category', 'content', 'desktopHeight', 'header', 'mobileHeight', 'text', 'thumbnail', 'viz']
    assert len(data) > 0
    for i in range(len(data)):
        for attr in attrs:
            assert attr in data[i]
        assert 'div' in data[0]['text'] 

def test_plots(test_client):
    response = test_client.get('/api/plots')
    data = json.loads(response.get_data(as_text=True))

    attrs = ['category', 'column', 'group', 'header', 'html', 'order', 'phu', 'row']
    assert len(data) > 0
    for i in range(len(data)):
        for attr in attrs:
            assert attr in data[i]
        assert 'data' in data[0]['html'] 

def test_source(test_client):
    response = test_client.get('/api/source')
    data = json.loads(response.get_data(as_text=True))

    attrs = ['contact', 'contributor', 'data_feed_type', 'description', 'download', 'link', 'name', 'refresh', 'region', 'source', 'type']
    assert len(data) > 0
    for i in range(len(data)):
        for attr in attrs:
            assert attr in data[i]
