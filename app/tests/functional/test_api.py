import pytest
import time
from flask import json

api_endpoints = [
    '/api/viz',
    '/api/plots',
    '/api/source',
    '/api/team',
    '/api/reopening',
    '/api/howsmyflattening',
    '/api/summary',
    '/api/vaccination',
    '/api/times',
    '/api/alerts',
]

@pytest.mark.parametrize('endpoint', api_endpoints)
def test_api(client,endpoint):
    """Test all api routes to ensure responds with 200 and contains items"""
    response = client.get(endpoint)
    data = json.loads(response.data)
    assert response.status_code == 200
    assert len(data) > 0
