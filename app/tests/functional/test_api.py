import pytest
import time

testdata = [
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

responseThresold = 2000

@pytest.mark.parametrize('endpoint', testdata)
def test_api(test_client, endpoint):
    start = time.time()
    response = test_client.get(endpoint)
    elapsed = int((time.time() - start)*1000)
    assert response.status_code == 200
    assert response.content_type == 'text/csv'
    assert response.content_length > 0
    assert elapsed < responseThresold, "({} > {}) Request time exceeded threshold".format(elapsed, responseThresold)
