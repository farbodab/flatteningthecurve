import pytest
import time
from flask import json
from app.api import routes

# def test_one(app):
#     """Test all api routes to ensure responds with 200 and contains items"""
#     response = routes.get_results()
#     data = json.loads(response.data)
#     assert len(data) > 0
