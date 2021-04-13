import pytest
from app.api.cli import *

def test_image_upload(cli):
    result = cli.invoke(get_images)
    assert 'your file url' in result.output

def test_email(cli):
    result = cli.invoke(email, ["daily"])
    assert '200' in result.output
