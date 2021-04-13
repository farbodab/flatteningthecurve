import pytest
from app.api.cli import *

def test_image_upload(cli):
    result = cli.invoke(get_images)
    assert 'done' in result.output

def test_email(cli):
    result = cli.invoke(email, ["daily"])
    assert 'done' in result.output

def test_tweet(cli):
    result = cli.invoke(tweet)
    assert 'done' in result.output
